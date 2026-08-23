package io.herald.producer;

import io.herald.common.SnowflakeIdGenerator;
import io.herald.protocol.ErrorCode;
import io.herald.protocol.Frame;
import io.herald.protocol.Message;
import io.herald.protocol.MetadataRequest;
import io.herald.protocol.MetadataResponse;
import io.herald.protocol.Opcode;
import io.herald.protocol.ProduceRequest;
import io.herald.protocol.ProduceResponse;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 生产端 SDK：累加器批量 + 分区选择 + 重试 + 元数据缓存 + 连接池。
 *
 * <pre>{@code
 * HeraldProducer producer = HeraldProducer.builder()
 *     .bootstrapServers("127.0.0.1:9092")
 *     .build();
 * SendResult r = producer.send("user-registered", message);
 * producer.close();
 * }</pre>
 */
public final class HeraldProducer implements AutoCloseable {

    private static final int MAX_FRAME_SIZE = 64 * 1024 * 1024;

    private final ProducerConfig config;
    private final BrokerConnectionPool pool;
    private final MetadataCache metadata = new MetadataCache();
    private final DefaultPartitioner partitioner = new DefaultPartitioner();
    private final SnowflakeIdGenerator idGen;
    private final AtomicLong roundRobinCounter = new AtomicLong();
    private final RecordAccumulator accumulator;
    private final ScheduledExecutorService scheduler;
    private final Thread senderThread;
    private final AtomicBoolean closed = new AtomicBoolean();

    private HeraldProducer(ProducerConfig config) {
        this.config = config;
        this.accumulator = new RecordAccumulator(config.batchSize());
        this.pool = new BrokerConnectionPool(config.bootstrapServers(), config.connectTimeoutMs(), MAX_FRAME_SIZE);
        this.idGen = new SnowflakeIdGenerator(Math.floorMod(config.clientId().hashCode(), 1024));
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "herald-producer-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.senderThread = new Thread(this::runSender, "herald-producer-sender");
    }

    private HeraldProducer start() {
        senderThread.setDaemon(true);
        senderThread.start();
        scheduler.scheduleWithFixedDelay(this::refreshKnownTopics,
                config.metadataRefreshIntervalMs(), config.metadataRefreshIntervalMs(), TimeUnit.MILLISECONDS);
        return this;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** 同步发送，阻塞直到写入确认（或超时/失败）。 */
    public SendResult send(String topic, Message message) throws Exception {
        return sendAsync(topic, message).get(config.requestTimeoutMs() * (config.retries() + 2L), TimeUnit.MILLISECONDS);
    }

    /** 异步发送。 */
    public CompletableFuture<SendResult> sendAsync(String topic, Message message) {
        if (closed.get()) {
            throw new IllegalStateException("producer is closed");
        }
        message.topic(topic);
        if (message.messageId() <= 0) {
            message.messageId(idGen.nextId());
        }
        int partitionCount = metadata.partitionCount(topic);
        if (partitionCount <= 0) {
            refreshMetadata(topic);
            partitionCount = metadata.partitionCount(topic);
            if (partitionCount <= 0) {
                partitionCount = 1; // 元数据尚未建 topic，Broker 会自动创建
            }
        }
        int partition = partitioner.partition(message.key(), partitionCount, roundRobinCounter);
        message.partition(partition);
        CompletableFuture<SendResult> future = new CompletableFuture<>();
        accumulator.append(topic, partition, message, future);
        return future;
    }

    /** 等待累加器清空（尽力而为）。 */
    public void flush(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!accumulator.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(1);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            flush(Math.max(1000, config.requestTimeoutMs() * (config.retries() + 1)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        senderThread.interrupt();
        scheduler.shutdownNow();
        pool.close();
    }

    private void runSender() {
        while (!closed.get()) {
            List<RecordAccumulator.ProducerBatch> ready =
                    accumulator.drainReady(System.currentTimeMillis(), config.lingerMs());
            if (ready.isEmpty()) {
                sleepQuietly(Math.max(1, config.lingerMs()));
                continue;
            }
            for (RecordAccumulator.ProducerBatch batch : ready) {
                sendBatch(batch);
            }
        }
    }

    private void sendBatch(RecordAccumulator.ProducerBatch batch) {
        if (config.acks() == 0) {
            Frame frame = new Frame(Opcode.PRODUCE, Map.of(), buildProduceRequest(batch).encode());
            try {
                connection().send(frame);
                completeBatch(batch, -1);
            } catch (RuntimeException e) {
                failBatch(batch, -1);
            }
            return;
        }
        sendWithRetry(batch, 0);
    }

    private void sendWithRetry(RecordAccumulator.ProducerBatch batch, int attempt) {
        Frame frame = new Frame(Opcode.PRODUCE, Map.of(), buildProduceRequest(batch).encode());
        CompletableFuture<Frame> respFuture;
        try {
            respFuture = connection().send(frame);
        } catch (RuntimeException e) {
            retryOrFail(batch, attempt, -1);
            return;
        }
        respFuture.orTimeout(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                .whenComplete((respFrame, err) -> {
                    if (err != null) {
                        retryOrFail(batch, attempt, -1);
                        return;
                    }
                    try {
                        ProduceResponse resp = ProduceResponse.decode(ByteBuffer.wrap(respFrame.body()));
                        if (resp.errorCode() == ErrorCode.OK) {
                            completeBatch(batch, resp);
                        } else if (resp.errorCode() == ErrorCode.MESSAGE_TOO_LARGE) {
                            failBatch(batch, resp.errorCode());
                        } else {
                            retryOrFail(batch, attempt, resp.errorCode());
                        }
                    } catch (RuntimeException e) {
                        retryOrFail(batch, attempt, -1);
                    }
                });
    }

    private void retryOrFail(RecordAccumulator.ProducerBatch batch, int attempt, int errorCode) {
        if (attempt >= config.retries()) {
            failBatch(batch, errorCode);
        } else {
            long delay = Math.min(100L * (1L << attempt), 1000L);
            scheduler.schedule(() -> sendWithRetry(batch, attempt + 1), delay, TimeUnit.MILLISECONDS);
        }
    }

    private void completeBatch(RecordAccumulator.ProducerBatch batch, ProduceResponse resp) {
        List<Long> offsets = resp.offsets();
        List<Message> messages = batch.messages;
        List<CompletableFuture<SendResult>> futures = batch.futures;
        for (int i = 0; i < futures.size(); i++) {
            long offset = i < offsets.size() ? offsets.get(i) : -1;
            futures.get(i).complete(new SendResult(batch.topic, batch.partition, offset,
                    messages.get(i).messageId(), ErrorCode.OK));
        }
    }

    private void completeBatch(RecordAccumulator.ProducerBatch batch, long offset) {
        List<Message> messages = batch.messages;
        List<CompletableFuture<SendResult>> futures = batch.futures;
        for (int i = 0; i < futures.size(); i++) {
            futures.get(i).complete(new SendResult(batch.topic, batch.partition, offset,
                    messages.get(i).messageId(), ErrorCode.OK));
        }
    }

    private void failBatch(RecordAccumulator.ProducerBatch batch, int errorCode) {
        ProducerException ex = new ProducerException(
                "send failed topic=" + batch.topic + " partition=" + batch.partition, errorCode);
        for (CompletableFuture<SendResult> f : batch.futures) {
            f.completeExceptionally(ex);
        }
    }

    private void refreshKnownTopics() {
        if (closed.get()) {
            return;
        }
        for (String topic : metadata.topics()) {
            refreshMetadata(topic);
        }
    }

    private void refreshMetadata(String topic) {
        try {
            MetadataRequest req = new MetadataRequest().topic(topic);
            Frame frame = new Frame(Opcode.METADATA, Map.of(), req.encode());
            Frame resp = connection().send(frame)
                    .orTimeout(config.requestTimeoutMs(), TimeUnit.MILLISECONDS)
                    .join();
            MetadataResponse md = MetadataResponse.decode(ByteBuffer.wrap(resp.body()));
            metadata.update(md.topics());
        } catch (Exception ignored) {
            // 保留旧缓存，路由时回退到单分区
        }
    }

    private ProduceRequest buildProduceRequest(RecordAccumulator.ProducerBatch batch) {
        return new ProduceRequest()
                .topic(batch.topic)
                .partition(batch.partition)
                .acks(config.acks())
                .messages(batch.messages);
    }

    private BrokerConnection connection() {
        return pool.acquire();
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static final class Builder {
        private ProducerConfig config = new ProducerConfig();

        public Builder bootstrapServers(String v) { config.bootstrapServers(v); return this; }
        public Builder clientId(String v) { config.clientId(v); return this; }
        public Builder lingerMs(int v) { config.lingerMs(v); return this; }
        public Builder batchSize(int v) { config.batchSize(v); return this; }
        public Builder acks(int v) { config.acks(v); return this; }
        public Builder retries(int v) { config.retries(v); return this; }
        public Builder requestTimeoutMs(int v) { config.requestTimeoutMs(v); return this; }
        public Builder config(ProducerConfig v) { this.config = v == null ? new ProducerConfig() : v; return this; }

        public HeraldProducer build() {
            return new HeraldProducer(config).start();
        }
    }
}
