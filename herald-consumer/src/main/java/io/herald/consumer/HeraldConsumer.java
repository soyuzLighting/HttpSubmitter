package io.herald.consumer;

import io.herald.protocol.BrokerInfo;
import io.herald.protocol.CommitOffsetRequest;
import io.herald.protocol.ErrorCode;
import io.herald.protocol.FetchRequest;
import io.herald.protocol.FetchResponse;
import io.herald.protocol.Frame;
import io.herald.protocol.Message;
import io.herald.protocol.MetadataRequest;
import io.herald.protocol.MetadataResponse;
import io.herald.protocol.Opcode;
import io.herald.protocol.OffsetFetchRequest;
import io.herald.protocol.OffsetFetchResponse;
import io.herald.protocol.ProduceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费端 SDK：拉取循环 + 并发投递 + 重试退避 + offset 提交 + DLQ。按分区 leader 定向连接。
 *
 * <pre>{@code
 * HeraldConsumer consumer = HeraldConsumer.builder()
 *     .bootstrapServers("127.0.0.1:9092")
 *     .groupId("crm-notifier")
 *     .topics("user-subscribed")
 *     .build();
 * consumer.start();
 * }</pre>
 */
public final class HeraldConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HeraldConsumer.class);
    private static final long REDISCOVER_INTERVAL_MS = 5000;

    private final ConsumerConfig config;
    private final DeliveryConfig deliveryConfig;
    private final DeliveryHandler deliveryHandler;
    private final ExecutorService deliveryExecutor;
    private final AtomicBoolean running = new AtomicBoolean();
    private final Map<String, Map<Integer, Long>> offsets = new ConcurrentHashMap<>();

    private final Map<String, ConsumerConnection> connections = new ConcurrentHashMap<>();
    private volatile Map<Integer, BrokerInfo> brokers = Map.of();
    private final Map<String, List<Integer>> topicLeaders = new ConcurrentHashMap<>();

    private volatile List<PartitionCursor> cursors = new ArrayList<>();
    private Thread consumerThread;
    private volatile long lastDiscovery;

    private HeraldConsumer(ConsumerConfig config, DeliveryConfig deliveryConfig, DeliveryHandler deliveryHandler) {
        this.config = config;
        this.deliveryConfig = deliveryConfig;
        this.deliveryHandler = deliveryHandler;
        this.deliveryExecutor = Executors.newFixedThreadPool(config.deliveryConcurrency(), r -> {
            Thread t = new Thread(r, "herald-delivery");
            t.setDaemon(true);
            return t;
        });
    }

    public static Builder builder() {
        return new Builder();
    }

    /** 启动消费循环（后台守护线程）。 */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        discoverPartitions();
        lastDiscovery = System.currentTimeMillis();
        consumerThread = new Thread(this::run, "herald-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (consumerThread != null) {
            consumerThread.interrupt();
        }
        deliveryExecutor.shutdownNow();
        connections.values().forEach(ConsumerConnection::close);
        connections.clear();
    }

    private void run() {
        while (running.get()) {
            maybeRediscover();
            boolean anyData = false;
            for (PartitionCursor cursor : cursors) {
                if (!running.get()) {
                    break;
                }
                try {
                    anyData |= fetchAndProcess(cursor);
                } catch (IOException e) {
                    invalidateConnection(cursor.leaderAddress);
                    discoverPartitions();
                    break;
                }
            }
            if (!anyData) {
                sleepQuietly(config.pollIntervalMs());
            }
        }
    }

    private boolean fetchAndProcess(PartitionCursor cursor) throws IOException {
        FetchRequest req = new FetchRequest()
                .topic(cursor.topic)
                .partition(cursor.partition)
                .fetchOffset(cursor.nextOffset)
                .maxCount(config.fetchMaxCount())
                .maxBytes(config.fetchMaxBytes());
        Frame resp = connectionFor(cursor.leaderAddress).send(new Frame(Opcode.FETCH, Map.of(), req.encode()));
        FetchResponse fetch = FetchResponse.decode(ByteBuffer.wrap(resp.body()));
        if (fetch.errorCode() == ErrorCode.NOT_LEADER_OR_FOLLOWER) {
            discoverPartitions();
            return false;
        }
        if (fetch.errorCode() != ErrorCode.OK) {
            return false;
        }
        List<Message> messages = fetch.messages();
        if (messages.isEmpty()) {
            return false;
        }
        deliver(cursor, messages);
        long committed = fetch.nextOffset();
        if (config.commitEnabled()) {
            commit(cursor, committed);
        }
        cursor.nextOffset = committed;
        offsets.computeIfAbsent(cursor.topic, t -> new ConcurrentHashMap<>())
                .put(cursor.partition, committed);
        return true;
    }

    private void deliver(PartitionCursor cursor, List<Message> messages) {
        List<CompletableFuture<Void>> futures = new ArrayList<>(messages.size());
        for (Message m : messages) {
            futures.add(CompletableFuture
                    .supplyAsync(() -> deliverWithRetry(m), deliveryExecutor)
                    .thenAccept(ok -> {
                        if (!ok) {
                            writeToDlq(cursor, m, "delivery failed after " + deliveryConfig.maxRetries() + " retries");
                        }
                    }));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private boolean deliverWithRetry(Message message) {
        for (int attempt = 0; attempt <= deliveryConfig.maxRetries(); attempt++) {
            boolean ok = false;
            try {
                ok = deliveryHandler.deliver(message)
                        .get(deliveryConfig.timeoutMs() + 1000L, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
                ok = false;
            }
            if (ok) {
                return true;
            }
            if (attempt < deliveryConfig.maxRetries()) {
                sleepQuietly(backoffMs(attempt));
            }
        }
        return false;
    }

    private long backoffMs(int attempt) {
        long base = deliveryConfig.retryBackoffMs() * (1L << attempt);
        return base + ThreadLocalRandom.current().nextLong(Math.max(1, base / 4));
    }

    private void commit(PartitionCursor cursor, long offset) {
        try {
            CommitOffsetRequest req = new CommitOffsetRequest()
                    .groupId(config.groupId()).topic(cursor.topic).partition(cursor.partition).committedOffset(offset);
            connectionFor(cursor.leaderAddress).send(new Frame(Opcode.COMMIT_OFFSET, Map.of(), req.encode()));
        } catch (IOException e) {
            log.warn("commit failed topic={} partition={} offset={}", cursor.topic, cursor.partition, offset, e);
        }
    }

    private void writeToDlq(PartitionCursor cursor, Message message, String reason) {
        try {
            Message dlq = new Message()
                    .key(message.key())
                    .url(message.url())
                    .method(message.method())
                    .headers(message.headers())
                    .body(message.body())
                    .addHeader("x-herald-dlq-reason", reason)
                    .addHeader("x-herald-dlq-original-topic", cursor.topic);
            ProduceRequest req = new ProduceRequest()
                    .topic(cursor.topic + ".DLQ").partition(-1).acks(1).addMessage(dlq);
            connectionFor(cursor.leaderAddress).send(new Frame(Opcode.PRODUCE, Map.of(), req.encode()));
        } catch (IOException e) {
            log.warn("write to DLQ failed originalTopic={}", cursor.topic, e);
        }
    }

    private void discoverPartitions() {
        refreshMetadata();
        List<PartitionCursor> newCursors = new ArrayList<>();
        for (String topic : config.topics()) {
            int count = partitionCount(topic);
            if (count <= 0) {
                count = 1; // topic 尚未创建，先占位，后续 rediscover 更新
            }
            for (int p = 0; p < count; p++) {
                String addr = leaderAddress(topic, p);
                long off = offsetOf(topic, p, addr);
                newCursors.add(new PartitionCursor(topic, p, off, addr));
            }
        }
        cursors = newCursors;
    }

    private void maybeRediscover() {
        long now = System.currentTimeMillis();
        if (now - lastDiscovery >= REDISCOVER_INTERVAL_MS) {
            discoverPartitions();
            lastDiscovery = now;
        }
    }

    private void refreshMetadata() {
        try {
            MetadataRequest req = new MetadataRequest();
            Frame resp = connectionFor(anyAddress()).send(new Frame(Opcode.METADATA, Map.of(), req.encode()));
            MetadataResponse md = MetadataResponse.decode(ByteBuffer.wrap(resp.body()));
            if (!md.brokers().isEmpty()) {
                brokers = new ConcurrentHashMap<>(md.brokers());
            }
            md.topicLeaders().forEach((topic, leaders) -> topicLeaders.put(topic, new ArrayList<>(leaders)));
        } catch (IOException e) {
            log.debug("metadata refresh failed", e);
        }
    }

    private int partitionCount(String topic) {
        List<Integer> leaders = topicLeaders.get(topic);
        return leaders == null ? 0 : leaders.size();
    }

    private int leaderOf(String topic, int partition) {
        List<Integer> leaders = topicLeaders.get(topic);
        if (leaders == null || partition < 0 || partition >= leaders.size()) {
            return -1;
        }
        return leaders.get(partition);
    }

    private String leaderAddress(String topic, int partition) {
        BrokerInfo b = brokers.get(leaderOf(topic, partition));
        return b == null ? null : b.host() + ":" + b.port();
    }

    private String anyAddress() {
        for (BrokerInfo b : brokers.values()) {
            return b.host() + ":" + b.port();
        }
        return config.bootstrapServers().split(",")[0].trim();
    }

    private long offsetOf(String topic, int partition, String address) {
        Map<Integer, Long> m = offsets.get(topic);
        if (m != null) {
            Long v = m.get(partition);
            if (v != null) {
                return v;
            }
        }
        long committed = fetchCommittedOffset(address, topic, partition);
        return committed >= 0 ? committed : config.initialOffset();
    }

    /** 查询该消费组在 broker 端已提交的位点；失败或无记录返回 -1。 */
    private long fetchCommittedOffset(String address, String topic, int partition) {
        try {
            OffsetFetchRequest req = new OffsetFetchRequest()
                    .groupId(config.groupId()).topic(topic).partition(partition);
            Frame resp = connectionFor(address).send(new Frame(Opcode.OFFSET_FETCH, Map.of(), req.encode()));
            OffsetFetchResponse r = OffsetFetchResponse.decode(ByteBuffer.wrap(resp.body()));
            return r.errorCode() == ErrorCode.OK ? r.committedOffset() : -1;
        } catch (Exception e) {
            return -1;
        }
    }

    private ConsumerConnection connectionFor(String address) {
        if (address == null) {
            address = anyAddress();
        }
        ConsumerConnection conn = connections.get(address);
        if (conn == null || conn.isClosed()) {
            conn = connectTo(address);
            connections.put(address, conn);
        }
        return conn;
    }

    private void invalidateConnection(String address) {
        if (address == null) {
            return;
        }
        ConsumerConnection conn = connections.remove(address);
        if (conn != null) {
            conn.close();
        }
    }

    private ConsumerConnection connectTo(String address) {
        int idx = address.lastIndexOf(':');
        String host = address.substring(0, idx);
        int port = Integer.parseInt(address.substring(idx + 1));
        try {
            return ConsumerConnection.connect(host, port, config.connectTimeoutMs());
        } catch (IOException e) {
            throw new IllegalStateException("failed to connect to broker " + address, e);
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class PartitionCursor {
        final String topic;
        final int partition;
        volatile String leaderAddress;
        volatile long nextOffset;

        PartitionCursor(String topic, int partition, long nextOffset, String leaderAddress) {
            this.topic = topic;
            this.partition = partition;
            this.nextOffset = nextOffset;
            this.leaderAddress = leaderAddress;
        }
    }

    public static final class Builder {
        private ConsumerConfig config = new ConsumerConfig();
        private DeliveryConfig deliveryConfig = new DeliveryConfig();
        private DeliveryHandler deliveryHandler;

        public Builder bootstrapServers(String v) { config.bootstrapServers(v); return this; }
        public Builder config(ConsumerConfig v) { this.config = v == null ? new ConsumerConfig() : v; return this; }
        public Builder groupId(String v) { config.groupId(v); return this; }
        public Builder topics(String... v) { config.topics(v); return this; }
        public Builder fetchMaxCount(int v) { config.fetchMaxCount(v); return this; }
        public Builder pollIntervalMs(int v) { config.pollIntervalMs(v); return this; }
        public Builder deliveryConcurrency(int v) { config.deliveryConcurrency(v); return this; }
        public Builder initialOffset(long v) { config.initialOffset(v); return this; }
        public Builder commitEnabled(boolean v) { config.commitEnabled(v); return this; }
        public Builder deliveryConfig(DeliveryConfig v) { this.deliveryConfig = v == null ? new DeliveryConfig() : v; return this; }
        public Builder deliveryHandler(DeliveryHandler v) { this.deliveryHandler = v; return this; }

        public HeraldConsumer build() {
            DeliveryHandler handler = deliveryHandler != null
                    ? deliveryHandler
                    : new HttpDeliveryHandler(deliveryConfig.timeoutMs());
            return new HeraldConsumer(config, deliveryConfig, handler);
        }
    }
}
