package io.herald.producer;

import io.herald.protocol.Message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 累加器：按 (topic, partition) 聚批。批次在「字节数达到 {@code batchSize}」或
 * 「驻留超过 {@code lingerMs}」时标记为可发送，由 Sender 线程 {@link #drainReady} 取走。
 */
public final class RecordAccumulator {

    private final int batchSize;
    private final Map<TopicPartition, ProducerBatch> batches = new LinkedHashMap<>();
    private final Object lock = new Object();

    public RecordAccumulator(int batchSize) {
        this.batchSize = batchSize;
    }

    public void append(String topic, int partition, Message message, CompletableFuture<SendResult> future) {
        synchronized (lock) {
            ProducerBatch batch = batches.computeIfAbsent(
                    new TopicPartition(topic, partition), k -> new ProducerBatch(topic, partition));
            batch.messages.add(message);
            batch.futures.add(future);
            batch.bytes += message.encode().length;
            if (batch.bytes >= batchSize) {
                batch.full = true;
            }
        }
    }

    /** 取出所有可发送的批次（已满 或 驻留超时）。 */
    public List<ProducerBatch> drainReady(long now, int lingerMs) {
        List<ProducerBatch> result = new ArrayList<>();
        synchronized (lock) {
            Iterator<Map.Entry<TopicPartition, ProducerBatch>> it = batches.entrySet().iterator();
            while (it.hasNext()) {
                ProducerBatch batch = it.next().getValue();
                if (batch.full || (now - batch.createdAt >= lingerMs)) {
                    it.remove();
                    result.add(batch);
                }
            }
        }
        return result;
    }

    public boolean isEmpty() {
        synchronized (lock) {
            return batches.isEmpty();
        }
    }

    /** 单个待发送批次，绑定一组消息与对应的完成 Future。 */
    public static final class ProducerBatch {
        final String topic;
        final int partition;
        final long createdAt = System.currentTimeMillis();
        final List<Message> messages = new ArrayList<>();
        final List<CompletableFuture<SendResult>> futures = new ArrayList<>();
        int bytes;
        boolean full;

        ProducerBatch(String topic, int partition) {
            this.topic = topic;
            this.partition = partition;
        }

        public String topic() { return topic; }
        public int partition() { return partition; }
        public List<Message> messages() { return messages; }
    }

    private record TopicPartition(String topic, int partition) {
    }
}
