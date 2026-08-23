package io.herald.producer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 分区选择：指定 key 则 {@code hash(key) % partitions}（同一 key 落到同一分区，保证有序），
 * 否则轮询（负载均衡）。
 */
public final class DefaultPartitioner {

    public int partition(String key, int partitionCount, AtomicLong roundRobinCounter) {
        if (partitionCount <= 0) {
            return 0;
        }
        if (key == null || key.isEmpty()) {
            return (int) Math.floorMod(roundRobinCounter.getAndIncrement(), partitionCount);
        }
        return Math.floorMod(key.hashCode(), partitionCount);
    }
}
