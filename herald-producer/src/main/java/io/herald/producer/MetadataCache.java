package io.herald.producer;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 缓存 topic -> 分区数。分区数未知（尚未获取）返回 0。 */
public final class MetadataCache {

    private final ConcurrentHashMap<String, Integer> partitions = new ConcurrentHashMap<>();

    public void update(Map<String, Integer> topics) {
        topics.forEach(partitions::put);
    }

    public int partitionCount(String topic) {
        Integer n = partitions.get(topic);
        return n == null ? 0 : n;
    }

    public boolean knows(String topic) {
        return partitions.containsKey(topic);
    }

    public Set<String> topics() {
        return partitions.keySet();
    }
}
