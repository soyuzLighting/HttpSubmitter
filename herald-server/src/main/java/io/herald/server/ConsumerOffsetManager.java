package io.herald.server;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费位点管理：内存中维护 {@code groupId -> topic -> partition -> committedOffset}。
 *
 * <p>单机阶段为纯内存实现；集群阶段（阶段 6）将迁移到 Raft 状态机持久化。</p>
 */
public final class ConsumerOffsetManager {

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>> offsets =
            new ConcurrentHashMap<>();

    public void commit(String groupId, String topic, int partition, long offset) {
        offsets.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>())
                .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                .put(partition, offset);
    }

    /** 返回已提交位点；无记录返回 -1。 */
    public long committed(String groupId, String topic, int partition) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> g = offsets.get(groupId);
        if (g == null) {
            return -1;
        }
        ConcurrentHashMap<Integer, Long> t = g.get(topic);
        if (t == null) {
            return -1;
        }
        Long v = t.get(partition);
        return v == null ? -1 : v;
    }
}
