package io.herald.server;

import io.herald.storage.LogConfig;
import io.herald.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理 topic -> 分区日志。每个分区对应一个 {@link PartitionLog}。
 *
 * <p>线程安全：topic 与分区日志的创建/查找基于 {@link ConcurrentHashMap}；
 * 单个分区的追加写由 {@link PartitionLog} 自身保证。</p>
 */
public final class TopicManager implements AutoCloseable {

    private final Path dataDir;
    private final LogConfig logConfig;
    private final ConcurrentHashMap<String, PartitionLog[]> topics = new ConcurrentHashMap<>();

    public TopicManager(Path dataDir, LogConfig logConfig) {
        this.dataDir = dataDir;
        this.logConfig = logConfig;
    }

    /** 创建（或返回已存在的）topic，返回其分区日志数组。 */
    public PartitionLog[] createTopic(String topic, int partitions) throws IOException {
        if (partitions <= 0) {
            throw new IllegalArgumentException("partitions must be > 0");
        }
        PartitionLog[] existing = topics.get(topic);
        if (existing != null) {
            return existing;
        }
        PartitionLog[] logs = new PartitionLog[partitions];
        for (int i = 0; i < partitions; i++) {
            logs[i] = PartitionLog.open(partitionDir(topic, i), logConfig);
        }
        PartitionLog[] prev = topics.putIfAbsent(topic, logs);
        if (prev != null) {
            for (PartitionLog l : logs) {
                l.close();
            }
            return prev;
        }
        return logs;
    }

    /** 返回指定分区的日志；topic 或分区不存在时返回 null。 */
    public PartitionLog getLog(String topic, int partition) {
        PartitionLog[] logs = topics.get(topic);
        if (logs == null || partition < 0 || partition >= logs.length) {
            return null;
        }
        return logs[partition];
    }

    /** 返回 topic 的分区数；不存在返回 0。 */
    public int partitionCount(String topic) {
        PartitionLog[] logs = topics.get(topic);
        return logs == null ? 0 : logs.length;
    }

    /** 返回全部 topic -> 分区数的快照。 */
    public Map<String, Integer> topics() {
        Map<String, Integer> m = new LinkedHashMap<>();
        topics.forEach((t, logs) -> m.put(t, logs.length));
        return m;
    }

    @Override
    public void close() {
        topics.forEach((t, logs) -> {
            for (PartitionLog l : logs) {
                l.close();
            }
        });
        topics.clear();
    }

    private Path partitionDir(String topic, int partition) {
        return dataDir.resolve(topic).resolve(String.valueOf(partition));
    }
}
