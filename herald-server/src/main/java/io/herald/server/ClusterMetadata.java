package io.herald.server;

import io.herald.protocol.ByteReader;
import io.herald.protocol.ByteWriter;
import io.herald.raft.RaftStateMachine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 集群元数据状态机：经 Raft 复制维护 broker 注册、topic/分区副本分配与分区 leader。
 *
 * <p>命令负载用二进制编码（见 {@link #registerBroker}/{@link #createTopic}/{@link #electLeader}）。
 * 所有分区元数据在创建后不可变，leader 变更以「整条替换」方式提交，读操作无需加锁。</p>
 */
public final class ClusterMetadata implements RaftStateMachine {

    static final byte CMD_REGISTER_BROKER = 1;
    static final byte CMD_CREATE_TOPIC = 2;
    static final byte CMD_ELECT_LEADER = 3;
    static final byte CMD_COMMIT_OFFSET = 4;

    private record PartitionMeta(int leader, List<Integer> replicas) {
    }

    private final ConcurrentHashMap<Integer, Peer> brokers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, PartitionMeta[]> topics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>> offsets =
            new ConcurrentHashMap<>();

    // ---- 命令编码（静态工厂） ----

    public static byte[] registerBroker(int nodeId, String host, int dataPort, int raftPort) {
        ByteWriter w = new ByteWriter();
        w.putByte(CMD_REGISTER_BROKER);
        w.putVarInt(nodeId);
        w.putString(host);
        w.putVarInt(dataPort);
        w.putVarInt(raftPort);
        return w.toByteArray();
    }

    public static byte[] createTopic(String topic, int partitions, int replicationFactor) {
        ByteWriter w = new ByteWriter();
        w.putByte(CMD_CREATE_TOPIC);
        w.putString(topic);
        w.putVarInt(partitions);
        w.putVarInt(replicationFactor);
        return w.toByteArray();
    }

    public static byte[] electLeader(String topic, int partition, int newLeader) {
        ByteWriter w = new ByteWriter();
        w.putByte(CMD_ELECT_LEADER);
        w.putString(topic);
        w.putVarInt(partition);
        w.putVarInt(newLeader);
        return w.toByteArray();
    }

    public static byte[] commitOffset(String groupId, String topic, int partition, long offset) {
        ByteWriter w = new ByteWriter();
        w.putByte(CMD_COMMIT_OFFSET);
        w.putString(groupId);
        w.putString(topic);
        w.putVarInt(partition);
        w.putLong(offset);
        return w.toByteArray();
    }

    // ---- 状态机 ----

    @Override
    public void apply(long index, byte[] command) {
        ByteBuffer buf = ByteBuffer.wrap(command);
        byte type = buf.get();
        switch (type) {
            case CMD_REGISTER_BROKER -> {
                int nodeId = ByteReader.readVarInt(buf);
                String host = ByteReader.readString(buf);
                int dataPort = ByteReader.readVarInt(buf);
                int raftPort = ByteReader.readVarInt(buf);
                brokers.put(nodeId, new Peer(nodeId, host, dataPort, raftPort));
            }
            case CMD_CREATE_TOPIC -> {
                String topic = ByteReader.readString(buf);
                int partitions = ByteReader.readVarInt(buf);
                int rf = ByteReader.readVarInt(buf);
                if (partitions <= 0) {
                    throw new IllegalArgumentException("partitions must be > 0");
                }
                topics.putIfAbsent(topic, assign(topic, partitions, rf));
            }
            case CMD_ELECT_LEADER -> {
                String topic = ByteReader.readString(buf);
                int partition = ByteReader.readVarInt(buf);
                int newLeader = ByteReader.readVarInt(buf);
                PartitionMeta[] metas = topics.get(topic);
                if (metas != null && partition >= 0 && partition < metas.length) {
                    PartitionMeta old = metas[partition];
                    if (old.replicas().contains(newLeader)) {
                        metas[partition] = new PartitionMeta(newLeader, old.replicas());
                    }
                }
            }
            case CMD_COMMIT_OFFSET -> {
                String groupId = ByteReader.readString(buf);
                String topic = ByteReader.readString(buf);
                int partition = ByteReader.readVarInt(buf);
                long offset = ByteReader.readLong(buf);
                offsets.computeIfAbsent(groupId, g -> new ConcurrentHashMap<>())
                        .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                        .put(partition, offset);
            }
            default -> throw new IllegalArgumentException("unknown metadata command: " + type);
        }
    }

    /** 基于当前 broker 集合，按轮询为每个分区分配副本并设首个副本为 leader。 */
    private PartitionMeta[] assign(String topic, int partitions, int rf) {
        List<Integer> sortedBrokers = new ArrayList<>(new TreeSet<>(brokers.keySet()));
        int brokerCount = Math.max(1, sortedBrokers.size());
        int factor = Math.min(Math.max(1, rf), brokerCount);
        PartitionMeta[] metas = new PartitionMeta[partitions];
        for (int p = 0; p < partitions; p++) {
            List<Integer> replicas = new ArrayList<>(factor);
            for (int i = 0; i < factor; i++) {
                replicas.add(sortedBrokers.get(Math.floorMod(p * factor + i, brokerCount)));
            }
            metas[p] = new PartitionMeta(replicas.get(0), replicas);
        }
        return metas;
    }

    // ---- 查询 ----

    public Peer broker(int nodeId) {
        return brokers.get(nodeId);
    }

    public Map<Integer, Peer> brokers() {
        return new LinkedHashMap<>(brokers);
    }

    public int partitionCount(String topic) {
        PartitionMeta[] metas = topics.get(topic);
        return metas == null ? 0 : metas.length;
    }

    public int leaderOf(String topic, int partition) {
        PartitionMeta[] metas = topics.get(topic);
        if (metas == null || partition < 0 || partition >= metas.length) {
            return -1;
        }
        return metas[partition].leader();
    }

    public List<Integer> replicasOf(String topic, int partition) {
        PartitionMeta[] metas = topics.get(topic);
        if (metas == null || partition < 0 || partition >= metas.length) {
            return List.of();
        }
        return metas[partition].replicas();
    }

    public Map<String, List<Integer>> topicLeaders() {
        Map<String, List<Integer>> out = new LinkedHashMap<>();
        topics.forEach((t, metas) -> {
            List<Integer> leaders = new ArrayList<>(metas.length);
            for (PartitionMeta m : metas) {
                leaders.add(m.leader());
            }
            out.put(t, leaders);
        });
        return out;
    }

    /** 查询消费组在指定分区已提交的位点；无记录返回 -1。 */
    public long committedOffset(String groupId, String topic, int partition) {
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

    /** 返回本节点作为 follower 副本需要从 leader 拉取的分区（topic, partition, leader）。 */
    public List<ReplicaTask> followerTasks(int selfNodeId) {
        List<ReplicaTask> tasks = new ArrayList<>();
        topics.forEach((t, metas) -> {
            for (int p = 0; p < metas.length; p++) {
                PartitionMeta m = metas[p];
                if (m.leader() != selfNodeId && m.replicas().contains(selfNodeId)) {
                    tasks.add(new ReplicaTask(t, p, m.leader()));
                }
            }
        });
        return tasks;
    }

    /** 一条副本拉取任务。 */
    public record ReplicaTask(String topic, int partition, int leader) {
    }
}
