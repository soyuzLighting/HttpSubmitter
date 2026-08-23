package io.herald.raft;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 内嵌 Raft 节点配置。{@code peers} 为 {@code nodeId -> "host:raftPort"}（不含自身）；
 * 为空表示单节点集群，启动后立即成为 leader。
 */
public final class RaftConfig {

    private int nodeId = 0;
    private final Map<Integer, String> peers = new LinkedHashMap<>();
    private long electionTimeoutMinMs = 150;
    private long electionTimeoutMaxMs = 300;
    private long heartbeatIntervalMs = 50;
    private long rpcTimeoutMs = 2000;
    private Path logDir;

    public int nodeId() { return nodeId; }
    public RaftConfig nodeId(int v) { this.nodeId = v; return this; }

    public Map<Integer, String> peers() { return peers; }
    public RaftConfig peer(int nodeId, String address) { this.peers.put(nodeId, address); return this; }
    public RaftConfig peers(Map<Integer, String> v) {
        this.peers.clear();
        if (v != null) {
            this.peers.putAll(v);
        }
        return this;
    }

    public long electionTimeoutMinMs() { return electionTimeoutMinMs; }
    public RaftConfig electionTimeoutMinMs(long v) { this.electionTimeoutMinMs = v; return this; }

    public long electionTimeoutMaxMs() { return electionTimeoutMaxMs; }
    public RaftConfig electionTimeoutMaxMs(long v) { this.electionTimeoutMaxMs = v; return this; }

    public long heartbeatIntervalMs() { return heartbeatIntervalMs; }
    public RaftConfig heartbeatIntervalMs(long v) { this.heartbeatIntervalMs = v; return this; }

    public long rpcTimeoutMs() { return rpcTimeoutMs; }
    public RaftConfig rpcTimeoutMs(long v) { this.rpcTimeoutMs = v; return this; }

    /** Raft 日志（WAL）持久化目录；为 null 表示纯内存（测试用）。 */
    public Path logDir() { return logDir; }
    public RaftConfig logDir(Path v) { this.logDir = v; return this; }
}
