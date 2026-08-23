package io.herald.server;

import io.herald.raft.RaftTransport;
import io.herald.storage.LogConfig;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Broker 配置。字段名带括号的调用形式为流式 setter（返回 this），无参数形式为 getter。
 * {@code peers} 为空即单节点集群（启动后自任 leader）。
 */
public final class BrokerConfig {

    private String host = "0.0.0.0";
    private String advertisedHost = "127.0.0.1";
    private int port = 9092;
    private Path dataDir = Path.of("data");
    private int nodeId = 0;
    private int defaultPartitions = 4;
    private int replicationFactor = 1;
    private int raftPort = 0;
    private long replicaFetchIntervalMs = 100;
    private int maxFrameSize = 64 * 1024 * 1024;
    private LogConfig logConfig = new LogConfig();
    private final Map<Integer, Peer> peers = new LinkedHashMap<>();
    private RaftTransport raftTransport;

    public String host() { return host; }
    public BrokerConfig host(String v) { this.host = v == null ? "0.0.0.0" : v; return this; }

    public String advertisedHost() { return advertisedHost; }
    public BrokerConfig advertisedHost(String v) { this.advertisedHost = v == null ? "127.0.0.1" : v; return this; }

    public int port() { return port; }
    public BrokerConfig port(int v) { this.port = v; return this; }

    public Path dataDir() { return dataDir; }
    public BrokerConfig dataDir(Path v) { this.dataDir = v == null ? Path.of("data") : v; return this; }

    public int nodeId() { return nodeId; }
    public BrokerConfig nodeId(int v) { this.nodeId = v; return this; }

    public int defaultPartitions() { return defaultPartitions; }
    public BrokerConfig defaultPartitions(int v) { this.defaultPartitions = v; return this; }

    public int replicationFactor() { return replicationFactor; }
    public BrokerConfig replicationFactor(int v) { this.replicationFactor = v; return this; }

    public int raftPort() { return raftPort; }
    public BrokerConfig raftPort(int v) { this.raftPort = v; return this; }

    public long replicaFetchIntervalMs() { return replicaFetchIntervalMs; }
    public BrokerConfig replicaFetchIntervalMs(long v) { this.replicaFetchIntervalMs = v; return this; }

    public int maxFrameSize() { return maxFrameSize; }
    public BrokerConfig maxFrameSize(int v) { this.maxFrameSize = v; return this; }

    public LogConfig logConfig() { return logConfig; }
    public BrokerConfig logConfig(LogConfig v) { this.logConfig = v == null ? new LogConfig() : v; return this; }

    public Map<Integer, Peer> peers() { return peers; }
    public BrokerConfig peer(Peer p) { this.peers.put(p.nodeId(), p); return this; }
    public BrokerConfig peers(Map<Integer, Peer> v) {
        this.peers.clear();
        if (v != null) {
            this.peers.putAll(v);
        }
        return this;
    }

    public RaftTransport raftTransport() { return raftTransport; }
    public BrokerConfig raftTransport(RaftTransport v) { this.raftTransport = v; return this; }
}
