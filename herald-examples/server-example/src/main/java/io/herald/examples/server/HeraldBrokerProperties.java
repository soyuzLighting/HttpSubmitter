package io.herald.examples.server;

import io.herald.server.BrokerConfig;
import io.herald.server.Peer;
import io.herald.storage.FlushMode;
import io.herald.storage.LogConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Broker 配置属性，前缀 {@code herald.broker.*}。
 * {@code peers} 为集群中「其他」节点列表（不含自身），格式 {@code id:host:dataPort:raftPort}。
 */
@ConfigurationProperties(prefix = "herald.broker")
public class HeraldBrokerProperties {

    private int nodeId = 0;
    private String host = "0.0.0.0";
    private String advertisedHost = "127.0.0.1";
    private int port = 9092;
    private String dataDir = "data";
    private int defaultPartitions = 4;
    private int replicationFactor = 1;
    private int raftPort = 0;
    private String flushMode = "async";
    private List<String> peers = new ArrayList<>();

    public BrokerConfig toBrokerConfig() {
        BrokerConfig cfg = new BrokerConfig()
                .nodeId(nodeId)
                .host(host)
                .advertisedHost(advertisedHost)
                .port(port)
                .dataDir(Path.of(dataDir))
                .defaultPartitions(defaultPartitions)
                .replicationFactor(replicationFactor)
                .raftPort(raftPort)
                .logConfig(new LogConfig().flushMode("sync".equalsIgnoreCase(flushMode)
                        ? FlushMode.SYNC : FlushMode.ASYNC));
        for (String peer : peers) {
            if (peer != null && !peer.isBlank()) {
                cfg.peer(parsePeer(peer));
            }
        }
        return cfg;
    }

    private static Peer parsePeer(String spec) {
        String[] parts = spec.split(":");
        if (parts.length != 4) {
            throw new IllegalArgumentException("peer must be 'id:host:dataPort:raftPort', got: " + spec);
        }
        return new Peer(Integer.parseInt(parts[0]), parts[1],
                Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
    }

    public int getNodeId() { return nodeId; }
    public void setNodeId(int nodeId) { this.nodeId = nodeId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public String getAdvertisedHost() { return advertisedHost; }
    public void setAdvertisedHost(String advertisedHost) { this.advertisedHost = advertisedHost; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public int getDefaultPartitions() { return defaultPartitions; }
    public void setDefaultPartitions(int defaultPartitions) { this.defaultPartitions = defaultPartitions; }

    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    public int getRaftPort() { return raftPort; }
    public void setRaftPort(int raftPort) { this.raftPort = raftPort; }

    public String getFlushMode() { return flushMode; }
    public void setFlushMode(String flushMode) { this.flushMode = flushMode; }

    public List<String> getPeers() { return peers; }
    public void setPeers(List<String> peers) { this.peers = peers; }
}
