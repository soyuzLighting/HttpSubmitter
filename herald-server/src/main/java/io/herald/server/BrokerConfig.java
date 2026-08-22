package io.herald.server;

import io.herald.storage.LogConfig;

import java.nio.file.Path;

/**
 * 单机 Broker 配置。字段名带括号的调用形式为流式 setter（返回 this），
 * 无参数形式为 getter，例如 {@code new BrokerConfig().port(9092).dataDir(Path.of("data"))}。
 */
public final class BrokerConfig {

    private String host = "0.0.0.0";
    private int port = 9092;
    private Path dataDir = Path.of("data");
    private int nodeId = 0;
    private int defaultPartitions = 4;
    private int maxFrameSize = 64 * 1024 * 1024;
    private LogConfig logConfig = new LogConfig();

    public String host() { return host; }
    public BrokerConfig host(String v) { this.host = v == null ? "0.0.0.0" : v; return this; }

    public int port() { return port; }
    public BrokerConfig port(int v) { this.port = v; return this; }

    public Path dataDir() { return dataDir; }
    public BrokerConfig dataDir(Path v) { this.dataDir = v == null ? Path.of("data") : v; return this; }

    public int nodeId() { return nodeId; }
    public BrokerConfig nodeId(int v) { this.nodeId = v; return this; }

    public int defaultPartitions() { return defaultPartitions; }
    public BrokerConfig defaultPartitions(int v) { this.defaultPartitions = v; return this; }

    public int maxFrameSize() { return maxFrameSize; }
    public BrokerConfig maxFrameSize(int v) { this.maxFrameSize = v; return this; }

    public LogConfig logConfig() { return logConfig; }
    public BrokerConfig logConfig(LogConfig v) { this.logConfig = v == null ? new LogConfig() : v; return this; }
}
