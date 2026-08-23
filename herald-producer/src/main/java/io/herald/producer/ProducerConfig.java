package io.herald.producer;

/**
 * 生产端配置。字段名带括号的调用形式为流式 setter（返回 this），
 * 无参数形式为 getter，例如 {@code new ProducerConfig().lingerMs(10).batchSize(65536)}。
 */
public final class ProducerConfig {

    private String bootstrapServers = "127.0.0.1:9092";
    private String clientId = "herald-producer";
    private int lingerMs = 5;
    private int batchSize = 16 * 1024;
    private int acks = 1;              // 0/1/-1
    private int retries = 3;
    private int requestTimeoutMs = 5000;
    private int connectTimeoutMs = 3000;
    private long metadataRefreshIntervalMs = 60_000;

    public String bootstrapServers() { return bootstrapServers; }
    public ProducerConfig bootstrapServers(String v) { this.bootstrapServers = v == null ? "127.0.0.1:9092" : v; return this; }

    public String clientId() { return clientId; }
    public ProducerConfig clientId(String v) { this.clientId = v == null ? "herald-producer" : v; return this; }

    public int lingerMs() { return lingerMs; }
    public ProducerConfig lingerMs(int v) { this.lingerMs = v; return this; }

    public int batchSize() { return batchSize; }
    public ProducerConfig batchSize(int v) { this.batchSize = v; return this; }

    public int acks() { return acks; }
    public ProducerConfig acks(int v) { this.acks = v; return this; }

    public int retries() { return retries; }
    public ProducerConfig retries(int v) { this.retries = v; return this; }

    public int requestTimeoutMs() { return requestTimeoutMs; }
    public ProducerConfig requestTimeoutMs(int v) { this.requestTimeoutMs = v; return this; }

    public int connectTimeoutMs() { return connectTimeoutMs; }
    public ProducerConfig connectTimeoutMs(int v) { this.connectTimeoutMs = v; return this; }

    public long metadataRefreshIntervalMs() { return metadataRefreshIntervalMs; }
    public ProducerConfig metadataRefreshIntervalMs(long v) { this.metadataRefreshIntervalMs = v; return this; }
}
