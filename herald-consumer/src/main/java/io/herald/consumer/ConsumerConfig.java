package io.herald.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** 消费端配置。字段名带括号的调用形式为流式 setter（返回 this），无参数形式为 getter。 */
public final class ConsumerConfig {

    private String bootstrapServers = "127.0.0.1:9092";
    private String groupId = "herald-consumer";
    private List<String> topics = new ArrayList<>();
    private int fetchMaxCount = 500;
    private int fetchMaxBytes = 1024 * 1024;
    private int pollIntervalMs = 100;      // 无数据时的轮询间隔
    private int deliveryConcurrency = 8;   // 批次内并发投递度
    private long initialOffset = 0;
    private boolean commitEnabled = true;
    private int connectTimeoutMs = 3000;
    private int requestTimeoutMs = 5000;

    public String bootstrapServers() { return bootstrapServers; }
    public ConsumerConfig bootstrapServers(String v) { this.bootstrapServers = v == null ? "127.0.0.1:9092" : v; return this; }

    public String groupId() { return groupId; }
    public ConsumerConfig groupId(String v) { this.groupId = v == null ? "herald-consumer" : v; return this; }

    public List<String> topics() { return topics; }
    public ConsumerConfig topics(String... v) { this.topics = new ArrayList<>(Arrays.asList(v)); return this; }
    public ConsumerConfig topics(List<String> v) { this.topics = v == null ? new ArrayList<>() : new ArrayList<>(v); return this; }

    public int fetchMaxCount() { return fetchMaxCount; }
    public ConsumerConfig fetchMaxCount(int v) { this.fetchMaxCount = v; return this; }

    public int fetchMaxBytes() { return fetchMaxBytes; }
    public ConsumerConfig fetchMaxBytes(int v) { this.fetchMaxBytes = v; return this; }

    public int pollIntervalMs() { return pollIntervalMs; }
    public ConsumerConfig pollIntervalMs(int v) { this.pollIntervalMs = v; return this; }

    public int deliveryConcurrency() { return deliveryConcurrency; }
    public ConsumerConfig deliveryConcurrency(int v) { this.deliveryConcurrency = v; return this; }

    public long initialOffset() { return initialOffset; }
    public ConsumerConfig initialOffset(long v) { this.initialOffset = v; return this; }

    public boolean commitEnabled() { return commitEnabled; }
    public ConsumerConfig commitEnabled(boolean v) { this.commitEnabled = v; return this; }

    public int connectTimeoutMs() { return connectTimeoutMs; }
    public ConsumerConfig connectTimeoutMs(int v) { this.connectTimeoutMs = v; return this; }

    public int requestTimeoutMs() { return requestTimeoutMs; }
    public ConsumerConfig requestTimeoutMs(int v) { this.requestTimeoutMs = v; return this; }
}
