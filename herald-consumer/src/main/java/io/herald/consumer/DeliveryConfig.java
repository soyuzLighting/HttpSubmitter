package io.herald.consumer;

/** 投递配置：重试、退避、超时。字段名带括号的调用形式为流式 setter（返回 this）。 */
public final class DeliveryConfig {

    private int maxRetries = 5;
    private long retryBackoffMs = 1000;
    private int timeoutMs = 5000;

    public int maxRetries() { return maxRetries; }
    public DeliveryConfig maxRetries(int v) { this.maxRetries = v; return this; }

    public long retryBackoffMs() { return retryBackoffMs; }
    public DeliveryConfig retryBackoffMs(long v) { this.retryBackoffMs = v; return this; }

    public int timeoutMs() { return timeoutMs; }
    public DeliveryConfig timeoutMs(int v) { this.timeoutMs = v; return this; }
}
