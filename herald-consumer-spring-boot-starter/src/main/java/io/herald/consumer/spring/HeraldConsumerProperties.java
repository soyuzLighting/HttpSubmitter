package io.herald.consumer.spring;

import io.herald.consumer.ConsumerConfig;
import io.herald.consumer.DeliveryConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/** 消费端配置属性，前缀 {@code herald.consumer.*}。 */
@ConfigurationProperties(prefix = "herald.consumer")
public class HeraldConsumerProperties {

    private String bootstrapServers = "127.0.0.1:9092";
    private String groupId = "herald-consumer";
    private List<String> topics = new ArrayList<>();
    private int fetchMaxCount = 500;
    private int pollIntervalMs = 100;
    private int deliveryConcurrency = 8;
    private long initialOffset = 0;
    private boolean commitEnabled = true;
    private final Delivery delivery = new Delivery();

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }

    public int getFetchMaxCount() { return fetchMaxCount; }
    public void setFetchMaxCount(int fetchMaxCount) { this.fetchMaxCount = fetchMaxCount; }

    public int getPollIntervalMs() { return pollIntervalMs; }
    public void setPollIntervalMs(int pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }

    public int getDeliveryConcurrency() { return deliveryConcurrency; }
    public void setDeliveryConcurrency(int deliveryConcurrency) { this.deliveryConcurrency = deliveryConcurrency; }

    public long getInitialOffset() { return initialOffset; }
    public void setInitialOffset(long initialOffset) { this.initialOffset = initialOffset; }

    public boolean isCommitEnabled() { return commitEnabled; }
    public void setCommitEnabled(boolean commitEnabled) { this.commitEnabled = commitEnabled; }

    public Delivery getDelivery() { return delivery; }

    public ConsumerConfig toConsumerConfig() {
        return new ConsumerConfig()
                .bootstrapServers(bootstrapServers)
                .groupId(groupId)
                .topics(topics)
                .fetchMaxCount(fetchMaxCount)
                .pollIntervalMs(pollIntervalMs)
                .deliveryConcurrency(deliveryConcurrency)
                .initialOffset(initialOffset)
                .commitEnabled(commitEnabled);
    }

    public DeliveryConfig toDeliveryConfig() {
        return new DeliveryConfig()
                .maxRetries(delivery.maxRetries)
                .retryBackoffMs(delivery.retryBackoffMs)
                .timeoutMs(delivery.timeoutMs);
    }

    public static class Delivery {
        private int maxRetries = 5;
        private long retryBackoffMs = 1000;
        private int timeoutMs = 5000;

        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

        public long getRetryBackoffMs() { return retryBackoffMs; }
        public void setRetryBackoffMs(long retryBackoffMs) { this.retryBackoffMs = retryBackoffMs; }

        public int getTimeoutMs() { return timeoutMs; }
        public void setTimeoutMs(int timeoutMs) { this.timeoutMs = timeoutMs; }
    }
}
