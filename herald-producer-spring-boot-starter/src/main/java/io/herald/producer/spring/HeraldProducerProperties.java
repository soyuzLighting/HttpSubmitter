package io.herald.producer.spring;

import io.herald.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** 生产端配置属性，前缀 {@code herald.producer.*}。 */
@ConfigurationProperties(prefix = "herald.producer")
public class HeraldProducerProperties {

    private String bootstrapServers = "127.0.0.1:9092";
    private String clientId = "herald-producer";
    private int lingerMs = 5;
    private int batchSize = 16 * 1024;
    private int acks = 1;
    private int retries = 3;
    private int requestTimeoutMs = 5000;
    private int connectTimeoutMs = 3000;

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public int getLingerMs() { return lingerMs; }
    public void setLingerMs(int lingerMs) { this.lingerMs = lingerMs; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public int getAcks() { return acks; }
    public void setAcks(int acks) { this.acks = acks; }

    public int getRetries() { return retries; }
    public void setRetries(int retries) { this.retries = retries; }

    public int getRequestTimeoutMs() { return requestTimeoutMs; }
    public void setRequestTimeoutMs(int requestTimeoutMs) { this.requestTimeoutMs = requestTimeoutMs; }

    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; }

    public ProducerConfig toProducerConfig() {
        return new ProducerConfig()
                .bootstrapServers(bootstrapServers)
                .clientId(clientId)
                .lingerMs(lingerMs)
                .batchSize(batchSize)
                .acks(acks)
                .retries(retries)
                .requestTimeoutMs(requestTimeoutMs)
                .connectTimeoutMs(connectTimeoutMs);
    }
}
