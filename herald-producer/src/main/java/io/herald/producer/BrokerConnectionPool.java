package io.herald.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 连接池：对每个 Broker 地址维持一条长连接，按轮询选取可用连接。
 *
 * <p>单机阶段仅一个地址；阶段 5 引入 leader 路由后扩展为按分区 leader 定向。</p>
 */
public final class BrokerConnectionPool implements AutoCloseable {

    private final List<String> addresses;
    private final int connectTimeoutMs;
    private final int maxFrameSize;
    private final ConcurrentHashMap<String, BrokerConnection> connections = new ConcurrentHashMap<>();
    private final AtomicLong roundRobin = new AtomicLong();

    public BrokerConnectionPool(String bootstrapServers, int connectTimeoutMs, int maxFrameSize) {
        this.addresses = parseAddresses(bootstrapServers);
        this.connectTimeoutMs = connectTimeoutMs;
        this.maxFrameSize = maxFrameSize;
    }

    /** 返回一个可用连接；全部不可用时抛出 {@link ProducerException}。 */
    public BrokerConnection acquire() {
        int n = addresses.size();
        for (int i = 0; i < n; i++) {
            String addr = addresses.get((int) Math.floorMod(roundRobin.getAndIncrement(), n));
            BrokerConnection conn = connections.get(addr);
            if (conn == null) {
                conn = tryConnect(addr);
                if (conn != null) {
                    BrokerConnection prev = connections.putIfAbsent(addr, conn);
                    if (prev != null) {
                        conn.close();
                        conn = prev;
                    }
                }
            }
            if (conn != null && conn.isActive()) {
                return conn;
            }
        }
        throw new ProducerException("no available broker among " + addresses);
    }

    @Override
    public void close() {
        connections.values().forEach(BrokerConnection::close);
        connections.clear();
    }

    private BrokerConnection tryConnect(String addr) {
        try {
            int idx = addr.lastIndexOf(':');
            String host = addr.substring(0, idx);
            int port = Integer.parseInt(addr.substring(idx + 1));
            return BrokerConnection.connect(host, port, connectTimeoutMs, maxFrameSize);
        } catch (Exception e) {
            return null;
        }
    }

    private static List<String> parseAddresses(String bootstrapServers) {
        List<String> list = new ArrayList<>();
        for (String s : bootstrapServers.split(",")) {
            String trimmed = s.trim();
            if (!trimmed.isEmpty()) {
                list.add(trimmed);
            }
        }
        if (list.isEmpty()) {
            throw new IllegalArgumentException("bootstrapServers must not be empty");
        }
        return list;
    }
}
