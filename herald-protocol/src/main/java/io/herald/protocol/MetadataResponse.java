package io.herald.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 元数据响应：broker 列表（数据面地址）与每个 topic 各分区的 leader nodeId。
 *
 * <p>编码：errorCode, brokerCount, [nodeId, host, port]*, topicCount, [topic, partitionCount, leader*]*。</p>
 */
public final class MetadataResponse {

    private int errorCode = ErrorCode.OK;
    private final Map<Integer, BrokerInfo> brokers = new LinkedHashMap<>();
    private final Map<String, List<Integer>> topicLeaders = new LinkedHashMap<>();

    public MetadataResponse errorCode(int v) { this.errorCode = v; return this; }
    public MetadataResponse addBroker(int nodeId, String host, int port) {
        this.brokers.put(nodeId, new BrokerInfo(nodeId, host, port));
        return this;
    }
    public MetadataResponse addTopic(String topic, List<Integer> leaders) {
        this.topicLeaders.put(topic, new ArrayList<>(leaders));
        return this;
    }

    public int errorCode() { return errorCode; }
    public Map<Integer, BrokerInfo> brokers() { return brokers; }
    public Map<String, List<Integer>> topicLeaders() { return topicLeaders; }

    public int partitionCount(String topic) {
        List<Integer> leaders = topicLeaders.get(topic);
        return leaders == null ? 0 : leaders.size();
    }

    public int leaderOf(String topic, int partition) {
        List<Integer> leaders = topicLeaders.get(topic);
        if (leaders == null || partition < 0 || partition >= leaders.size()) {
            return -1;
        }
        return leaders.get(partition);
    }

    public String leaderAddress(String topic, int partition) {
        int nodeId = leaderOf(topic, partition);
        BrokerInfo b = brokers.get(nodeId);
        return b == null ? null : b.host() + ":" + b.port();
    }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putVarInt(errorCode);
        w.putVarInt(brokers.size());
        for (BrokerInfo b : brokers.values()) {
            w.putVarInt(b.nodeId());
            w.putString(b.host());
            w.putVarInt(b.port());
        }
        w.putVarInt(topicLeaders.size());
        for (Map.Entry<String, List<Integer>> e : topicLeaders.entrySet()) {
            w.putString(e.getKey());
            w.putVarInt(e.getValue().size());
            for (int leader : e.getValue()) {
                w.putVarInt(leader);
            }
        }
        return w.toByteArray();
    }

    public static MetadataResponse decode(ByteBuffer buf) {
        MetadataResponse r = new MetadataResponse();
        r.errorCode = ByteReader.readVarInt(buf);
        int brokerCount = ByteReader.readVarInt(buf);
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = ByteReader.readVarInt(buf);
            String host = ByteReader.readString(buf);
            int port = ByteReader.readVarInt(buf);
            r.brokers.put(nodeId, new BrokerInfo(nodeId, host, port));
        }
        int topicCount = ByteReader.readVarInt(buf);
        for (int i = 0; i < topicCount; i++) {
            String topic = ByteReader.readString(buf);
            int partitionCount = ByteReader.readVarInt(buf);
            List<Integer> leaders = new ArrayList<>(partitionCount);
            for (int p = 0; p < partitionCount; p++) {
                leaders.add(ByteReader.readVarInt(buf));
            }
            r.topicLeaders.put(topic, leaders);
        }
        return r;
    }
}
