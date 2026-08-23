package io.herald.producer;

import io.herald.protocol.BrokerInfo;
import io.herald.protocol.MetadataResponse;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** 缓存集群 broker 地址与 topic -> 各分区 leader。分区数未知（尚未获取）返回 0。 */
public final class MetadataCache {

    private volatile Map<Integer, BrokerInfo> brokers = Map.of();
    private final ConcurrentHashMap<String, List<Integer>> topicLeaders = new ConcurrentHashMap<>();

    public void update(MetadataResponse md) {
        if (!md.brokers().isEmpty()) {
            brokers = new LinkedHashMap<>(md.brokers());
        }
        md.topicLeaders().forEach((topic, leaders) -> topicLeaders.put(topic, new ArrayList<>(leaders)));
    }

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
        BrokerInfo b = brokers.get(leaderOf(topic, partition));
        return b == null ? null : b.host() + ":" + b.port();
    }

    /** 任一已知 broker 地址，用于元数据请求；未知返回 null。 */
    public String anyBrokerAddress() {
        for (BrokerInfo b : brokers.values()) {
            return b.host() + ":" + b.port();
        }
        return null;
    }

    public boolean knows(String topic) {
        return topicLeaders.containsKey(topic);
    }

    public Set<String> topics() {
        return topicLeaders.keySet();
    }
}
