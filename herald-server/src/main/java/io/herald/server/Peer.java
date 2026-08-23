package io.herald.server;

/** 集群内一个 Broker 的地址：数据面（host:dataPort）与控制面 Raft（host:raftPort）。 */
public record Peer(int nodeId, String host, int dataPort, int raftPort) {
}
