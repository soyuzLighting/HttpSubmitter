package io.herald.protocol;

/** Broker 数据面地址，随元数据下发给客户端用于 leader 路由。 */
public record BrokerInfo(int nodeId, String host, int port) {
}
