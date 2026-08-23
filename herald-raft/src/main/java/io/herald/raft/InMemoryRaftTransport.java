package io.herald.raft;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存传输：同一 JVM 内多节点共用一条总线，{@link #send} 直接同步调用目标节点的处理器。
 * 用于单元测试与单 JVM 多 Broker 集成测试。
 */
public final class InMemoryRaftTransport implements RaftTransport {

    private final int nodeId;
    private final ConcurrentHashMap<Integer, InMemoryRaftTransport> bus;
    private volatile MessageHandler handler;

    public InMemoryRaftTransport(int nodeId, ConcurrentHashMap<Integer, InMemoryRaftTransport> bus) {
        this.nodeId = nodeId;
        this.bus = bus;
    }

    @Override
    public void start(MessageHandler handler) {
        this.handler = handler;
        bus.put(nodeId, this);
    }

    @Override
    public RaftMessage send(int nodeId, RaftMessage message) {
        InMemoryRaftTransport target = bus.get(nodeId);
        if (target == null || target.handler == null) {
            throw new IllegalStateException("raft node not reachable: " + nodeId);
        }
        return target.handler.handle(message);
    }

    @Override
    public void close() {
        bus.remove(nodeId, this);
        handler = null;
    }
}
