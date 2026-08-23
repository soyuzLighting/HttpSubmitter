package io.herald.raft;

/**
 * Raft 节点间同步 RPC 传输抽象。实现负责把 {@link #send} 请求路由到目标节点并返回响应。
 */
public interface RaftTransport extends AutoCloseable {

    /** 入站消息处理器：对接收到的消息同步返回响应。 */
    interface MessageHandler {
        RaftMessage handle(RaftMessage message);
    }

    /** 启动并注册本地处理器。 */
    void start(MessageHandler handler);

    /** 向目标节点发送消息并阻塞等待响应。 */
    RaftMessage send(int nodeId, RaftMessage message);

    @Override
    void close();
}
