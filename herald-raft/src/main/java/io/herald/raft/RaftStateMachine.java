package io.herald.raft;

/**
 * Raft 状态机：按日志顺序应用已提交命令。实现必须幂等（重放安全）且线程安全，
 * 因为 apply 可能在心跳线程或提交线程上被调用。
 */
public interface RaftStateMachine {

    /** 应用一条已提交的命令。 */
    void apply(long index, byte[] command);
}
