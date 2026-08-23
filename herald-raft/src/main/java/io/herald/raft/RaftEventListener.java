package io.herald.raft;

/** Raft 事件回调：leader 选举与节点可达性变化，供上层（如控制器）触发分区 leader 切换。 */
public interface RaftEventListener {

    /** 本节点成为 leader。 */
    void onLeaderElected(int nodeId);

    /** 某节点的可达性发生变化。 */
    void onPeerStatusChanged(int nodeId, boolean alive);
}
