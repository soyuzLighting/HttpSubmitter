package io.herald.storage;

/**
 * 刷盘策略。
 *
 * <p>{@link #ASYNC} 批量异步刷盘（依赖页缓存，吞吐最高，宕机最多丢一个刷盘窗口）；
 * {@link #SYNC} 每条消息同步刷盘（零丢失，吞吐下降）。</p>
 */
public enum FlushMode {
    /** 批量异步刷盘，吞吐优先。 */
    ASYNC,
    /** 每条消息同步刷盘，可靠性优先。 */
    SYNC
}
