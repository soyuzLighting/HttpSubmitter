package io.herald.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 雪花算法 ID 生成器：64 位 = 1 符号位 + 41 时间戳 + 10 节点 + 12 序列。
 * 线程安全（同步）；时钟回拨时保持单调递增。
 */
public final class SnowflakeIdGenerator {

    private static final long EPOCH = 1700000000000L; // 2023-11-14，任取
    private static final long NODE_BITS = 10;
    private static final long SEQ_BITS = 12;
    private static final long MAX_NODE = (1L << NODE_BITS) - 1;
    private static final long MAX_SEQ = (1L << SEQ_BITS) - 1;
    private static final long SEQ_SHIFT = 0;
    private static final long NODE_SHIFT = SEQ_BITS;
    private static final long TIME_SHIFT = SEQ_BITS + NODE_BITS;

    private final long nodeId;
    private final AtomicLong seq = new AtomicLong();
    private long lastTimestamp = -1L;

    public SnowflakeIdGenerator(long nodeId) {
        if (nodeId < 0 || nodeId > MAX_NODE) {
            throw new IllegalArgumentException("nodeId must be in [0, " + MAX_NODE + "]");
        }
        this.nodeId = nodeId;
    }

    public synchronized long nextId() {
        long ts = System.currentTimeMillis();
        if (ts < lastTimestamp) {
            ts = lastTimestamp; // 时钟回拨：保持单调
        }
        if (ts == lastTimestamp) {
            long s = (seq.incrementAndGet()) & MAX_SEQ;
            if (s == 0) {
                while (ts <= lastTimestamp) {
                    ts = System.currentTimeMillis();
                }
                seq.set(0);
            }
        } else {
            seq.set(0);
        }
        lastTimestamp = ts;
        return ((ts - EPOCH) << TIME_SHIFT) | (nodeId << NODE_SHIFT) | (seq.get() << SEQ_SHIFT);
    }
}
