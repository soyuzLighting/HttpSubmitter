package io.herald.storage;

/**
 * 分区日志的持久化配置。字段名带括号的调用形式为流式 setter（返回 this），
 * 无参数形式为 getter，例如 {@code new LogConfig().segmentBytes(1024).flushMode(FlushMode.SYNC)}。
 */
public final class LogConfig {

    public static final long DEFAULT_SEGMENT_BYTES = 512L * 1024 * 1024;
    public static final int DEFAULT_INDEX_INTERVAL_BYTES = 4096;
    public static final int DEFAULT_MAX_RECORD_SIZE = 10 * 1024 * 1024;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 100;
    public static final int DEFAULT_FLUSH_MESSAGES = 10000;

    /** 单个段文件的最大大小（字节）。写满后滚动到新段。 */
    private long segmentBytes = DEFAULT_SEGMENT_BYTES;
    /** 稀疏索引间隔：每写入这么多字节记录一条索引。 */
    private int indexIntervalBytes = DEFAULT_INDEX_INTERVAL_BYTES;
    /** 单条消息最大大小（字节）。 */
    private int maxRecordSize = DEFAULT_MAX_RECORD_SIZE;
    /** 刷盘模式。 */
    private FlushMode flushMode = FlushMode.ASYNC;
    /** 异步刷盘的时间间隔（毫秒）。 */
    private long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
    /** 异步刷盘的消息条数阈值。 */
    private int flushMessages = DEFAULT_FLUSH_MESSAGES;

    public long segmentBytes() {
        return segmentBytes;
    }

    public LogConfig segmentBytes(long segmentBytes) {
        this.segmentBytes = segmentBytes;
        return this;
    }

    public int indexIntervalBytes() {
        return indexIntervalBytes;
    }

    public LogConfig indexIntervalBytes(int indexIntervalBytes) {
        this.indexIntervalBytes = indexIntervalBytes;
        return this;
    }

    public int maxRecordSize() {
        return maxRecordSize;
    }

    public LogConfig maxRecordSize(int maxRecordSize) {
        this.maxRecordSize = maxRecordSize;
        return this;
    }

    public FlushMode flushMode() {
        return flushMode;
    }

    public LogConfig flushMode(FlushMode flushMode) {
        this.flushMode = flushMode;
        return this;
    }

    public long flushIntervalMs() {
        return flushIntervalMs;
    }

    public LogConfig flushIntervalMs(long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
        return this;
    }

    public int flushMessages() {
        return flushMessages;
    }

    public LogConfig flushMessages(int flushMessages) {
        this.flushMessages = flushMessages;
        return this;
    }
}
