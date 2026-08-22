package io.herald.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 单个分区的追加写日志：管理若干按起始 offset 有序的 {@link LogSegment}，
 * 负责追加写、按 offset 读、段滚动、双模式刷盘、崩溃恢复。
 *
 * <p>追加操作线程安全；读操作不加锁（依赖 mmap 的页缓存一致性）。</p>
 */
public final class PartitionLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PartitionLog.class);

    private final Path dir;
    private final LogConfig config;
    private final List<LogSegment> segments = new ArrayList<>();
    private final Object writeLock = new Object();

    private LogSegment activeSegment;
    private long nextOffset;
    private int pendingMessages; // 异步模式待刷盘消息计数（写锁保护）
    private ScheduledExecutorService flusher;

    private PartitionLog(Path dir, LogConfig config) {
        this.dir = dir;
        this.config = config;
    }

    /** 打开（或创建）分区日志并执行崩溃恢复。 */
    public static PartitionLog open(Path dir, LogConfig config) throws IOException {
        PartitionLog log = new PartitionLog(dir, config);
        log.recover();
        log.startFlusher();
        return log;
    }

    private void recover() throws IOException {
        Files.createDirectories(dir);
        List<Path> logFiles = listLogFiles(dir);
        long maxOffset = -1;
        for (int i = 0; i < logFiles.size(); i++) {
            Path file = logFiles.get(i);
            long base = parseBaseOffset(file);
            boolean writable = (i == logFiles.size() - 1);
            LogSegment seg = LogSegment.open(file, base, config, writable);
            long lastOffset = seg.rebuild();
            if (lastOffset > maxOffset) {
                maxOffset = lastOffset;
            }
            segments.add(seg);
        }
        if (segments.isEmpty()) {
            segments.add(LogSegment.create(newSegmentFile(dir, 0), 0, config));
        }
        activeSegment = segments.get(segments.size() - 1);
        nextOffset = maxOffset + 1;
    }

    private void startFlusher() {
        if (config.flushMode() == FlushMode.ASYNC) {
            flusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "herald-flusher");
                t.setDaemon(true);
                return t;
            });
            flusher.scheduleWithFixedDelay(this::flush, config.flushIntervalMs(),
                    config.flushIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    /** 追加一条消息，返回分配到的 offset。 */
    public long append(byte[] payload) throws IOException {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (payload.length > config.maxRecordSize()) {
            throw new IllegalArgumentException("payload exceeds maxRecordSize=" + config.maxRecordSize());
        }
        int recordSize = LogRecord.sizeOf(payload.length);
        synchronized (writeLock) {
            if (!activeSegment.canAppend(recordSize)) {
                rollToNewSegment();
            }
            long offset = nextOffset;
            activeSegment.append(offset, payload);
            nextOffset = offset + 1;
            if (config.flushMode() == FlushMode.SYNC) {
                activeSegment.flush();
            } else if (++pendingMessages >= config.flushMessages()) {
                activeSegment.flush();
                pendingMessages = 0;
            }
            return offset;
        }
    }

    /** 读取指定 offset 的消息负载；不存在（已清理/越界）返回 null。 */
    public byte[] read(long offset) {
        LogSegment seg = findSegment(offset);
        return seg == null ? null : seg.read(offset);
    }

    /** 立即刷盘（异步模式下通常由后台线程触发）。 */
    public void flush() {
        synchronized (writeLock) {
            if (activeSegment != null) {
                activeSegment.flush();
                pendingMessages = 0;
            }
        }
    }

    public long nextOffset() {
        return nextOffset;
    }

    public long startOffset() {
        return segments.get(0).baseOffset();
    }

    public int segmentCount() {
        return segments.size();
    }

    @Override
    public void close() {
        synchronized (writeLock) {
            if (flusher != null) {
                flusher.shutdownNow();
            }
            for (LogSegment seg : segments) {
                try {
                    seg.close();
                } catch (IOException e) {
                    log.warn("failed to close segment {}", seg.file(), e);
                }
            }
        }
    }

    private void rollToNewSegment() throws IOException {
        activeSegment.close();
        long base = nextOffset;
        LogSegment seg = LogSegment.create(newSegmentFile(dir, base), base, config);
        segments.add(seg);
        activeSegment = seg;
    }

    private LogSegment findSegment(long offset) {
        for (int i = segments.size() - 1; i >= 0; i--) {
            LogSegment seg = segments.get(i);
            if (seg.baseOffset() <= offset) {
                return seg;
            }
        }
        return null;
    }

    private static List<Path> listLogFiles(Path dir) throws IOException {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.log")) {
            for (Path p : ds) {
                files.add(p);
            }
        }
        files.sort(Comparator.comparingLong(PartitionLog::parseBaseOffset));
        return files;
    }

    private static long parseBaseOffset(Path file) {
        String name = file.getFileName().toString();
        return Long.parseLong(name.substring(0, name.length() - 4)); // 去掉 ".log"
    }

    private static Path newSegmentFile(Path dir, long baseOffset) {
        return dir.resolve(String.format("%020d.log", baseOffset));
    }
}
