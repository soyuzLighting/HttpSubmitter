package io.herald.storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个段（segment）：磁盘上一个按起始 offset 命名的 {@code .log} 文件。
 *
 * <p>创建时预分配（稀疏）到 {@code segmentBytes} 并整体 mmap，写与读都直接操作映射缓冲区，
 * 实现顺序追加写 + 零拷贝读。刷盘通过 {@link MappedByteBuffer#force(int, int)} 只刷增量区间。</p>
 */
public final class LogSegment {

    private final Path file;
    private final long baseOffset;
    private final int segmentBytes;
    private final int indexIntervalBytes;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final ByteBuffer writeBuf;
    private final OffsetIndex index = new OffsetIndex();

    private long writePosition;    // 逻辑有效结束位置（字节）
    private long flushedPosition;  // 已刷盘位置（字节）
    private int lastIndexPosition; // 上次写索引的位置（字节）

    private LogSegment(Path file, long baseOffset, LogConfig config,
                       RandomAccessFile raf, FileChannel channel, MappedByteBuffer buffer) {
        this.file = file;
        this.baseOffset = baseOffset;
        this.segmentBytes = (int) config.segmentBytes();
        this.indexIntervalBytes = config.indexIntervalBytes();
        this.raf = raf;
        this.channel = channel;
        this.buffer = buffer;
        this.writeBuf = buffer.duplicate();
        this.writePosition = 0;
        this.flushedPosition = 0;
        this.lastIndexPosition = 0;
    }

    static LogSegment create(Path file, long baseOffset, LogConfig config) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
        raf.setLength(config.segmentBytes()); // 稀疏预分配
        FileChannel channel = raf.getChannel();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, config.segmentBytes());
        return new LogSegment(file, baseOffset, config, raf, channel, buffer);
    }

    static LogSegment open(Path file, long baseOffset, LogConfig config, boolean writable) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file.toFile(), writable ? "rw" : "r");
        long fileLen = raf.length();
        if (writable && fileLen < config.segmentBytes()) {
            raf.setLength(config.segmentBytes());
            fileLen = config.segmentBytes();
        }
        FileChannel channel = raf.getChannel();
        MappedByteBuffer buffer = channel.map(
                writable ? FileChannel.MapMode.READ_WRITE : FileChannel.MapMode.READ_ONLY, 0, fileLen);
        return new LogSegment(file, baseOffset, config, raf, channel, buffer);
    }

    /**
     * 重建索引并找到有效结束位置：从头扫描记录，遇到第一条损坏/半写记录即停止。
     *
     * @return 段内最高有效 offset；若段为空则返回 {@code baseOffset - 1}
     */
    long rebuild() {
        index.clear();
        writePosition = 0;
        flushedPosition = 0;
        lastIndexPosition = 0;
        long pos = 0;
        long lastOffset = baseOffset - 1;
        ByteBuffer buf = buffer.duplicate();
        buf.position(0);
        while (buf.remaining() >= LogRecord.HEADER_SIZE) {
            LogRecord.Decoded d = LogRecord.decode(buf);
            if (d == null) {
                break;
            }
            long rel = d.offset - baseOffset;
            if (rel < 0 || d.offset <= lastOffset) {
                break;
            }
            maybeIndex(rel, pos);
            pos += d.size;
            lastOffset = d.offset;
        }
        writePosition = pos;
        return lastOffset;
    }

    void append(long offset, byte[] payload) {
        int pos = (int) writePosition;
        maybeIndex(offset - baseOffset, pos);
        writeBuf.position(pos);
        LogRecord.write(writeBuf, offset, payload);
        writePosition = pos + LogRecord.sizeOf(payload.length);
    }

    byte[] read(long offset) {
        if (offset < baseOffset) {
            return null;
        }
        long pos = index.lookup(offset - baseOffset);
        if (pos < 0) {
            return null;
        }
        ByteBuffer buf = buffer.duplicate();
        buf.position((int) pos);
        long limit = writePosition;
        while (buf.position() < limit) {
            LogRecord.Decoded d = LogRecord.decode(buf);
            if (d == null) {
                return null;
            }
            if (d.offset == offset) {
                return d.payload;
            }
            if (d.offset > offset) {
                return null;
            }
        }
        return null;
    }

    /**
     * 从 {@code startOffset} 起顺序读取一批记录。
     *
     * @param maxCount 最多返回的条数
     * @param maxBytes 返回条目负载总字节数的上限；首个条目即使超限也会返回以推进进度
     */
    List<LogEntry> readBatch(long startOffset, int maxCount, int maxBytes) {
        List<LogEntry> out = new ArrayList<>();
        if (maxCount <= 0) {
            return out;
        }
        long rel = startOffset - baseOffset;
        if (rel < 0) {
            rel = 0;
        }
        long pos = index.lookup(rel);
        if (pos < 0) {
            return out;
        }
        ByteBuffer buf = buffer.duplicate();
        buf.position((int) pos);
        long limit = writePosition;
        int bytes = 0;
        while (buf.position() < limit && out.size() < maxCount) {
            LogRecord.Decoded d = LogRecord.decode(buf);
            if (d == null) {
                break;
            }
            if (d.offset < startOffset) {
                continue; // 索引粒度导致落在目标之前，跳过
            }
            if (bytes + d.payload.length > maxBytes && !out.isEmpty()) {
                break;
            }
            out.add(new LogEntry(d.offset, d.payload));
            bytes += d.payload.length;
        }
        return out;
    }

    boolean canAppend(int recordSize) {
        return writePosition + recordSize <= segmentBytes;
    }

    /** 将 [flushedPosition, writePosition) 区间强制刷盘。 */
    void flush() {
        if (writePosition <= flushedPosition) {
            return;
        }
        buffer.force((int) flushedPosition, (int) (writePosition - flushedPosition));
        flushedPosition = writePosition;
    }

    void close() throws IOException {
        try {
            flush();
        } finally {
            try {
                channel.close();
            } finally {
                raf.close();
            }
        }
    }

    private void maybeIndex(long relOffset, long pos) {
        if (index.isEmpty() || pos - lastIndexPosition >= indexIntervalBytes) {
            index.append(relOffset, pos);
            lastIndexPosition = (int) pos;
        }
    }

    long baseOffset() {
        return baseOffset;
    }

    /** 有效数据大小（字节）。 */
    long size() {
        return writePosition;
    }

    Path file() {
        return file;
    }
}
