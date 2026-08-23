package io.herald.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Raft 日志：以 1 为起始下标（index 0 为哨兵，term=0）。
 * 传入持久化目录时以 WAL 追加写落盘（每条 fsync），重启时重放重建状态机；否则纯内存。
 *
 * <p>磁盘记录格式（大端）：</p>
 * <pre>
 * +--------------+----------+----------+-----------+
 * | recordBytes  | term     | cmdLen   | command   |
 * | int32        | int64    | int32    | cmdLen    |
 * +--------------+----------+----------+-----------+
 * </pre>
 * <p>{@code recordBytes} 为紧随其后（term + cmdLen + command）的字节数，用于界定记录边界；
 * 加载时遇半写记录（长度越界/截断）则丢弃尾部。</p>
 */
final class RaftLog implements AutoCloseable {

    /** 一条日志项：任期与命令负载。 */
    record Entry(long term, byte[] command) {
    }

    private final List<Entry> entries = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>(); // entries[i] 在文件中的起始字节偏移
    private final FileChannel channel; // null => 纯内存

    RaftLog() {
        this.channel = null;
        entries.add(new Entry(0, new byte[0]));
        offsets.add(0L);
    }

    RaftLog(Path dir) {
        entries.add(new Entry(0, new byte[0]));
        offsets.add(0L);
        FileChannel ch = null;
        try {
            Files.createDirectories(dir);
            ch = FileChannel.open(dir.resolve("raft.log"),
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            load(ch);
        } catch (IOException e) {
            entries.clear();
            entries.add(new Entry(0, new byte[0]));
            offsets.clear();
            offsets.add(0L);
            ch = null;
        }
        this.channel = ch;
    }

    long lastIndex() {
        return entries.size() - 1L;
    }

    long lastTerm() {
        return termAt(lastIndex());
    }

    long termAt(long index) {
        return entries.get((int) index).term();
    }

    Entry entryAt(long index) {
        return entries.get((int) index);
    }

    /** 追加一条，返回其下标。 */
    long append(long term, byte[] command) {
        Entry entry = new Entry(term, command);
        if (channel != null) {
            try {
                long start = channel.size();
                ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 4 + command.length);
                buf.putInt(8 + 4 + command.length); // recordBytes
                buf.putLong(term);
                buf.putInt(command.length);
                buf.put(command);
                buf.flip();
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }
                channel.force(true);
                offsets.add(start);
            } catch (IOException e) {
                throw new IllegalStateException("raft log append failed", e);
            }
        }
        entries.add(entry);
        return lastIndex();
    }

    /**
     * 追加 AppendEntries 携带的日志：校验 {@code prevLogIndex/prevLogTerm} 是否匹配，
     * 匹配则截断冲突并追加，返回 true；否则 false。
     */
    boolean appendEntries(long prevLogIndex, long prevLogTerm, List<Entry> incoming) {
        if (prevLogIndex > lastIndex() || termAt(prevLogIndex) != prevLogTerm) {
            return false;
        }
        if (incoming.isEmpty()) {
            return true;
        }
        int firstNew = (int) prevLogIndex + 1;
        for (int i = 0; i < incoming.size(); i++) {
            long idx = firstNew + i;
            if (idx <= lastIndex() && termAt(idx) != incoming.get(i).term()) {
                truncateFrom(idx);
                break;
            }
        }
        for (int i = 0; i < incoming.size(); i++) {
            long idx = firstNew + i;
            if (idx > lastIndex()) {
                append(incoming.get(i).term(), incoming.get(i).command());
            }
        }
        return true;
    }

    /** 从 {@code fromIndex} 起截断（leader 回退 follower 时丢弃未提交日志）。 */
    void truncateFrom(long fromIndex) {
        long targetOffset = 0;
        if (channel != null) {
            targetOffset = offsets.get((int) fromIndex);
        }
        while (entries.size() > fromIndex) {
            entries.remove(entries.size() - 1);
            if (channel != null) {
                offsets.remove(offsets.size() - 1);
            }
        }
        if (channel != null) {
            try {
                channel.truncate(targetOffset);
                channel.force(true);
            } catch (IOException e) {
                throw new IllegalStateException("raft log truncate failed", e);
            }
        }
    }

    /** 复制 [{@code fromIndex}, {@code lastIndex}] 的日志项。 */
    List<Entry> slice(long fromIndex) {
        List<Entry> out = new ArrayList<>();
        for (long i = fromIndex; i <= lastIndex(); i++) {
            out.add(entryAt(i));
        }
        return out;
    }

    private void load(FileChannel ch) throws IOException {
        long size = ch.size();
        long pos = 0;
        long lastGoodEnd = 0;
        while (pos + 4 <= size) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (ch.read(lenBuf, pos) != 4) {
                break;
            }
            lenBuf.flip();
            int recordBytes = lenBuf.getInt();
            if (recordBytes < 12 || pos + 4 + recordBytes > size) {
                break; // 半写记录
            }
            ByteBuffer rec = ByteBuffer.allocate(recordBytes);
            if (ch.read(rec, pos + 4) != recordBytes) {
                break;
            }
            rec.flip();
            long term = rec.getLong();
            int cmdLen = rec.getInt();
            if (cmdLen != recordBytes - 12) {
                break;
            }
            byte[] cmd = new byte[cmdLen];
            rec.get(cmd);
            entries.add(new Entry(term, cmd));
            offsets.add(pos);
            lastGoodEnd = pos + 4 + recordBytes;
            pos = lastGoodEnd;
        }
        if (lastGoodEnd < size) {
            ch.truncate(lastGoodEnd);
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
    }
}
