package io.herald.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Raft WAL 持久化测试：追加/重载、冲突截断、半写尾部丢弃。
 */
class RaftLogTest {

    @TempDir
    Path tmp;

    @Test
    void persistAndReload() {
        RaftLog log = new RaftLog(tmp);
        log.append(1, "a".getBytes(StandardCharsets.UTF_8));
        log.append(2, "bb".getBytes(StandardCharsets.UTF_8));
        log.append(2, "ccc".getBytes(StandardCharsets.UTF_8));
        log.close();

        RaftLog reloaded = new RaftLog(tmp);
        assertEquals(3, reloaded.lastIndex());
        assertEquals(2, reloaded.lastTerm());
        assertEquals(1, reloaded.termAt(1));
        assertEquals("a", new String(reloaded.entryAt(1).command(), StandardCharsets.UTF_8));
        assertEquals("ccc", new String(reloaded.entryAt(3).command(), StandardCharsets.UTF_8));
        reloaded.close();
    }

    @Test
    void truncateFromPersists() {
        RaftLog log = new RaftLog(tmp);
        log.append(1, "a".getBytes(StandardCharsets.UTF_8));
        log.append(1, "b".getBytes(StandardCharsets.UTF_8));
        log.append(1, "c".getBytes(StandardCharsets.UTF_8));
        log.truncateFrom(2);
        assertEquals(1, log.lastIndex());
        log.close();

        RaftLog reloaded = new RaftLog(tmp);
        assertEquals(1, reloaded.lastIndex());
        assertEquals("a", new String(reloaded.entryAt(1).command(), StandardCharsets.UTF_8));
        reloaded.close();
    }

    @Test
    void appendEntriesTruncatesConflictingTail() {
        RaftLog log = new RaftLog(tmp);
        log.append(1, "a".getBytes(StandardCharsets.UTF_8));
        log.append(1, "b".getBytes(StandardCharsets.UTF_8));

        List<RaftLog.Entry> incoming = List.of(new RaftLog.Entry(2, "b2".getBytes(StandardCharsets.UTF_8)));
        boolean ok = log.appendEntries(1, 1, incoming);
        assertTrue(ok);
        assertEquals(2, log.lastIndex());
        assertEquals(2, log.termAt(2));
        assertEquals("b2", new String(log.entryAt(2).command(), StandardCharsets.UTF_8));
        log.close();
    }

    @Test
    void tornTailDiscardedOnReload() throws Exception {
        RaftLog log = new RaftLog(tmp);
        log.append(1, "a".getBytes(StandardCharsets.UTF_8));
        log.append(1, "b".getBytes(StandardCharsets.UTF_8));
        log.close();

        // 模拟崩溃半写：追加一个只写了 recordBytes 头、未写 payload 的不完整记录
        try (FileChannel ch = FileChannel.open(tmp.resolve("raft.log"),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer partial = ByteBuffer.allocate(4);
            partial.putInt(100); // 声称后面还有 100 字节，但实际没有
            partial.flip();
            ch.write(partial);
        }

        RaftLog reloaded = new RaftLog(tmp);
        assertEquals(2, reloaded.lastIndex(), "torn tail should be discarded");
        assertEquals("b", new String(reloaded.entryAt(2).command(), StandardCharsets.UTF_8));
        reloaded.close();
    }
}
