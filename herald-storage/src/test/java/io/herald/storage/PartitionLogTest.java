package io.herald.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionLogTest {

    @TempDir
    Path tmp;

    @Test
    void appendReadAndReopen() throws IOException {
        Path dir = tmp.resolve("t0");
        LogConfig cfg = new LogConfig().segmentBytes(1024 * 1024);
        try (PartitionLog log = PartitionLog.open(dir, cfg)) {
            for (int i = 0; i < 100; i++) {
                log.append(("m" + i).getBytes());
            }
            assertEquals(100, log.nextOffset());
            for (int i = 0; i < 100; i++) {
                assertArrayEquals(("m" + i).getBytes(), log.read(i));
            }
        }
        try (PartitionLog reopened = PartitionLog.open(dir, cfg)) {
            assertEquals(100, reopened.nextOffset());
            assertArrayEquals("m42".getBytes(), reopened.read(42));
        }
    }

    @Test
    void segmentRolling() throws IOException {
        Path dir = tmp.resolve("t1");
        // 每段只能放一条 4 字节负载的记录（24 字节/条），50 条 => 50 段
        LogConfig cfg = new LogConfig().segmentBytes(LogRecord.HEADER_SIZE + 4);
        byte[] payload = new byte[4];
        try (PartitionLog log = PartitionLog.open(dir, cfg)) {
            for (int i = 0; i < 50; i++) {
                log.append(payload);
            }
            assertTrue(log.segmentCount() > 1);
            assertEquals(50, log.nextOffset());
        }
        try (PartitionLog reopened = PartitionLog.open(dir, cfg)) {
            assertEquals(50, reopened.nextOffset());
            for (int i = 0; i < 50; i++) {
                assertArrayEquals(payload, reopened.read(i));
            }
        }
    }

    @Test
    void tornTailDiscardedOnRecovery() throws IOException {
        Path dir = tmp.resolve("t2");
        LogConfig cfg = new LogConfig().segmentBytes(1024 * 1024);
        byte[] payload = new byte[100]; // 记录大小 = 120 字节
        try (PartitionLog log = PartitionLog.open(dir, cfg)) {
            for (int i = 0; i < 5; i++) {
                log.append(payload);
            }
            assertEquals(4, log.nextOffset() - 1);
        }
        // 破坏第 4 条记录（offset=3）的 CRC 字段：文件位置 3*120 + 12 = 372
        Path segFile = dir.resolve("00000000000000000000.log");
        try (RandomAccessFile raf = new RandomAccessFile(segFile.toFile(), "rw")) {
            raf.seek(372);
            raf.writeInt(0x12345678);
        }
        // 恢复应截断到第 4 条记录之前，只保留 offset 0..2
        try (PartitionLog reopened = PartitionLog.open(dir, cfg)) {
            assertEquals(3, reopened.nextOffset());
            assertArrayEquals(payload, reopened.read(0));
            assertArrayEquals(payload, reopened.read(2));
            assertNull(reopened.read(3));
            assertNull(reopened.read(4));
        }
    }

    @Test
    void bothFlushModesAppendAndRead() throws IOException {
        for (FlushMode mode : FlushMode.values()) {
            Path dir = tmp.resolve("mode-" + mode);
            LogConfig cfg = new LogConfig().segmentBytes(1024 * 1024).flushMode(mode).flushIntervalMs(10);
            try (PartitionLog log = PartitionLog.open(dir, cfg)) {
                for (int i = 0; i < 20; i++) {
                    log.append(("p" + i).getBytes());
                }
                assertArrayEquals("p19".getBytes(), log.read(19));
            }
        }
    }
}
