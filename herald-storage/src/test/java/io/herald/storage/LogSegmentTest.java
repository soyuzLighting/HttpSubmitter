package io.herald.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogSegmentTest {

    @TempDir
    Path tmp;

    @Test
    void appendAndRead() throws IOException {
        LogConfig cfg = new LogConfig().segmentBytes(1024 * 1024).indexIntervalBytes(256);
        LogSegment seg = LogSegment.create(tmp.resolve("00000000000000000000.log"), 0, cfg);
        for (int i = 0; i < 10; i++) {
            seg.append(i, ("msg" + i).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            assertArrayEquals(("msg" + i).getBytes(), seg.read(i));
        }
        assertNull(seg.read(10));
        seg.close();
    }

    @Test
    void sparseIndexStillFindsEveryRecord() throws IOException {
        // 索引间隔远大于段内总数据，只有第一条记录入索引，读仍应通过顺序扫描命中
        LogConfig cfg = new LogConfig().segmentBytes(1024 * 1024).indexIntervalBytes(4096);
        LogSegment seg = LogSegment.create(tmp.resolve("00000000000000000000.log"), 0, cfg);
        for (int i = 0; i < 200; i++) {
            seg.append(i, ("v" + i).getBytes());
        }
        assertArrayEquals("v199".getBytes(), seg.read(199));
        assertArrayEquals("v0".getBytes(), seg.read(0));
        seg.close();
    }

    @Test
    void fullDetection() throws IOException {
        LogConfig cfg = new LogConfig().segmentBytes(LogRecord.HEADER_SIZE + 10);
        LogSegment seg = LogSegment.create(tmp.resolve("00000000000000000000.log"), 0, cfg);
        int recordSize = LogRecord.sizeOf(10);
        assertTrue(seg.canAppend(recordSize));
        seg.append(0, new byte[10]);
        assertFalse(seg.canAppend(recordSize));
        seg.close();
    }
}
