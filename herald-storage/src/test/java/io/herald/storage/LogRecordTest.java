package io.herald.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class LogRecordTest {

    @Test
    void roundtrip() {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        byte[] payload = "hello".getBytes();
        int size = LogRecord.write(buf, 42L, payload);
        assertEquals(LogRecord.sizeOf(payload.length), size);
        buf.flip();
        LogRecord.Decoded d = LogRecord.decode(buf);
        assertEquals(42L, d.offset);
        assertArrayEquals(payload, d.payload);
        assertEquals(size, d.size);
    }

    @Test
    void corruptedPayloadReturnsNull() {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        byte[] payload = {1, 2, 3};
        LogRecord.write(buf, 1L, payload);
        buf.put(LogRecord.HEADER_SIZE, (byte) 0x7F); // 破坏 payload 首字节
        buf.flip();
        assertNull(LogRecord.decode(buf));
    }

    @Test
    void truncatedFrameReturnsNull() {
        ByteBuffer buf = ByteBuffer.allocate(8); // 不足 HEADER_SIZE
        assertNull(LogRecord.decode(buf));
    }

    @Test
    void badFrameLengthReturnsNull() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.putInt(1000); // frameLength 超过 remaining
        buf.flip();
        assertNull(LogRecord.decode(buf));
    }

    @Test
    void sizeOf() {
        assertEquals(20, LogRecord.sizeOf(0));
        assertEquals(30, LogRecord.sizeOf(10));
    }
}
