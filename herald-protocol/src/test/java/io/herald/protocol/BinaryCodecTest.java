package io.herald.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BinaryCodecTest {

    @Test
    void varIntRoundTrip() {
        int[] values = {0, 1, 127, 128, 300, 16383, 16384, Integer.MAX_VALUE, -1};
        for (int v : values) {
            ByteWriter w = new ByteWriter();
            w.putVarInt(v);
            assertEquals(v, ByteReader.readVarInt(ByteBuffer.wrap(w.toByteArray())), "value " + v);
        }
    }

    @Test
    void stringAndBytesRoundTrip() {
        ByteWriter w = new ByteWriter();
        w.putString("你好 herald");
        w.putString("");
        w.putBytes(new byte[]{1, 2, 3});
        w.putBytes(new byte[0]);

        ByteBuffer buf = ByteBuffer.wrap(w.toByteArray());
        assertEquals("你好 herald", ByteReader.readString(buf));
        assertEquals("", ByteReader.readString(buf));
        assertArrayEquals(new byte[]{1, 2, 3}, ByteReader.readBytes(buf));
        assertArrayEquals(new byte[0], ByteReader.readBytes(buf));
    }

    @Test
    void longRoundTrip() {
        long[] values = {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 0x0102030405060708L};
        for (long v : values) {
            ByteWriter w = new ByteWriter();
            w.putLong(v);
            assertEquals(v, ByteReader.readLong(ByteBuffer.wrap(w.toByteArray())));
        }
    }
}
