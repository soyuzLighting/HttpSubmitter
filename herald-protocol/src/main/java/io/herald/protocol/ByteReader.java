package io.herald.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** 与 {@link ByteWriter} 对应的二进制读出器，基于 {@link ByteBuffer}。 */
public final class ByteReader {

    private ByteReader() {
    }

    public static byte readByte(ByteBuffer buf) {
        return buf.get();
    }

    public static int readVarInt(ByteBuffer buf) {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = buf.get();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalArgumentException("malformed varint");
            }
        }
    }

    public static long readLong(ByteBuffer buf) {
        return buf.getLong();
    }

    public static String readString(ByteBuffer buf) {
        int len = readVarInt(buf);
        byte[] b = new byte[len];
        buf.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    public static byte[] readBytes(ByteBuffer buf) {
        int len = readVarInt(buf);
        byte[] b = new byte[len];
        buf.get(b);
        return b;
    }
}
