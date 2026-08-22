package io.herald.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 可增长的二进制写出器（大端）。整数用无符号 LEB128 varint，字符串/字节数组用
 * varint 长度前缀。用于请求/响应/消息的编码。
 */
public final class ByteWriter {

    private byte[] data = new byte[64];
    private int size;

    public ByteWriter putByte(byte b) {
        ensure(1);
        data[size++] = b;
        return this;
    }

    public ByteWriter putShort(short v) {
        ensure(2);
        data[size++] = (byte) (v >>> 8);
        data[size++] = (byte) v;
        return this;
    }

    public ByteWriter putInt(int v) {
        ensure(4);
        data[size++] = (byte) (v >>> 24);
        data[size++] = (byte) (v >>> 16);
        data[size++] = (byte) (v >>> 8);
        data[size++] = (byte) v;
        return this;
    }

    public ByteWriter putLong(long v) {
        ensure(8);
        for (int i = 7; i >= 0; i--) {
            data[size++] = (byte) (v >>> (i * 8));
        }
        return this;
    }

    public ByteWriter putVarInt(int value) {
        int v = value;
        while ((v & ~0x7F) != 0) {
            putByte((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        return putByte((byte) v);
    }

    public ByteWriter putString(String s) {
        byte[] b = (s == null ? "" : s).getBytes(StandardCharsets.UTF_8);
        return putBytes(b);
    }

    public ByteWriter putBytes(byte[] b) {
        if (b == null || b.length == 0) {
            return putVarInt(0);
        }
        putVarInt(b.length);
        ensure(b.length);
        System.arraycopy(b, 0, data, size, b.length);
        size += b.length;
        return this;
    }

    /** 原样追加字节，不加长度前缀。 */
    public ByteWriter putRaw(byte[] b) {
        ensure(b.length);
        System.arraycopy(b, 0, data, size, b.length);
        size += b.length;
        return this;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(data, size);
    }

    public int size() {
        return size;
    }

    private void ensure(int n) {
        if (size + n > data.length) {
            data = Arrays.copyOf(data, Math.max(size + n, data.length * 2));
        }
    }
}
