package io.herald.storage;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * 磁盘记录格式（大端）：
 *
 * <pre>
 * +-------------+-----------+----------+-------------+-------------+
 * | frameLength | offset    | crc      | payloadLen  | payload     |
 * | int32       | int64     | int32    | int32       | payloadLen  |
 * +-------------+-----------+----------+-------------+-------------+
 * </pre>
 *
 * <p>{@code frameLength} 表示其后所有字节数（16 + payloadLen），用于确定记录边界；
 * {@code crc} 为 {@code offset + payload} 的 CRC32C，用于检测半写（torn write）与损坏。</p>
 */
public final class LogRecord {

    public static final int HEADER_SIZE = 20; // 4 frameLength + 8 offset + 4 crc + 4 payloadLen

    private LogRecord() {
    }

    public static int sizeOf(int payloadLen) {
        return HEADER_SIZE + payloadLen;
    }

    /**
     * 将一条记录写入缓冲区当前 position，并推进 position。
     *
     * @return 写入的总字节数（{@link #HEADER_SIZE} + payloadLen）
     */
    public static int write(ByteBuffer buf, long offset, byte[] payload) {
        int payloadLen = payload.length;
        buf.putInt(16 + payloadLen); // frameLength
        buf.putLong(offset);
        buf.putInt(crc32(offset, payload));
        buf.putInt(payloadLen);
        buf.put(payload);
        return HEADER_SIZE + payloadLen;
    }

    /**
     * 从缓冲区当前 position 解码一条记录并推进 position。
     *
     * @return 解码结果；若记录损坏/半写（CRC 不匹配、长度越界等）则返回 {@code null}，
     *         并将 position 还原到记录起点。
     */
    static Decoded decode(ByteBuffer buf) {
        if (buf.remaining() < HEADER_SIZE) {
            return null;
        }
        int start = buf.position();
        int frameLength = buf.getInt();
        if (frameLength < 16 || frameLength > buf.remaining()) {
            buf.position(start);
            return null;
        }
        long offset = buf.getLong();
        int crc = buf.getInt();
        int payloadLen = buf.getInt();
        if (payloadLen != frameLength - 16) {
            buf.position(start);
            return null;
        }
        byte[] payload = new byte[payloadLen];
        buf.get(payload);
        if (crc32(offset, payload) != crc) {
            buf.position(start);
            return null;
        }
        return new Decoded(offset, payload, HEADER_SIZE + payloadLen);
    }

    private static int crc32(long offset, byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update((byte) (offset >>> 56));
        crc.update((byte) (offset >>> 48));
        crc.update((byte) (offset >>> 40));
        crc.update((byte) (offset >>> 32));
        crc.update((byte) (offset >>> 24));
        crc.update((byte) (offset >>> 16));
        crc.update((byte) (offset >>> 8));
        crc.update((byte) offset);
        crc.update(payload, 0, payload.length);
        return (int) crc.getValue();
    }

    static final class Decoded {
        final long offset;
        final byte[] payload;
        final int size;

        Decoded(long offset, byte[] payload, int size) {
            this.offset = offset;
            this.payload = payload;
            this.size = size;
        }
    }
}
