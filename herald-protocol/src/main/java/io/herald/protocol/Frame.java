package io.herald.protocol;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 传输帧：
 *
 * <pre>
 * magic(2) version(1) opcode(1) frameLen(4) | header(变长KV) | body(原始字节)
 * </pre>
 *
 * <p>{@code frameLen} 为 header + body 的总字节数（即其后所有字节）。header 目前仅承载
 * {@code requestId}，用于请求/响应关联。</p>
 */
public final class Frame {

    public static final short MAGIC = (short) 0x4845; // "HE"
    public static final byte VERSION = 1;
    public static final int HEADER_SIZE = 8;
    public static final String REQUEST_ID = "requestId";

    private final byte opcode;
    private final Map<String, String> header;
    private final byte[] body;

    public Frame(byte opcode, Map<String, String> header, byte[] body) {
        this.opcode = opcode;
        this.header = header == null ? new LinkedHashMap<>() : header;
        this.body = body == null ? new byte[0] : body;
    }

    public byte opcode() {
        return opcode;
    }

    public Map<String, String> header() {
        return header;
    }

    public String header(String key) {
        return header.get(key);
    }

    public byte[] body() {
        return body;
    }

    public byte[] encode() {
        ByteWriter h = new ByteWriter();
        h.putVarInt(header.size());
        for (Map.Entry<String, String> e : header.entrySet()) {
            h.putString(e.getKey());
            h.putString(e.getValue());
        }
        byte[] headerBytes = h.toByteArray();

        ByteWriter w = new ByteWriter();
        w.putShort(MAGIC);
        w.putByte(VERSION);
        w.putByte(opcode);
        w.putInt(headerBytes.length + body.length);
        w.putRaw(headerBytes);
        w.putRaw(body);
        return w.toByteArray();
    }

    public static Frame decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        short magic = buf.getShort();
        if (magic != MAGIC) {
            throw new IllegalArgumentException("bad magic: 0x" + Integer.toHexString(magic & 0xFFFF));
        }
        byte version = buf.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported version: " + version);
        }
        byte opcode = buf.get();
        int frameLen = buf.getInt();
        if (frameLen != buf.remaining()) {
            throw new IllegalArgumentException("frame length mismatch: " + frameLen + " vs " + buf.remaining());
        }
        int count = ByteReader.readVarInt(buf);
        Map<String, String> header = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            header.put(ByteReader.readString(buf), ByteReader.readString(buf));
        }
        byte[] body = new byte[buf.remaining()];
        buf.get(body);
        return new Frame(opcode, header, body);
    }
}
