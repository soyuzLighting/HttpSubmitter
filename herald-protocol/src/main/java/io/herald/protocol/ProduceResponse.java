package io.herald.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** 生产响应：返回写入的 baseOffset 与每条消息的 offset。 */
public final class ProduceResponse {

    private int errorCode = ErrorCode.OK;
    private long baseOffset = -1;
    private final List<Long> offsets = new ArrayList<>();

    public ProduceResponse errorCode(int v) { this.errorCode = v; return this; }
    public ProduceResponse baseOffset(long v) { this.baseOffset = v; return this; }
    public ProduceResponse addOffset(long v) { this.offsets.add(v); return this; }

    public int errorCode() { return errorCode; }
    public long baseOffset() { return baseOffset; }
    public List<Long> offsets() { return offsets; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putVarInt(errorCode);
        w.putLong(baseOffset);
        w.putVarInt(offsets.size());
        for (long o : offsets) {
            w.putLong(o);
        }
        return w.toByteArray();
    }

    public static ProduceResponse decode(ByteBuffer buf) {
        ProduceResponse r = new ProduceResponse();
        r.errorCode = ByteReader.readVarInt(buf);
        r.baseOffset = ByteReader.readLong(buf);
        int count = ByteReader.readVarInt(buf);
        for (int i = 0; i < count; i++) {
            r.offsets.add(ByteReader.readLong(buf));
        }
        return r;
    }
}
