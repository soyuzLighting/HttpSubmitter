package io.herald.protocol;

import java.nio.ByteBuffer;

/** 位点查询响应：{@code committedOffset} 为已提交位点，无记录时为 -1。 */
public final class OffsetFetchResponse {

    private int errorCode = ErrorCode.OK;
    private long committedOffset = -1;

    public OffsetFetchResponse errorCode(int v) { this.errorCode = v; return this; }
    public OffsetFetchResponse committedOffset(long v) { this.committedOffset = v; return this; }

    public int errorCode() { return errorCode; }
    public long committedOffset() { return committedOffset; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putVarInt(errorCode);
        w.putLong(committedOffset);
        return w.toByteArray();
    }

    public static OffsetFetchResponse decode(ByteBuffer buf) {
        OffsetFetchResponse r = new OffsetFetchResponse();
        r.errorCode = ByteReader.readVarInt(buf);
        r.committedOffset = ByteReader.readLong(buf);
        return r;
    }
}
