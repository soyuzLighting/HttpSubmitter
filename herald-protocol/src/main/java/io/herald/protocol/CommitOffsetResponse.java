package io.herald.protocol;

import java.nio.ByteBuffer;

/** 提交位点响应。 */
public final class CommitOffsetResponse {

    private int errorCode = ErrorCode.OK;

    public CommitOffsetResponse errorCode(int v) { this.errorCode = v; return this; }
    public int errorCode() { return errorCode; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putVarInt(errorCode);
        return w.toByteArray();
    }

    public static CommitOffsetResponse decode(ByteBuffer buf) {
        CommitOffsetResponse r = new CommitOffsetResponse();
        r.errorCode = ByteReader.readVarInt(buf);
        return r;
    }
}
