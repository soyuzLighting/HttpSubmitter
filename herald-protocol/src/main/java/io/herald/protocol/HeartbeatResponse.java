package io.herald.protocol;

import java.nio.ByteBuffer;

/** 心跳响应。 */
public final class HeartbeatResponse {

    private int errorCode = ErrorCode.OK;

    public HeartbeatResponse errorCode(int v) { this.errorCode = v; return this; }
    public int errorCode() { return errorCode; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putVarInt(errorCode);
        return w.toByteArray();
    }

    public static HeartbeatResponse decode(ByteBuffer buf) {
        HeartbeatResponse r = new HeartbeatResponse();
        r.errorCode = ByteReader.readVarInt(buf);
        return r;
    }
}
