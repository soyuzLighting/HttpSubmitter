package io.herald.protocol;

import java.nio.ByteBuffer;

/** 心跳/保活请求。 */
public final class HeartbeatRequest {

    private String clientId = "";

    public HeartbeatRequest clientId(String v) { this.clientId = v == null ? "" : v; return this; }
    public String clientId() { return clientId; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putString(clientId);
        return w.toByteArray();
    }

    public static HeartbeatRequest decode(ByteBuffer buf) {
        HeartbeatRequest r = new HeartbeatRequest();
        r.clientId = ByteReader.readString(buf);
        return r;
    }
}
