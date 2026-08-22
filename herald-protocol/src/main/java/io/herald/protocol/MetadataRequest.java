package io.herald.protocol;

import java.nio.ByteBuffer;

/** 元数据请求：topic 为空表示查询全部。 */
public final class MetadataRequest {

    private String topic = "";

    public MetadataRequest topic(String v) { this.topic = v == null ? "" : v; return this; }
    public String topic() { return topic; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putString(topic);
        return w.toByteArray();
    }

    public static MetadataRequest decode(ByteBuffer buf) {
        MetadataRequest r = new MetadataRequest();
        r.topic = ByteReader.readString(buf);
        return r;
    }
}
