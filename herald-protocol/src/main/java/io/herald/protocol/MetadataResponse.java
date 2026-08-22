package io.herald.protocol;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/** 元数据响应：topic -> 分区数。 */
public final class MetadataResponse {

    private int errorCode = ErrorCode.OK;
    private final Map<String, Integer> topics = new LinkedHashMap<>();

    public MetadataResponse errorCode(int v) { this.errorCode = v; return this; }
    public MetadataResponse addTopic(String topic, int partitions) { this.topics.put(topic, partitions); return this; }

    public int errorCode() { return errorCode; }
    public Map<String, Integer> topics() { return topics; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putVarInt(errorCode);
        w.putVarInt(topics.size());
        for (Map.Entry<String, Integer> e : topics.entrySet()) {
            w.putString(e.getKey());
            w.putVarInt(e.getValue());
        }
        return w.toByteArray();
    }

    public static MetadataResponse decode(ByteBuffer buf) {
        MetadataResponse r = new MetadataResponse();
        r.errorCode = ByteReader.readVarInt(buf);
        int count = ByteReader.readVarInt(buf);
        for (int i = 0; i < count; i++) {
            r.topics.put(ByteReader.readString(buf), ByteReader.readVarInt(buf));
        }
        return r;
    }
}
