package io.herald.protocol;

import java.nio.ByteBuffer;

/** 拉取请求：从指定 offset 起拉取最多 maxCount 条、maxBytes 字节。 */
public final class FetchRequest {

    private String topic = "";
    private int partition;
    private long fetchOffset;
    private int maxBytes = 1024 * 1024;
    private int maxCount = 500;

    public FetchRequest topic(String v) { this.topic = v == null ? "" : v; return this; }
    public FetchRequest partition(int v) { this.partition = v; return this; }
    public FetchRequest fetchOffset(long v) { this.fetchOffset = v; return this; }
    public FetchRequest maxBytes(int v) { this.maxBytes = v; return this; }
    public FetchRequest maxCount(int v) { this.maxCount = v; return this; }

    public String topic() { return topic; }
    public int partition() { return partition; }
    public long fetchOffset() { return fetchOffset; }
    public int maxBytes() { return maxBytes; }
    public int maxCount() { return maxCount; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putString(topic);
        w.putVarInt(partition);
        w.putLong(fetchOffset);
        w.putVarInt(maxBytes);
        w.putVarInt(maxCount);
        return w.toByteArray();
    }

    public static FetchRequest decode(ByteBuffer buf) {
        FetchRequest r = new FetchRequest();
        r.topic = ByteReader.readString(buf);
        r.partition = ByteReader.readVarInt(buf);
        r.fetchOffset = ByteReader.readLong(buf);
        r.maxBytes = ByteReader.readVarInt(buf);
        r.maxCount = ByteReader.readVarInt(buf);
        return r;
    }
}
