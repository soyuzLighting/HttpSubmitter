package io.herald.protocol;

import java.nio.ByteBuffer;

/** 查询某消费组在指定 topic/partition 的已提交位点。 */
public final class OffsetFetchRequest {

    private String groupId = "";
    private String topic = "";
    private int partition = 0;

    public OffsetFetchRequest groupId(String v) { this.groupId = v == null ? "" : v; return this; }
    public OffsetFetchRequest topic(String v) { this.topic = v == null ? "" : v; return this; }
    public OffsetFetchRequest partition(int v) { this.partition = v; return this; }

    public String groupId() { return groupId; }
    public String topic() { return topic; }
    public int partition() { return partition; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putString(groupId);
        w.putString(topic);
        w.putVarInt(partition);
        return w.toByteArray();
    }

    public static OffsetFetchRequest decode(ByteBuffer buf) {
        OffsetFetchRequest r = new OffsetFetchRequest();
        r.groupId = ByteReader.readString(buf);
        r.topic = ByteReader.readString(buf);
        r.partition = ByteReader.readVarInt(buf);
        return r;
    }
}
