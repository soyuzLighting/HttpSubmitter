package io.herald.protocol;

import java.nio.ByteBuffer;

/** 提交消费位点。 */
public final class CommitOffsetRequest {

    private String groupId = "";
    private String topic = "";
    private int partition;
    private long committedOffset;

    public CommitOffsetRequest groupId(String v) { this.groupId = v == null ? "" : v; return this; }
    public CommitOffsetRequest topic(String v) { this.topic = v == null ? "" : v; return this; }
    public CommitOffsetRequest partition(int v) { this.partition = v; return this; }
    public CommitOffsetRequest committedOffset(long v) { this.committedOffset = v; return this; }

    public String groupId() { return groupId; }
    public String topic() { return topic; }
    public int partition() { return partition; }
    public long committedOffset() { return committedOffset; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putString(groupId);
        w.putString(topic);
        w.putVarInt(partition);
        w.putLong(committedOffset);
        return w.toByteArray();
    }

    public static CommitOffsetRequest decode(ByteBuffer buf) {
        CommitOffsetRequest r = new CommitOffsetRequest();
        r.groupId = ByteReader.readString(buf);
        r.topic = ByteReader.readString(buf);
        r.partition = ByteReader.readVarInt(buf);
        r.committedOffset = ByteReader.readLong(buf);
        return r;
    }
}
