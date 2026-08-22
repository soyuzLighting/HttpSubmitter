package io.herald.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** 生产请求：向指定 topic/partition 批量写入消息。 */
public final class ProduceRequest {

    private String topic = "";
    private int partition = -1; // -1 表示自动分区
    private int acks = 1;       // 0/1/-1
    private final List<Message> messages = new ArrayList<>();

    public ProduceRequest topic(String v) { this.topic = v == null ? "" : v; return this; }
    public ProduceRequest partition(int v) { this.partition = v; return this; }
    public ProduceRequest acks(int v) { this.acks = v; return this; }
    public ProduceRequest addMessage(Message m) { this.messages.add(m); return this; }
    public ProduceRequest messages(List<Message> v) { this.messages.clear(); this.messages.addAll(v); return this; }

    public String topic() { return topic; }
    public int partition() { return partition; }
    public int acks() { return acks; }
    public List<Message> messages() { return messages; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putString(topic);
        w.putVarInt(partition);
        w.putVarInt(acks);
        w.putVarInt(messages.size());
        for (Message m : messages) {
            w.putRaw(m.encode());
        }
        return w.toByteArray();
    }

    public static ProduceRequest decode(ByteBuffer buf) {
        ProduceRequest r = new ProduceRequest();
        r.topic = ByteReader.readString(buf);
        r.partition = ByteReader.readVarInt(buf);
        r.acks = ByteReader.readVarInt(buf);
        int count = ByteReader.readVarInt(buf);
        for (int i = 0; i < count; i++) {
            r.messages.add(Message.decode(buf));
        }
        return r;
    }
}
