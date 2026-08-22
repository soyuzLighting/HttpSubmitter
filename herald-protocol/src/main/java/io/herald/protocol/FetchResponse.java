package io.herald.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** 拉取响应：返回消息列表与下一次拉取的 offset。 */
public final class FetchResponse {

    private int errorCode = ErrorCode.OK;
    private long nextOffset;
    private final List<Message> messages = new ArrayList<>();

    public FetchResponse errorCode(int v) { this.errorCode = v; return this; }
    public FetchResponse nextOffset(long v) { this.nextOffset = v; return this; }
    public FetchResponse messages(List<Message> v) { this.messages.clear(); this.messages.addAll(v); return this; }

    public int errorCode() { return errorCode; }
    public long nextOffset() { return nextOffset; }
    public List<Message> messages() { return messages; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putVarInt(errorCode);
        w.putLong(nextOffset);
        w.putVarInt(messages.size());
        for (Message m : messages) {
            w.putRaw(m.encode());
        }
        return w.toByteArray();
    }

    public static FetchResponse decode(ByteBuffer buf) {
        FetchResponse r = new FetchResponse();
        r.errorCode = ByteReader.readVarInt(buf);
        r.nextOffset = ByteReader.readLong(buf);
        int count = ByteReader.readVarInt(buf);
        for (int i = 0; i < count; i++) {
            r.messages.add(Message.decode(buf));
        }
        return r;
    }
}
