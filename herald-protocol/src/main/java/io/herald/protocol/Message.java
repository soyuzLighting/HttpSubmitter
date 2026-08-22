package io.herald.protocol;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一条通知消息：携带完整 HTTP 请求信息（url/method/headers/body），消费端据此通用投递。
 *
 * <p>序列化字段：messageId, offset, partition, topic, key, createTime, retryCount, url, method, headers, body, flags。
 * 生产时 {@code offset}/{@code partition} 尚未确定（均为 -1），由服务端在写入与拉取时填充。</p>
 */
public final class Message {

    private long messageId;
    private long offset = -1L;
    private int partition = -1;
    private String topic = "";
    private String key = "";
    private long createTime;
    private int retryCount;
    private String url = "";
    private String method = "POST";
    private Map<String, String> headers = new LinkedHashMap<>();
    private byte[] body = new byte[0];
    private int flags;

    // --- fluent setters ---
    public Message messageId(long v) { this.messageId = v; return this; }
    public Message offset(long v) { this.offset = v; return this; }
    public Message partition(int v) { this.partition = v; return this; }
    public Message topic(String v) { this.topic = v == null ? "" : v; return this; }
    public Message key(String v) { this.key = v == null ? "" : v; return this; }
    public Message createTime(long v) { this.createTime = v; return this; }
    public Message retryCount(int v) { this.retryCount = v; return this; }
    public Message url(String v) { this.url = v == null ? "" : v; return this; }
    public Message method(String v) { this.method = v == null ? "POST" : v; return this; }
    public Message headers(Map<String, String> v) {
        this.headers = v == null ? new LinkedHashMap<>() : new LinkedHashMap<>(v);
        return this;
    }
    public Message addHeader(String k, String v) { this.headers.put(k, v); return this; }
    public Message body(byte[] v) { this.body = v == null ? new byte[0] : v; return this; }
    public Message flags(int v) { this.flags = v; return this; }

    // --- getters ---
    public long messageId() { return messageId; }
    public long offset() { return offset; }
    public int partition() { return partition; }
    public String topic() { return topic; }
    public String key() { return key; }
    public long createTime() { return createTime; }
    public int retryCount() { return retryCount; }
    public String url() { return url; }
    public String method() { return method; }
    public Map<String, String> headers() { return headers; }
    public byte[] body() { return body; }
    public int flags() { return flags; }

    public byte[] encode() {
        ByteWriter w = new ByteWriter();
        w.putLong(messageId);
        w.putLong(offset);
        w.putVarInt(partition);
        w.putString(topic);
        w.putString(key);
        w.putLong(createTime);
        w.putVarInt(retryCount);
        w.putString(url);
        w.putString(method);
        w.putVarInt(headers.size());
        for (Map.Entry<String, String> e : headers.entrySet()) {
            w.putString(e.getKey());
            w.putString(e.getValue());
        }
        w.putBytes(body);
        w.putVarInt(flags);
        return w.toByteArray();
    }

    public static Message decode(ByteBuffer buf) {
        Message m = new Message();
        m.messageId = ByteReader.readLong(buf);
        m.offset = ByteReader.readLong(buf);
        m.partition = ByteReader.readVarInt(buf);
        m.topic = ByteReader.readString(buf);
        m.key = ByteReader.readString(buf);
        m.createTime = ByteReader.readLong(buf);
        m.retryCount = ByteReader.readVarInt(buf);
        m.url = ByteReader.readString(buf);
        m.method = ByteReader.readString(buf);
        int headerCount = ByteReader.readVarInt(buf);
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 0; i < headerCount; i++) {
            headers.put(ByteReader.readString(buf), ByteReader.readString(buf));
        }
        m.headers = headers;
        m.body = ByteReader.readBytes(buf);
        m.flags = ByteReader.readVarInt(buf);
        return m;
    }

    @Override
    public String toString() {
        return "Message{messageId=" + messageId + ", offset=" + offset + ", partition=" + partition
                + ", topic='" + topic + "', url='" + url + "', method='" + method
                + "', bodyBytes=" + body.length + '}';
    }
}
