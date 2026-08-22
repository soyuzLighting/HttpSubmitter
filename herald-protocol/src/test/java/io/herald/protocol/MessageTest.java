package io.herald.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageTest {

    @Test
    void roundTripPreservesAllSerializedFields() {
        Message m = new Message()
                .messageId(123456789L)
                .topic("order")
                .key("user-42")
                .createTime(1700000000000L)
                .retryCount(2)
                .url("https://vendor.example/api/notify")
                .method("POST")
                .addHeader("Content-Type", "application/json")
                .addHeader("X-Trace", "abc123")
                .body("{\"a\":1}".getBytes())
                .flags(1);

        Message decoded = Message.decode(ByteBuffer.wrap(m.encode()));

        assertEquals(m.messageId(), decoded.messageId());
        assertEquals(m.topic(), decoded.topic());
        assertEquals(m.key(), decoded.key());
        assertEquals(m.createTime(), decoded.createTime());
        assertEquals(m.retryCount(), decoded.retryCount());
        assertEquals(m.url(), decoded.url());
        assertEquals(m.method(), decoded.method());
        assertEquals(m.headers(), decoded.headers());
        assertArrayEquals(m.body(), decoded.body());
        assertEquals(m.flags(), decoded.flags());
    }

    @Test
    void offsetAndPartitionAreSerialized() {
        Message m = new Message().offset(999L).partition(3).body(new byte[]{1});
        Message decoded = Message.decode(ByteBuffer.wrap(m.encode()));
        assertEquals(999L, decoded.offset());
        assertEquals(3, decoded.partition());
    }
}
