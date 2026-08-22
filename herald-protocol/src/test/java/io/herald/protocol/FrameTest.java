package io.herald.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FrameTest {

    @Test
    void roundTrip() {
        Map<String, String> header = new LinkedHashMap<>();
        header.put(Frame.REQUEST_ID, "req-1");
        Frame f = new Frame(Opcode.PRODUCE, header, "body-bytes".getBytes(StandardCharsets.UTF_8));

        Frame decoded = Frame.decode(f.encode());

        assertEquals(Opcode.PRODUCE, decoded.opcode());
        assertEquals("req-1", decoded.header(Frame.REQUEST_ID));
        assertArrayEquals("body-bytes".getBytes(StandardCharsets.UTF_8), decoded.body());
    }

    @Test
    void emptyHeaderAndBody() {
        Frame f = new Frame(Opcode.HEARTBEAT, null, null);
        Frame decoded = Frame.decode(f.encode());
        assertEquals(Opcode.HEARTBEAT, decoded.opcode());
        assertEquals(0, decoded.header().size());
        assertEquals(0, decoded.body().length);
    }

    @Test
    void rejectBadMagic() {
        byte[] bytes = new Frame(Opcode.PRODUCE, null, null).encode();
        bytes[0] = 0x00;
        assertThrows(IllegalArgumentException.class, () -> Frame.decode(bytes));
    }

    @Test
    void rejectVersionMismatch() {
        byte[] bytes = new Frame(Opcode.PRODUCE, null, null).encode();
        bytes[2] = 99;
        assertThrows(IllegalArgumentException.class, () -> Frame.decode(bytes));
    }
}
