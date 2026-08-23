package io.herald.server;

import io.herald.protocol.CommitOffsetRequest;
import io.herald.protocol.CommitOffsetResponse;
import io.herald.protocol.ErrorCode;
import io.herald.protocol.FetchRequest;
import io.herald.protocol.FetchResponse;
import io.herald.protocol.Frame;
import io.herald.protocol.HeartbeatRequest;
import io.herald.protocol.HeartbeatResponse;
import io.herald.protocol.Message;
import io.herald.protocol.MetadataRequest;
import io.herald.protocol.MetadataResponse;
import io.herald.protocol.Opcode;
import io.herald.protocol.ProduceRequest;
import io.herald.protocol.ProduceResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerIntegrationTest {

    @TempDir
    Path tmp;

    private Broker broker;
    private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;

    @BeforeEach
    void setUp() throws Exception {
        BrokerConfig cfg = new BrokerConfig()
                .port(0)
                .dataDir(tmp.resolve("data"))
                .defaultPartitions(2);
        broker = new Broker(cfg);
        broker.start();
        socket = new Socket("127.0.0.1", broker.localPort());
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (socket != null) {
            socket.close();
        }
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void produceFetchRoundTrip() throws Exception {
        ProduceRequest req = new ProduceRequest()
                .topic("orders").partition(0).acks(1)
                .addMessage(new Message().messageId(1).url("http://vendor/a").body("hello".getBytes()));
        Frame resp = roundTrip(Opcode.PRODUCE, req.encode());
        assertEquals(Opcode.PRODUCE_ACK, resp.opcode());
        ProduceResponse pr = ProduceResponse.decode(ByteBuffer.wrap(resp.body()));
        assertEquals(ErrorCode.OK, pr.errorCode());
        assertEquals(1, pr.offsets().size());
        assertEquals(0L, pr.baseOffset());

        FetchResponse f = FetchResponse.decode(ByteBuffer.wrap(roundTrip(Opcode.FETCH,
                new FetchRequest().topic("orders").partition(0).fetchOffset(0).maxBytes(1024 * 1024).maxCount(10).encode()).body()));
        assertEquals(ErrorCode.OK, f.errorCode());
        assertEquals(1, f.messages().size());
        Message m = f.messages().get(0);
        assertEquals("http://vendor/a", m.url());
        assertEquals(0L, m.offset());
        assertEquals(0, m.partition());
        assertEquals("orders", m.topic());
        assertEquals(1L, f.nextOffset());
    }

    @Test
    void batchOrderingPreserved() throws Exception {
        ProduceRequest req = new ProduceRequest().topic("orders").acks(1);
        for (int i = 0; i < 5; i++) {
            req.addMessage(new Message().messageId(100 + i).body(("m" + i).getBytes()));
        }
        ProduceResponse pr = ProduceResponse.decode(ByteBuffer.wrap(roundTrip(Opcode.PRODUCE, req.encode()).body()));
        assertEquals(5, pr.offsets().size());
        for (int i = 1; i < 5; i++) {
            assertEquals(pr.offsets().get(i - 1) + 1, pr.offsets().get(i));
        }

        FetchResponse f = FetchResponse.decode(ByteBuffer.wrap(roundTrip(Opcode.FETCH,
                new FetchRequest().topic("orders").partition(0).fetchOffset(0).maxBytes(1024 * 1024).maxCount(10).encode()).body()));
        assertEquals(5, f.messages().size());
        for (int i = 0; i < 5; i++) {
            assertEquals(i, f.messages().get(i).offset());
            assertEquals("m" + i, new String(f.messages().get(i).body()));
        }
    }

    @Test
    void metadataHeartbeatCommit() throws Exception {
        roundTrip(Opcode.PRODUCE, new ProduceRequest().topic("t").acks(1).addMessage(new Message()).encode());

        MetadataResponse md = MetadataResponse.decode(ByteBuffer.wrap(
                roundTrip(Opcode.METADATA, new MetadataRequest().encode()).body()));
        assertEquals(2, md.partitionCount("t"));
        assertEquals(0, md.leaderOf("t", 0));

        HeartbeatResponse hb = HeartbeatResponse.decode(ByteBuffer.wrap(
                roundTrip(Opcode.HEARTBEAT, new HeartbeatRequest().clientId("c1").encode()).body()));
        assertEquals(ErrorCode.OK, hb.errorCode());

        CommitOffsetResponse co = CommitOffsetResponse.decode(ByteBuffer.wrap(
                roundTrip(Opcode.COMMIT_OFFSET,
                        new CommitOffsetRequest().groupId("g").topic("t").partition(0).committedOffset(5).encode()).body()));
        assertEquals(ErrorCode.OK, co.errorCode());
    }

    @Test
    void unknownTopicFetchReturnsError() throws Exception {
        FetchResponse f = FetchResponse.decode(ByteBuffer.wrap(roundTrip(Opcode.FETCH,
                new FetchRequest().topic("nope").partition(0).fetchOffset(0).encode()).body()));
        assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, f.errorCode());
    }

    private Frame roundTrip(byte opcode, byte[] body) throws Exception {
        Map<String, String> header = new LinkedHashMap<>();
        header.put(Frame.REQUEST_ID, "req");
        byte[] frameBytes = new Frame(opcode, header, body).encode();
        out.write(frameBytes);
        out.flush();

        byte[] head = new byte[Frame.HEADER_SIZE];
        in.readFully(head);
        ByteBuffer hb = ByteBuffer.wrap(head);
        hb.getShort();
        hb.get();
        hb.get();
        int frameLen = hb.getInt();
        byte[] full = new byte[Frame.HEADER_SIZE + frameLen];
        System.arraycopy(head, 0, full, 0, Frame.HEADER_SIZE);
        in.readFully(full, Frame.HEADER_SIZE, frameLen);
        return Frame.decode(full);
    }
}
