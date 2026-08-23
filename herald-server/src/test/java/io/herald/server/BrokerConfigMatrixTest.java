package io.herald.server;

import io.herald.protocol.ErrorCode;
import io.herald.protocol.FetchRequest;
import io.herald.protocol.FetchResponse;
import io.herald.protocol.Frame;
import io.herald.protocol.Message;
import io.herald.protocol.Opcode;
import io.herald.protocol.ProduceRequest;
import io.herald.protocol.ProduceResponse;
import io.herald.storage.FlushMode;
import io.herald.storage.LogConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 双模式刷盘（SYNC/ASYNC）× 复制确认（acks=0/1/-1）配置矩阵：
 * 验证每种组合下消息都能正确写入并读回；acks=0 不回确认但仍落盘。
 */
class BrokerConfigMatrixTest {

    private static final int[] ACKS = {0, 1, -1};

    @TempDir
    Path tmp;

    @Test
    void flushAndAcksMatrixProducesAndReadsBack() throws Exception {
        for (FlushMode flush : FlushMode.values()) {
            for (int acks : ACKS) {
                String tag = flush + "/acks=" + acks;
                BrokerConfig cfg = new BrokerConfig()
                        .nodeId(0)
                        .host("127.0.0.1")
                        .advertisedHost("127.0.0.1")
                        .port(0)
                        .dataDir(tmp.resolve("d-" + flush + "-" + acks))
                        .defaultPartitions(1)
                        .replicationFactor(1)
                        .logConfig(new LogConfig().flushMode(flush));
                try (Broker broker = new Broker(cfg)) {
                    broker.start();
                    int port = broker.localPort();
                    String body = "m-" + tag;
                    produce(port, "orders", 0, acks, body);

                    if (acks == 0) {
                        // 无确认，稍候再读以越过 Netty 处理窗口
                        Thread.sleep(50);
                    }
                    FetchResponse fr = fetch(port, "orders", 0, 0);
                    assertEquals(ErrorCode.OK, fr.errorCode(), tag);
                    assertEquals(1, fr.messages().size(), tag);
                    assertEquals(body, new String(fr.messages().get(0).body()), tag);
                }
            }
        }
    }

    @Test
    void acksZeroReturnsNoAckButStillPersists() throws Exception {
        try (Broker broker = new Broker(new BrokerConfig()
                .nodeId(0).host("127.0.0.1").advertisedHost("127.0.0.1").port(0)
                .dataDir(tmp.resolve("ack0")).defaultPartitions(1))) {
            broker.start();
            int port = broker.localPort();

            ProduceRequest req = new ProduceRequest().topic("orders").partition(0).acks(0)
                    .addMessage(new Message().body("fire-and-forget".getBytes()));
            Frame request = new Frame(Opcode.PRODUCE, Map.of(Frame.REQUEST_ID, "r"), req.encode());

            try (Socket socket = new Socket("127.0.0.1", port)) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.write(request.encode());
                out.flush();
                socket.setSoTimeout(300);
                DataInputStream in = new DataInputStream(socket.getInputStream());
                // acks=0 时 broker 不应回包
                assertThrows(SocketTimeoutException.class, () -> in.readFully(new byte[Frame.HEADER_SIZE]));
            }

            FetchResponse fr = fetch(port, "orders", 0, 0);
            assertEquals(ErrorCode.OK, fr.errorCode());
            assertEquals(1, fr.messages().size());
            assertEquals("fire-and-forget", new String(fr.messages().get(0).body()));
        }
    }

    private void produce(int port, String topic, int partition, int acks, String body) throws Exception {
        ProduceRequest req = new ProduceRequest()
                .topic(topic).partition(partition).acks(acks)
                .addMessage(new Message().body(body.getBytes()));
        if (acks == 0) {
            try (Socket socket = new Socket("127.0.0.1", port)) {
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.write(new Frame(Opcode.PRODUCE, Map.of(Frame.REQUEST_ID, "r"), req.encode()).encode());
                out.flush();
            }
            return;
        }
        Frame resp = roundTrip(port, Opcode.PRODUCE, req.encode());
        ProduceResponse pr = ProduceResponse.decode(ByteBuffer.wrap(resp.body()));
        assertEquals(ErrorCode.OK, pr.errorCode());
    }

    private FetchResponse fetch(int port, String topic, int partition, long offset) throws Exception {
        FetchRequest req = new FetchRequest()
                .topic(topic).partition(partition).fetchOffset(offset).maxBytes(1024 * 1024).maxCount(100);
        Frame resp = roundTrip(port, Opcode.FETCH, req.encode());
        return FetchResponse.decode(ByteBuffer.wrap(resp.body()));
    }

    private static Frame roundTrip(int port, byte opcode, byte[] body) throws Exception {
        Map<String, String> header = new LinkedHashMap<>();
        header.put(Frame.REQUEST_ID, "req");
        byte[] frameBytes = new Frame(opcode, header, body).encode();
        try (Socket socket = new Socket("127.0.0.1", port)) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
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
}
