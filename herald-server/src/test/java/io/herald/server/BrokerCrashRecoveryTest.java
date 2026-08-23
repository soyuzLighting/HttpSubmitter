package io.herald.server;

import io.herald.protocol.CommitOffsetRequest;
import io.herald.protocol.ErrorCode;
import io.herald.protocol.FetchRequest;
import io.herald.protocol.FetchResponse;
import io.herald.protocol.Frame;
import io.herald.protocol.Message;
import io.herald.protocol.MetadataRequest;
import io.herald.protocol.MetadataResponse;
import io.herald.protocol.OffsetFetchRequest;
import io.herald.protocol.OffsetFetchResponse;
import io.herald.protocol.Opcode;
import io.herald.protocol.ProduceRequest;
import io.herald.protocol.ProduceResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 崩溃恢复验证：单节点 Broker 写入消息 + 提交位点后重启，验证数据（分区日志）
 * 与元数据（topic/位点，经 Raft WAL 持久化）在重启后均能恢复。
 */
class BrokerCrashRecoveryTest {

    private static final String TOPIC = "recovery";
    private static final String GROUP = "g1";

    @TempDir
    Path tmp;

    @Test
    void dataAndOffsetsSurviveRestart() throws Exception {
        seed();

        // 同一 dataDir 重启新 Broker
        try (Broker broker = newBroker()) {
            broker.start();
            int port = broker.localPort();

            // 数据恢复
            FetchResponse fr = fetch(port, TOPIC, 0, 0);
            assertEquals(ErrorCode.OK, fr.errorCode(), "message data should survive restart");
            assertEquals(1, fr.messages().size());
            assertEquals("hello", new String(fr.messages().get(0).body()));

            // 位点恢复（经 Raft 状态机重放）
            assertEquals(1L, offsetFetch(port, GROUP, TOPIC, 0), "committed offset should survive restart");

            // 元数据恢复
            assertEquals(1, metadata(port).partitionCount(TOPIC));

            // 重启后仍可继续写入
            ProduceResponse pr = produce(port, TOPIC, 0, "world", 1);
            assertEquals(ErrorCode.OK, pr.errorCode(), "broker should accept writes after restart");
            assertEquals(1L, pr.baseOffset());
        }
    }

    private void seed() throws Exception {
        try (Broker broker = newBroker()) {
            broker.start();
            int port = broker.localPort();
            waitUntil(() -> metadata(port).brokers().size() >= 1, 10_000);

            ProduceResponse pr = produce(port, TOPIC, 0, "hello", 1);
            assertEquals(ErrorCode.OK, pr.errorCode());
            assertEquals(0L, pr.baseOffset());

            commit(port, GROUP, TOPIC, 0, 1);
            assertEquals(1L, offsetFetch(port, GROUP, TOPIC, 0));
        }
    }

    private Broker newBroker() {
        BrokerConfig cfg = new BrokerConfig()
                .nodeId(0)
                .host("127.0.0.1")
                .advertisedHost("127.0.0.1")
                .port(0)
                .dataDir(tmp)
                .defaultPartitions(1)
                .replicationFactor(1);
        return new Broker(cfg);
    }

    // ---- 协议辅助 ----

    private ProduceResponse produce(int port, String topic, int partition, String body, int acks) throws Exception {
        ProduceRequest req = new ProduceRequest()
                .topic(topic).partition(partition).acks(acks)
                .addMessage(new Message().body(body.getBytes()));
        Frame resp = roundTrip(port, Opcode.PRODUCE, req.encode());
        return ProduceResponse.decode(ByteBuffer.wrap(resp.body()));
    }

    private FetchResponse fetch(int port, String topic, int partition, long offset) throws Exception {
        FetchRequest req = new FetchRequest()
                .topic(topic).partition(partition).fetchOffset(offset).maxBytes(1024 * 1024).maxCount(100);
        Frame resp = roundTrip(port, Opcode.FETCH, req.encode());
        return FetchResponse.decode(ByteBuffer.wrap(resp.body()));
    }

    private void commit(int port, String group, String topic, int partition, long offset) throws Exception {
        CommitOffsetRequest req = new CommitOffsetRequest()
                .groupId(group).topic(topic).partition(partition).committedOffset(offset);
        roundTrip(port, Opcode.COMMIT_OFFSET, req.encode());
    }

    private long offsetFetch(int port, String group, String topic, int partition) throws Exception {
        OffsetFetchRequest req = new OffsetFetchRequest().groupId(group).topic(topic).partition(partition);
        Frame resp = roundTrip(port, Opcode.OFFSET_FETCH, req.encode());
        OffsetFetchResponse r = OffsetFetchResponse.decode(ByteBuffer.wrap(resp.body()));
        return r.errorCode() == ErrorCode.OK ? r.committedOffset() : -1;
    }

    private MetadataResponse metadata(int port) {
        try {
            Frame resp = roundTrip(port, Opcode.METADATA, new MetadataRequest().encode());
            return MetadataResponse.decode(ByteBuffer.wrap(resp.body()));
        } catch (Exception e) {
            return new MetadataResponse();
        }
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

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            sleep(20);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeoutMs + "ms");
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
