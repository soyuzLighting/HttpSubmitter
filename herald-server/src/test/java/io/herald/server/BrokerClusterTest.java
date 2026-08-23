package io.herald.server;

import io.herald.protocol.ErrorCode;
import io.herald.protocol.FetchRequest;
import io.herald.protocol.FetchResponse;
import io.herald.protocol.Frame;
import io.herald.protocol.Message;
import io.herald.protocol.MetadataRequest;
import io.herald.protocol.MetadataResponse;
import io.herald.protocol.Opcode;
import io.herald.protocol.ProduceRequest;
import io.herald.protocol.ProduceResponse;
import io.herald.raft.InMemoryRaftTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 三节点 in-memory 集群集成测试：Raft 选举与元数据复制、写复制（acks=-1）、
 * leader 宕机后的自动切换与可用性。数据面走真实 Netty 端口，控制面走共享内存总线。
 */
class BrokerClusterTest {

    private static final String TOPIC = "orders";
    private static final int NODES = 3;

    @TempDir
    Path tmp;

    private final List<Broker> brokers = new ArrayList<>();
    private final ConcurrentHashMap<Integer, InMemoryRaftTransport> bus = new ConcurrentHashMap<>();

    @AfterEach
    void tearDown() {
        for (Broker b : brokers) {
            if (b != null) {
                b.close();
            }
        }
        brokers.clear();
        bus.clear();
    }

    private Broker newBroker(int nodeId) {
        BrokerConfig cfg = new BrokerConfig()
                .nodeId(nodeId)
                .host("127.0.0.1")
                .advertisedHost("127.0.0.1")
                .port(0)
                .dataDir(tmp.resolve("node" + nodeId))
                .defaultPartitions(1)
                .replicationFactor(NODES)
                .replicaFetchIntervalMs(30)
                .raftTransport(new InMemoryRaftTransport(nodeId, bus));
        for (int p = 0; p < NODES; p++) {
            if (p != nodeId) {
                cfg.peer(new Peer(p, "127.0.0.1", 0, 0));
            }
        }
        return new Broker(cfg);
    }

    @Test
    void writeReplicatesAndFailsOver() throws Exception {
        for (int i = 0; i < NODES; i++) {
            Broker b = newBroker(i);
            brokers.add(b);
            b.start();
        }

        // 1. 等待三个 broker 完成注册（元数据被 Raft 复制到节点 0）
        waitUntil(() -> metadata(brokers.get(0)).brokers().size() == NODES, 10_000);

        // 2. acks=-1 写入分区 leader（assign 规则下最低 nodeId 为 leader）
        ProduceResponse pr = produce(brokers.get(0), TOPIC, 0, -1, "first");
        assertEquals(ErrorCode.OK, pr.errorCode(), "leader produce should succeed");
        assertEquals(0L, pr.baseOffset());

        // 3. leader 可读
        FetchResponse fr = fetch(brokers.get(0), TOPIC, 0, 0);
        assertEquals(ErrorCode.OK, fr.errorCode());
        assertEquals(1, fr.messages().size());

        // 4. 宕掉 leader（node 0）
        brokers.get(0).close();
        brokers.set(0, null);

        // 5. 副本接任 leader 后仍能读到已复制的消息
        waitUntil(() -> fetchError(brokers.get(1), TOPIC, 0, 0) == ErrorCode.OK, 10_000);
        FetchResponse after = fetch(brokers.get(1), TOPIC, 0, 0);
        assertEquals(1, after.messages().size());
        assertEquals("first", new String(after.messages().get(0).body()));

        // 6. 新 leader 继续可写
        ProduceResponse pr2 = produce(brokers.get(1), TOPIC, 0, 1, "second");
        assertEquals(ErrorCode.OK, pr2.errorCode());
    }

    // ---- 协议辅助 ----

    private ProduceResponse produce(Broker b, String topic, int partition, int acks, String body) throws Exception {
        ProduceRequest req = new ProduceRequest()
                .topic(topic).partition(partition).acks(acks)
                .addMessage(new Message().messageId(System.nanoTime()).body(body.getBytes()));
        Frame resp = roundTrip(b, Opcode.PRODUCE, req.encode());
        return ProduceResponse.decode(ByteBuffer.wrap(resp.body()));
    }

    private int fetchError(Broker b, String topic, int partition, long offset) {
        try {
            return fetch(b, topic, partition, offset).errorCode();
        } catch (Exception e) {
            return -1;
        }
    }

    private FetchResponse fetch(Broker b, String topic, int partition, long offset) throws Exception {
        FetchRequest req = new FetchRequest()
                .topic(topic).partition(partition).fetchOffset(offset).maxBytes(1024 * 1024).maxCount(100);
        Frame resp = roundTrip(b, Opcode.FETCH, req.encode());
        return FetchResponse.decode(ByteBuffer.wrap(resp.body()));
    }

    private MetadataResponse metadata(Broker b) {
        try {
            Frame resp = roundTrip(b, Opcode.METADATA, new MetadataRequest().encode());
            return MetadataResponse.decode(ByteBuffer.wrap(resp.body()));
        } catch (Exception e) {
            return new MetadataResponse();
        }
    }

    private static Frame roundTrip(Broker b, byte opcode, byte[] body) throws Exception {
        Map<String, String> header = new LinkedHashMap<>();
        header.put(Frame.REQUEST_ID, "req");
        byte[] frameBytes = new Frame(opcode, header, body).encode();
        try (Socket socket = new Socket("127.0.0.1", b.localPort())) {
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
