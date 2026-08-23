package io.herald.consumer;

import com.sun.net.httpserver.HttpServer;
import io.herald.producer.HeraldProducer;
import io.herald.protocol.FetchRequest;
import io.herald.protocol.FetchResponse;
import io.herald.protocol.Frame;
import io.herald.protocol.Message;
import io.herald.protocol.Opcode;
import io.herald.server.Broker;
import io.herald.server.BrokerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class HeraldConsumerTest {

    @TempDir
    Path tmp;

    private Broker broker;
    private HttpServer httpServer;
    private final ConcurrentLinkedQueue<String> receivedBodies = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setUp() throws Exception {
        broker = new Broker(new BrokerConfig().port(0).dataDir(tmp.resolve("data")).defaultPartitions(2));
        broker.start();
    }

    @AfterEach
    void tearDown() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void deliversMessagesViaHttp() throws Exception {
        startHttpServer(200);
        String url = "http://127.0.0.1:" + httpServer.getAddress().getPort() + "/callback";

        try (HeraldProducer producer = producer()) {
            for (int i = 0; i < 3; i++) {
                producer.send("notify", new Message().url(url).method("POST").body(("body-" + i).getBytes()));
            }
        }

        HeraldConsumer consumer = HeraldConsumer.builder()
                .bootstrapServers("127.0.0.1:" + broker.localPort())
                .groupId("g1")
                .topics("notify")
                .pollIntervalMs(20)
                .deliveryConfig(new DeliveryConfig().maxRetries(1).retryBackoffMs(10).timeoutMs(2000))
                .build();
        consumer.start();

        waitUntil(() -> receivedBodies.size() >= 3, 5000);
        consumer.close();

        assertEquals(3, receivedBodies.size());
        List<String> bodies = receivedBodies.stream().sorted().toList();
        assertEquals("body-0", bodies.get(0));
        assertEquals("body-1", bodies.get(1));
        assertEquals("body-2", bodies.get(2));
    }

    @Test
    void failedDeliveryGoesToDlq() throws Exception {
        startHttpServer(500);
        String url = "http://127.0.0.1:" + httpServer.getAddress().getPort() + "/fail";

        try (HeraldProducer producer = producer()) {
            producer.send("notify", new Message().url(url).method("POST").body("boom".getBytes()));
        }

        HeraldConsumer consumer = HeraldConsumer.builder()
                .bootstrapServers("127.0.0.1:" + broker.localPort())
                .groupId("g1")
                .topics("notify")
                .pollIntervalMs(20)
                .deliveryConfig(new DeliveryConfig().maxRetries(1).retryBackoffMs(10).timeoutMs(1000))
                .build();
        consumer.start();

        waitUntil(() -> fetchCount("notify.DLQ", 0) >= 1, 5000);
        consumer.close();

        assertEquals(1, fetchCount("notify.DLQ", 0));
        // maxRetries=1 => 初始 + 1 次重试，共 2 次投递尝试
        assertEquals(2, receivedBodies.size());
    }

    private HeraldProducer producer() {
        return HeraldProducer.builder()
                .bootstrapServers("127.0.0.1:" + broker.localPort())
                .lingerMs(1)
                .build();
    }

    private void startHttpServer(int statusCode) throws Exception {
        httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.createContext("/", exchange -> {
            receivedBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(statusCode, -1);
            exchange.close();
        });
        httpServer.start();
    }

    private int fetchCount(String topic, int partition) {
        try (Socket s = new Socket("127.0.0.1", broker.localPort())) {
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());
            FetchRequest req = new FetchRequest().topic(topic).partition(partition)
                    .fetchOffset(0).maxBytes(1024 * 1024).maxCount(100);
            byte[] bytes = new Frame(Opcode.FETCH, Map.of(), req.encode()).encode();
            out.write(bytes);
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
            FetchResponse resp = FetchResponse.decode(ByteBuffer.wrap(Frame.decode(full).body()));
            return resp.messages().size();
        } catch (Exception e) {
            return 0;
        }
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        if (!condition.getAsBoolean()) {
            fail("condition not met within " + timeoutMs + "ms");
        }
    }
}
