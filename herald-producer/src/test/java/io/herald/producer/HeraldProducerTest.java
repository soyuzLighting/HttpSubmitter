package io.herald.producer;

import io.herald.protocol.Message;
import io.herald.server.Broker;
import io.herald.server.BrokerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeraldProducerTest {

    @TempDir
    Path tmp;

    private Broker broker;
    private HeraldProducer producer;

    @BeforeEach
    void setUp() throws Exception {
        BrokerConfig cfg = new BrokerConfig()
                .port(0)
                .dataDir(tmp.resolve("data"))
                .defaultPartitions(2);
        broker = new Broker(cfg);
        broker.start();
        producer = HeraldProducer.builder()
                .bootstrapServers("127.0.0.1:" + broker.localPort())
                .lingerMs(1)
                .retries(2)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (producer != null) {
            producer.close();
        }
        if (broker != null) {
            broker.close();
        }
    }

    @Test
    void sendReturnsValidResult() throws Exception {
        SendResult r = producer.send("orders", new Message().url("http://vendor/a").body("hello".getBytes()));
        assertTrue(r.ok());
        assertEquals(0L, r.offset());
        assertEquals("orders", r.topic());
        assertTrue(r.partition() >= 0 && r.partition() < 2);
        assertTrue(r.messageId() > 0);
    }

    @Test
    void multipleSendsAcknowledgedWithContiguousPerPartitionOffsets() throws Exception {
        Map<Integer, List<Long>> byPartition = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Message m = new Message()
                    .key(i % 2 == 0 ? "even" : "odd")
                    .url("http://vendor/a")
                    .body(("m" + i).getBytes());
            SendResult r = producer.send("orders", m);
            assertTrue(r.ok());
            byPartition.computeIfAbsent(r.partition(), k -> new ArrayList<>()).add(r.offset());
        }
        int total = 0;
        for (List<Long> offsets : byPartition.values()) {
            Collections.sort(offsets);
            for (int j = 0; j < offsets.size(); j++) {
                assertEquals(j, offsets.get(j), "per-partition offsets must be contiguous from 0");
            }
            total += offsets.size();
        }
        assertEquals(10, total);
    }

    @Test
    void sendAsyncCompletes() {
        List<CompletableFuture<SendResult>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(producer.sendAsync("orders", new Message().url("http://vendor/a").body(("a" + i).getBytes())));
        }
        for (CompletableFuture<SendResult> f : futures) {
            SendResult r = f.join();
            assertTrue(r.ok());
        }
    }
}
