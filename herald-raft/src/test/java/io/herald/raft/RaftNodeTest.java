
package io.herald.raft;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RaftNodeTest {

    @Test
    void singleNodeBecomesLeaderImmediately() {
        ConcurrentHashMap<Integer, InMemoryRaftTransport> bus = new ConcurrentHashMap<>();
        RaftNode node = newNode(0, new RecordingStateMachine(), List.of(), bus);
        node.start();
        try {
            assertTrue(node.isLeader());
            node.submit("hello".getBytes());
        } finally {
            node.close();
        }
    }

    @Test
    void threeNodesElectExactlyOneLeader() {
        ConcurrentHashMap<Integer, InMemoryRaftTransport> bus = new ConcurrentHashMap<>();
        RaftNode n0 = newNode(0, new RecordingStateMachine(), Arrays.asList(1, 2), bus);
        RaftNode n1 = newNode(1, new RecordingStateMachine(), Arrays.asList(0, 2), bus);
        RaftNode n2 = newNode(2, new RecordingStateMachine(), Arrays.asList(0, 1), bus);
        try {
            n0.start();
            n1.start();
            n2.start();
            waitUntil(() -> leaderCount(n0, n1, n2) == 1, 5000);
            assertEquals(1, leaderCount(n0, n1, n2));
        } finally {
            n0.close();
            n1.close();
            n2.close();
        }
    }

    @Test
    void logReplicatesToFollowers() {
        ConcurrentHashMap<Integer, InMemoryRaftTransport> bus = new ConcurrentHashMap<>();
        RecordingStateMachine sm0 = new RecordingStateMachine();
        RecordingStateMachine sm1 = new RecordingStateMachine();
        RecordingStateMachine sm2 = new RecordingStateMachine();
        RaftNode n0 = newNode(0, sm0, Arrays.asList(1, 2), bus);
        RaftNode n1 = newNode(1, sm1, Arrays.asList(0, 2), bus);
        RaftNode n2 = newNode(2, sm2, Arrays.asList(0, 1), bus);
        try {
            n0.start();
            n1.start();
            n2.start();
            waitUntil(() -> leaderId(n0, n1, n2) >= 0, 5000);
            RaftNode leader = leader(n0, n1, n2);

            leader.submit("cmd-1".getBytes());
            leader.submit("cmd-2".getBytes());

            waitUntil(() -> sm0.size() == 2 && sm1.size() == 2 && sm2.size() == 2, 5000);
            assertEquals("cmd-1", new String(sm0.get(0)));
            assertEquals("cmd-2", new String(sm0.get(1)));
            assertEquals("cmd-1", new String(sm1.get(0)));
            assertEquals("cmd-1", new String(sm2.get(0)));
        } finally {
            n0.close();
            n1.close();
            n2.close();
        }
    }

    @Test
    void followerForwardsSubmitToLeader() {
        ConcurrentHashMap<Integer, InMemoryRaftTransport> bus = new ConcurrentHashMap<>();
        RecordingStateMachine sm0 = new RecordingStateMachine();
        RecordingStateMachine sm1 = new RecordingStateMachine();
        RecordingStateMachine sm2 = new RecordingStateMachine();
        RaftNode n0 = newNode(0, sm0, Arrays.asList(1, 2), bus);
        RaftNode n1 = newNode(1, sm1, Arrays.asList(0, 2), bus);
        RaftNode n2 = newNode(2, sm2, Arrays.asList(0, 1), bus);
        try {
            n0.start();
            n1.start();
            n2.start();
            waitUntil(() -> leaderId(n0, n1, n2) >= 0, 5000);
            RaftNode leader = leader(n0, n1, n2);
            RaftNode follower = (n0 != leader) ? n0 : n1;

            follower.submit("via-follower".getBytes());

            waitUntil(() -> sm0.size() == 1 && sm1.size() == 1 && sm2.size() == 1, 5000);
            assertEquals("via-follower", new String(sm0.get(0)));
        } finally {
            n0.close();
            n1.close();
            n2.close();
        }
    }

    @Test
    void leaderFailoverElectsNewLeader() {
        ConcurrentHashMap<Integer, InMemoryRaftTransport> bus = new ConcurrentHashMap<>();
        RecordingStateMachine sm0 = new RecordingStateMachine();
        RecordingStateMachine sm1 = new RecordingStateMachine();
        RecordingStateMachine sm2 = new RecordingStateMachine();
        RaftNode n0 = newNode(0, sm0, Arrays.asList(1, 2), bus);
        RaftNode n1 = newNode(1, sm1, Arrays.asList(0, 2), bus);
        RaftNode n2 = newNode(2, sm2, Arrays.asList(0, 1), bus);
        try {
            n0.start();
            n1.start();
            n2.start();
            waitUntil(() -> leaderId(n0, n1, n2) >= 0, 5000);
            RaftNode oldLeader = leader(n0, n1, n2);
            oldLeader.close();

            List<RaftNode> alive = new ArrayList<>();
            for (RaftNode n : Arrays.asList(n0, n1, n2)) {
                if (n != oldLeader) {
                    alive.add(n);
                }
            }
            waitUntil(() -> leaderId(alive.get(0), alive.get(1)) >= 0, 5000);
            RaftNode newLeader = leader(alive.get(0), alive.get(1));
            assertFalse(newLeader == oldLeader);

            newLeader.submit("after-failover".getBytes());
            RecordingStateMachine[] sms = {sm0, sm1, sm2};
            waitUntil(() -> Arrays.stream(sms).anyMatch(s -> s.contains("after-failover")), 5000);
            assertTrue(Arrays.stream(sms).anyMatch(s -> s.contains("after-failover")));
        } finally {
            n0.close();
            n1.close();
            n2.close();
        }
    }

    // ---- helpers ----

    private static RaftNode newNode(int nodeId, RecordingStateMachine sm, List<Integer> peerIds,
                                    ConcurrentHashMap<Integer, InMemoryRaftTransport> bus) {
        RaftConfig cfg = new RaftConfig()
                .nodeId(nodeId)
                .electionTimeoutMinMs(150)
                .electionTimeoutMaxMs(300)
                .heartbeatIntervalMs(40)
                .rpcTimeoutMs(2000);
        for (int peer : peerIds) {
            cfg.peer(peer, "127.0.0.1:" + (19000 + peer));
        }
        InMemoryRaftTransport transport = new InMemoryRaftTransport(nodeId, bus);
        return new RaftNode(cfg, transport, sm);
    }

    private static int leaderCount(RaftNode... nodes) {
        int c = 0;
        for (RaftNode n : nodes) {
            if (n.isLeader()) {
                c++;
            }
        }
        return c;
    }

    private static int leaderId(RaftNode... nodes) {
        for (RaftNode n : nodes) {
            if (n.isLeader()) {
                return n.nodeId();
            }
        }
        return -1;
    }

    private static RaftNode leader(RaftNode... nodes) {
        for (RaftNode n : nodes) {
            if (n.isLeader()) {
                return n;
            }
        }
        throw new IllegalStateException("no leader");
    }

    private static void waitUntil(BooleanSupplier condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            sleep(10);
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

    private static final class RecordingStateMachine implements RaftStateMachine {
        private final CopyOnWriteArrayList<byte[]> applied = new CopyOnWriteArrayList<>();

        @Override
        public void apply(long index, byte[] command) {
            applied.add(command);
        }

        int size() {
            return applied.size();
        }

        byte[] get(int i) {
            return applied.get(i);
        }

        boolean contains(String s) {
            return applied.stream().anyMatch(b -> new String(b).equals(s));
        }
    }
}
