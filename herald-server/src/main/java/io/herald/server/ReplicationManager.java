package io.herald.server;

import io.herald.protocol.FetchRequest;
import io.herald.protocol.FetchResponse;
import io.herald.protocol.Frame;
import io.herald.protocol.Message;
import io.herald.protocol.Opcode;
import io.herald.storage.PartitionLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 数据面副本复制：follower 周期性向 leader 发送 {@code REPLICA_FETCH} 拉取并顺序追加到本地日志；
 * leader 记录各 follower 的复制进度，供 {@code acks=-1} 时等待 ISR 全部复制完成。
 */
public final class ReplicationManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    static final String HEADER_NODE_ID = "nodeId";

    private final int selfNodeId;
    private final ClusterMetadata metadata;
    private final TopicManager topicManager;
    private final long pullIntervalMs;
    private final ScheduledExecutorService puller;

    /** topic -> partition -> followerNodeId -> 该 follower 已复制到的 offset（下次将拉取的 offset）。 */
    private final Map<String, Map<Integer, Map<Integer, Long>>> followerProgress = new ConcurrentHashMap<>();

    public ReplicationManager(int selfNodeId, ClusterMetadata metadata, TopicManager topicManager, long pullIntervalMs) {
        this.selfNodeId = selfNodeId;
        this.metadata = metadata;
        this.topicManager = topicManager;
        this.pullIntervalMs = pullIntervalMs;
        this.puller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "herald-replicator-" + selfNodeId);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        puller.scheduleWithFixedDelay(this::pullLoop, pullIntervalMs, pullIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** leader 记录某 follower 的复制进度（下次要拉的 offset）。 */
    public void recordFollower(String topic, int partition, int followerNodeId, long nextFetchOffset) {
        followerProgress.computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(partition, p -> new ConcurrentHashMap<>())
                .put(followerNodeId, nextFetchOffset);
    }

    /** leader 等待所有 follower 副本复制到 {@code offset}（含）。 */
    public boolean awaitReplication(String topic, int partition, long offset, long timeoutMs) {
        List<Integer> replicas = metadata.replicasOf(topic, partition);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean done = true;
            for (int replica : replicas) {
                if (replica == selfNodeId) {
                    continue;
                }
                Long progress = progressOf(topic, partition, replica);
                if (progress == null || progress <= offset) {
                    done = false;
                    break;
                }
            }
            if (done) {
                return true;
            }
            sleep(5);
        }
        return false;
    }

    private Long progressOf(String topic, int partition, int replica) {
        Map<Integer, Map<Integer, Long>> byPartition = followerProgress.get(topic);
        if (byPartition == null) {
            return null;
        }
        Map<Integer, Long> byFollower = byPartition.get(partition);
        return byFollower == null ? null : byFollower.get(replica);
    }

    private void pullLoop() {
        for (ClusterMetadata.ReplicaTask task : metadata.followerTasks(selfNodeId)) {
            try {
                pull(task);
            } catch (Exception e) {
                log.debug("replica pull failed {}:{}", task.topic(), task.partition(), e);
            }
        }
    }

    private void pull(ClusterMetadata.ReplicaTask task) throws IOException {
        Peer leader = metadata.broker(task.leader());
        if (leader == null) {
            return;
        }
        ensureLocalTopic(task.topic());
        PartitionLog local = topicManager.getLog(task.topic(), task.partition());
        if (local == null) {
            return;
        }
        long fetchOffset = local.nextOffset();
        FetchResponse resp = fetchFrom(leader, task.topic(), task.partition(), fetchOffset);
        for (Message m : resp.messages()) {
            local.append(m.encode());
        }
    }

    private FetchResponse fetchFrom(Peer leader, String topic, int partition, long fetchOffset) throws IOException {
        FetchRequest req = new FetchRequest()
                .topic(topic).partition(partition).fetchOffset(fetchOffset)
                .maxBytes(1024 * 1024).maxCount(500);
        Map<String, String> header = Map.of(HEADER_NODE_ID, String.valueOf(selfNodeId));
        Frame request = new Frame(Opcode.REPLICA_FETCH, header, req.encode());
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(leader.host(), leader.dataPort()), 3000);
            socket.setSoTimeout(5000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            byte[] bytes = request.encode();
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
            Frame response = Frame.decode(full);
            return FetchResponse.decode(ByteBuffer.wrap(response.body()));
        }
    }

    private void ensureLocalTopic(String topic) {
        int count = metadata.partitionCount(topic);
        if (count > 0 && topicManager.partitionCount(topic) == 0) {
            try {
                topicManager.createTopic(topic, count);
            } catch (IOException e) {
                log.warn("failed to open local topic {}", topic, e);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        puller.shutdownNow();
    }
}
