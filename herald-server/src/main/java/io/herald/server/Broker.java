package io.herald.server;

import io.herald.common.SnowflakeIdGenerator;
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
import io.herald.protocol.OffsetFetchRequest;
import io.herald.protocol.OffsetFetchResponse;
import io.herald.protocol.ProduceRequest;
import io.herald.protocol.ProduceResponse;
import io.herald.raft.InMemoryRaftTransport;
import io.herald.raft.RaftConfig;
import io.herald.raft.RaftEventListener;
import io.herald.raft.RaftNode;
import io.herald.raft.RaftTransport;
import io.herald.raft.SocketRaftTransport;
import io.herald.storage.LogEntry;
import io.herald.storage.PartitionLog;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker：Netty 数据面 + 分区日志 + 内嵌 Raft 控制面 + 副本复制。
 *
 * <p>单节点（peers 为空）时自任 leader；集群模式下由 Raft 选出的控制器负责 topic 元数据与分区 leader 分配。</p>
 */
public final class Broker implements AutoCloseable, RaftEventListener {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final BrokerConfig config;
    private final int nodeId;
    private final SnowflakeIdGenerator idGen;
    private final TopicManager topicManager;
    private final ClusterMetadata clusterMetadata;
    private final RaftNode raftNode;
    private final ReplicationManager replicationManager;
    private final Set<Integer> deadBrokers = ConcurrentHashMap.newKeySet();
    private final AtomicLong partitionCounter = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final java.util.concurrent.ExecutorService controllerExecutor;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public Broker(BrokerConfig config) {
        this.config = config;
        this.nodeId = config.nodeId();
        this.idGen = new SnowflakeIdGenerator(config.nodeId());
        this.topicManager = new TopicManager(config.dataDir(), config.logConfig());
        this.clusterMetadata = new ClusterMetadata();
        this.raftNode = new RaftNode(raftConfig(config), raftTransport(config), clusterMetadata, this);
        this.replicationManager = new ReplicationManager(nodeId, clusterMetadata, topicManager,
                config.replicaFetchIntervalMs(), deadBrokers);
        this.controllerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "herald-controller-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    /** 启动 Raft、数据面与副本复制，并向集群注册自身。 */
    public void start() throws InterruptedException {
        raftNode.start();
        reopenTopicLogs();
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new FrameDecoder(config.maxFrameSize()))
                                .addLast(new FrameEncoder())
                                .addLast(new BrokerHandler(Broker.this));
                    }
                });
        serverChannel = b.bind(config.host(), config.port()).sync().channel();
        log.info("Herald broker {} started on {}:{}", nodeId, config.host(), localPort());

        registerSelf();
        replicationManager.start();
    }

    /** 实际绑定的本地端口。 */
    public int localPort() {
        return serverChannel == null
                ? config.port()
                : ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public int nodeId() {
        return nodeId;
    }

    public boolean isController() {
        return raftNode.isLeader();
    }

    /** 阻塞直到服务关闭。 */
    public void await() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.closeFuture().sync();
        }
    }

    /** 分发单个请求帧，返回响应帧（无需响应时返回 null）。 */
    public Frame handle(Frame frame) {
        try {
            switch (frame.opcode()) {
                case Opcode.PRODUCE:
                    return handleProduce(frame);
                case Opcode.FETCH:
                    return handleFetch(frame);
                case Opcode.COMMIT_OFFSET:
                    return handleCommit(frame);
                case Opcode.OFFSET_FETCH:
                    return handleOffsetFetch(frame);
                case Opcode.METADATA:
                    return handleMetadata(frame);
                case Opcode.HEARTBEAT:
                    return handleHeartbeat(frame);
                case Opcode.REPLICA_FETCH:
                    return handleReplicaFetch(frame);
                default:
                    log.warn("unknown opcode: {}", frame.opcode());
                    return null;
            }
        } catch (RuntimeException e) {
            log.warn("error handling opcode {}", frame.opcode(), e);
            return null;
        }
    }

    private Frame handleProduce(Frame frame) {
        ProduceRequest req = ProduceRequest.decode(ByteBuffer.wrap(frame.body()));
        ProduceResponse resp = new ProduceResponse();
        String topic = req.topic();
        if (topic.isEmpty() || req.messages().isEmpty()) {
            resp.errorCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            return req.acks() == 0 ? null : respond(frame, Opcode.PRODUCE_ACK, resp.encode());
        }
        try {
            ensureTopic(topic);
        } catch (RuntimeException e) {
            log.warn("failed to create topic {}", topic, e);
            resp.errorCode(ErrorCode.INTERNAL);
            return req.acks() == 0 ? null : respond(frame, Opcode.PRODUCE_ACK, resp.encode());
        }
        int partition = resolvePartition(topic, req.partition(), req.messages().get(0).key());
        int leader = clusterMetadata.leaderOf(topic, partition);
        if (leader != nodeId) {
            resp.errorCode(ErrorCode.NOT_LEADER_OR_FOLLOWER);
            return req.acks() == 0 ? null : respond(frame, Opcode.PRODUCE_ACK, resp.encode());
        }
        PartitionLog partitionLog = topicManager.getLog(topic, partition);
        if (partitionLog == null) {
            resp.errorCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            return req.acks() == 0 ? null : respond(frame, Opcode.PRODUCE_ACK, resp.encode());
        }
        long base = -1;
        long last = -1;
        try {
            for (Message m : req.messages()) {
                if (m.messageId() <= 0) {
                    m.messageId(idGen.nextId());
                }
                if (m.createTime() <= 0) {
                    m.createTime(System.currentTimeMillis());
                }
                m.topic(topic).partition(partition);
                long off = partitionLog.append(m.encode());
                if (base < 0) {
                    base = off;
                }
                last = off;
                resp.addOffset(off);
            }
            resp.baseOffset(base);
        } catch (IllegalArgumentException e) {
            resp.errorCode(ErrorCode.MESSAGE_TOO_LARGE);
        } catch (IOException e) {
            log.warn("append failed topic={} partition={}", topic, partition, e);
            resp.errorCode(ErrorCode.INTERNAL);
        }
        if (resp.errorCode() != ErrorCode.OK || req.acks() == 0) {
            return req.acks() == 0 ? null : respond(frame, Opcode.PRODUCE_ACK, resp.encode());
        }
        if (req.acks() == -1 && last >= 0) {
            boolean replicated = replicationManager.awaitReplication(topic, partition, last, 5000);
            if (!replicated) {
                resp.errorCode(ErrorCode.INTERNAL);
            }
        }
        return respond(frame, Opcode.PRODUCE_ACK, resp.encode());
    }

    private Frame handleFetch(Frame frame) {
        FetchRequest req = FetchRequest.decode(ByteBuffer.wrap(frame.body()));
        FetchResponse resp = new FetchResponse();
        int leader = clusterMetadata.leaderOf(req.topic(), req.partition());
        if (leader < 0) {
            resp.errorCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            return respond(frame, Opcode.FETCH_RESPONSE, resp.encode());
        }
        if (leader != nodeId) {
            resp.errorCode(ErrorCode.NOT_LEADER_OR_FOLLOWER);
            return respond(frame, Opcode.FETCH_RESPONSE, resp.encode());
        }
        PartitionLog partitionLog = topicManager.getLog(req.topic(), req.partition());
        if (partitionLog == null) {
            resp.errorCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            return respond(frame, Opcode.FETCH_RESPONSE, resp.encode());
        }
        long fetchOffset = req.fetchOffset();
        List<LogEntry> entries = partitionLog.readBatch(fetchOffset, req.maxCount(), req.maxBytes());
        List<Message> messages = new ArrayList<>(entries.size());
        long next = fetchOffset;
        for (LogEntry e : entries) {
            Message m = Message.decode(ByteBuffer.wrap(e.payload()));
            m.offset(e.offset()).partition(req.partition()).topic(req.topic());
            messages.add(m);
            next = e.offset() + 1;
        }
        resp.messages(messages).nextOffset(next);
        return respond(frame, Opcode.FETCH_RESPONSE, resp.encode());
    }

    private Frame handleReplicaFetch(Frame frame) {
        FetchRequest req = FetchRequest.decode(ByteBuffer.wrap(frame.body()));
        FetchResponse resp = new FetchResponse();
        int follower = parseNodeId(frame.header(ReplicationManager.HEADER_NODE_ID));
        PartitionLog partitionLog = topicManager.getLog(req.topic(), req.partition());
        if (partitionLog == null) {
            resp.errorCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            return respond(frame, Opcode.REPLICA_RESPONSE, resp.encode());
        }
        long fetchOffset = req.fetchOffset();
        List<LogEntry> entries = partitionLog.readBatch(fetchOffset, req.maxCount(), req.maxBytes());
        List<Message> messages = new ArrayList<>(entries.size());
        long next = fetchOffset;
        for (LogEntry e : entries) {
            Message m = Message.decode(ByteBuffer.wrap(e.payload()));
            m.offset(e.offset()).partition(req.partition()).topic(req.topic());
            messages.add(m);
            next = e.offset() + 1;
        }
        resp.messages(messages).nextOffset(next);
        if (follower >= 0) {
            replicationManager.recordFollower(req.topic(), req.partition(), follower, next);
        }
        return respond(frame, Opcode.REPLICA_RESPONSE, resp.encode());
    }

    private Frame handleCommit(Frame frame) {
        CommitOffsetRequest req = CommitOffsetRequest.decode(ByteBuffer.wrap(frame.body()));
        CommitOffsetResponse resp = new CommitOffsetResponse();
        try {
            raftNode.submit(ClusterMetadata.commitOffset(
                    req.groupId(), req.topic(), req.partition(), req.committedOffset()));
        } catch (RuntimeException e) {
            log.warn("commit offset failed group={} topic={} partition={}",
                    req.groupId(), req.topic(), req.partition(), e);
            resp.errorCode(ErrorCode.INTERNAL);
        }
        return respond(frame, Opcode.COMMIT_ACK, resp.encode());
    }

    private Frame handleOffsetFetch(Frame frame) {
        OffsetFetchRequest req = OffsetFetchRequest.decode(ByteBuffer.wrap(frame.body()));
        OffsetFetchResponse resp = new OffsetFetchResponse();
        long committed = clusterMetadata.committedOffset(req.groupId(), req.topic(), req.partition());
        resp.committedOffset(committed);
        return respond(frame, Opcode.OFFSET_FETCH_RESPONSE, resp.encode());
    }

    private Frame handleMetadata(Frame frame) {
        MetadataRequest req = MetadataRequest.decode(ByteBuffer.wrap(frame.body()));
        MetadataResponse resp = new MetadataResponse();
        clusterMetadata.brokers().forEach((id, peer) -> resp.addBroker(id, peer.host(), peer.dataPort()));
        if (req.topic().isEmpty()) {
            clusterMetadata.topicLeaders().forEach(resp::addTopic);
        } else {
            List<Integer> leaders = clusterMetadata.topicLeaders().get(req.topic());
            if (leaders != null && !leaders.isEmpty()) {
                resp.addTopic(req.topic(), leaders);
            }
        }
        return respond(frame, Opcode.METADATA_RESPONSE, resp.encode());
    }

    private Frame handleHeartbeat(Frame frame) {
        HeartbeatRequest req = HeartbeatRequest.decode(ByteBuffer.wrap(frame.body()));
        HeartbeatResponse resp = new HeartbeatResponse();
        return respond(frame, Opcode.HEARTBEAT_RESPONSE, resp.encode());
    }

    /** 确保 topic 已建并经 Raft 复制到本地元数据，同时打开本地分区日志。 */
    private void ensureTopic(String topic) {
        if (clusterMetadata.partitionCount(topic) == 0) {
            raftNode.submit(ClusterMetadata.createTopic(topic, config.defaultPartitions(), config.replicationFactor()));
        }
        int count = clusterMetadata.partitionCount(topic);
        if (count > 0 && topicManager.partitionCount(topic) == 0) {
            try {
                topicManager.createTopic(topic, count);
            } catch (IOException e) {
                throw new IllegalStateException("failed to open topic " + topic, e);
            }
        }
    }

    /** 崩溃恢复：Raft 重放元数据后，重新打开本节点已存在的分区日志，供读写。 */
    private void reopenTopicLogs() {
        clusterMetadata.topicLeaders().forEach((topic, leaders) -> {
            if (topicManager.partitionCount(topic) == 0) {
                try {
                    topicManager.createTopic(topic, leaders.size());
                } catch (IOException e) {
                    log.warn("failed to reopen topic {} on recovery", topic, e);
                }
            }
        });
    }

    /** 后台注册自身到集群：Raft 需多数派才可提交，节点启动先后不一，故重试直至成功。 */
    private void registerSelf() {
        int port = localPort();
        controllerExecutor.submit(() -> {
            while (!closed.get()) {
                try {
                    raftNode.submit(ClusterMetadata.registerBroker(nodeId, config.advertisedHost(), port, config.raftPort()));
                    log.info("broker {} registered to cluster at {}:{}", nodeId, config.advertisedHost(), port);
                    return;
                } catch (RuntimeException e) {
                    log.warn("broker self-registration failed, retrying", e);
                }
                sleepQuietly(500);
            }
        });
    }

    private int resolvePartition(String topic, int requested, String key) {
        int count = clusterMetadata.partitionCount(topic);
        if (requested >= 0 && requested < count) {
            return requested;
        }
        if (count <= 0) {
            return 0;
        }
        if (key == null || key.isEmpty()) {
            return (int) Math.floorMod(partitionCounter.getAndIncrement(), count);
        }
        return Math.floorMod(key.hashCode(), count);
    }

    // ---- RaftEventListener ----

    @Override
    public void onLeaderElected(int nodeId) {
        log.info("raft leader elected: {}", nodeId);
    }

    @Override
    public void onPeerStatusChanged(int nodeId, boolean alive) {
        if (alive) {
            deadBrokers.remove(nodeId);
        } else {
            deadBrokers.add(nodeId);
            controllerExecutor.submit(() -> reconcileLeader(nodeId));
        }
    }

    /** 控制器将挂在失效节点上的分区 leader 迁到存活副本。 */
    private void reconcileLeader(int failedNode) {
        if (!raftNode.isLeader()) {
            return;
        }
        for (Map.Entry<String, List<Integer>> e : clusterMetadata.topicLeaders().entrySet()) {
            List<Integer> leaders = e.getValue();
            for (int p = 0; p < leaders.size(); p++) {
                if (leaders.get(p) == failedNode) {
                    int newLeader = pickNewLeader(e.getKey(), p, failedNode);
                    if (newLeader >= 0) {
                        try {
                            raftNode.submit(ClusterMetadata.electLeader(e.getKey(), p, newLeader));
                        } catch (RuntimeException ex) {
                            log.warn("re-elect leader failed {}/{}", e.getKey(), p, ex);
                        }
                    }
                }
            }
        }
    }

    private int pickNewLeader(String topic, int partition, int failedNode) {
        int best = -1;
        for (int replica : clusterMetadata.replicasOf(topic, partition)) {
            if (replica == failedNode || deadBrokers.contains(replica)) {
                continue;
            }
            if (best < 0 || replica < best) {
                best = replica;
            }
        }
        return best;
    }

    private static RaftConfig raftConfig(BrokerConfig config) {
        RaftConfig raft = new RaftConfig()
                .nodeId(config.nodeId())
                .electionTimeoutMinMs(150)
                .electionTimeoutMaxMs(300)
                .heartbeatIntervalMs(50)
                .rpcTimeoutMs(2000)
                .logDir(config.dataDir().resolve("raft"));
        config.peers().forEach((id, peer) -> raft.peer(id, peer.host() + ":" + peer.raftPort()));
        return raft;
    }

    private static RaftTransport raftTransport(BrokerConfig config) {
        if (config.raftTransport() != null) {
            return config.raftTransport();
        }
        if (config.peers().isEmpty()) {
            return new InMemoryRaftTransport(config.nodeId(), new ConcurrentHashMap<>());
        }
        Map<Integer, String> peerAddrs = new LinkedHashMap<>();
        config.peers().forEach((id, peer) -> peerAddrs.put(id, peer.host() + ":" + peer.raftPort()));
        return new SocketRaftTransport(config.advertisedHost(), config.raftPort(), peerAddrs, 2000);
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int parseNodeId(String value) {
        if (value == null) {
            return -1;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static Frame respond(Frame request, byte opcode, byte[] payload) {
        Map<String, String> header = new LinkedHashMap<>();
        String requestId = request.header(Frame.REQUEST_ID);
        if (requestId != null) {
            header.put(Frame.REQUEST_ID, requestId);
        }
        return new Frame(opcode, header, payload);
    }

    @Override
    public void close() {
        closed.set(true);
        replicationManager.close();
        controllerExecutor.shutdownNow();
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        topicManager.close();
        raftNode.close();
    }
}
