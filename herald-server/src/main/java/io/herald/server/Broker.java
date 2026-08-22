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
import io.herald.protocol.ProduceRequest;
import io.herald.protocol.ProduceResponse;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单机 Broker：Netty 数据面 + 分区日志存储 + 消费位点。
 *
 * <p>处理 produce/fetch/commit/metadata/heartbeat 五类请求，消息写入本地分区日志并
 * 按 offset 顺序读取。集群能力（Raft 元数据、副本复制）在后续阶段接入。</p>
 */
public final class Broker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final BrokerConfig config;
    private final SnowflakeIdGenerator idGen;
    private final TopicManager topicManager;
    private final ConsumerOffsetManager offsetManager;
    private final AtomicLong partitionCounter = new AtomicLong();

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public Broker(BrokerConfig config) {
        this.config = config;
        this.idGen = new SnowflakeIdGenerator(config.nodeId());
        this.topicManager = new TopicManager(config.dataDir(), config.logConfig());
        this.offsetManager = new ConsumerOffsetManager();
    }

    /** 启动 Netty 服务并绑定端口。 */
    public void start() throws InterruptedException {
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
        log.info("Herald broker started on {}:{}", config.host(), localPort());
    }

    /** 实际绑定的本地端口（端口 0 时用于获取随机端口）。 */
    public int localPort() {
        return serverChannel == null
                ? config.port()
                : ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
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
                case Opcode.METADATA:
                    return handleMetadata(frame);
                case Opcode.HEARTBEAT:
                    return handleHeartbeat(frame);
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
            if (topicManager.partitionCount(topic) == 0) {
                topicManager.createTopic(topic, config.defaultPartitions());
            }
        } catch (IOException e) {
            log.warn("failed to create topic {}", topic, e);
            resp.errorCode(ErrorCode.INTERNAL);
            return respond(frame, Opcode.PRODUCE_ACK, resp.encode());
        }
        int partition = resolvePartition(topic, req.partition(), req.messages().get(0).key());
        PartitionLog partitionLog = topicManager.getLog(topic, partition);
        if (partitionLog == null) {
            resp.errorCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            return req.acks() == 0 ? null : respond(frame, Opcode.PRODUCE_ACK, resp.encode());
        }
        long base = -1;
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
                resp.addOffset(off);
            }
            resp.baseOffset(base);
        } catch (IllegalArgumentException e) {
            resp.errorCode(ErrorCode.MESSAGE_TOO_LARGE);
        } catch (IOException e) {
            log.warn("append failed topic={} partition={}", topic, partition, e);
            resp.errorCode(ErrorCode.INTERNAL);
        }
        if (req.acks() == 0) {
            return null;
        }
        return respond(frame, Opcode.PRODUCE_ACK, resp.encode());
    }

    private Frame handleFetch(Frame frame) {
        FetchRequest req = FetchRequest.decode(ByteBuffer.wrap(frame.body()));
        FetchResponse resp = new FetchResponse();
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

    private Frame handleCommit(Frame frame) {
        CommitOffsetRequest req = CommitOffsetRequest.decode(ByteBuffer.wrap(frame.body()));
        offsetManager.commit(req.groupId(), req.topic(), req.partition(), req.committedOffset());
        CommitOffsetResponse resp = new CommitOffsetResponse();
        return respond(frame, Opcode.COMMIT_ACK, resp.encode());
    }

    private Frame handleMetadata(Frame frame) {
        MetadataRequest req = MetadataRequest.decode(ByteBuffer.wrap(frame.body()));
        MetadataResponse resp = new MetadataResponse();
        if (req.topic().isEmpty()) {
            topicManager.topics().forEach(resp::addTopic);
        } else {
            int n = topicManager.partitionCount(req.topic());
            if (n > 0) {
                resp.addTopic(req.topic(), n);
            }
        }
        return respond(frame, Opcode.METADATA_RESPONSE, resp.encode());
    }

    private Frame handleHeartbeat(Frame frame) {
        HeartbeatRequest req = HeartbeatRequest.decode(ByteBuffer.wrap(frame.body()));
        HeartbeatResponse resp = new HeartbeatResponse();
        return respond(frame, Opcode.HEARTBEAT_RESPONSE, resp.encode());
    }

    private int resolvePartition(String topic, int requested, String key) {
        int count = topicManager.partitionCount(topic);
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
    }
}
