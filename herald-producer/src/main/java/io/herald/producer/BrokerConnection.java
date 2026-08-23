package io.herald.producer;

import io.herald.protocol.Frame;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 到单个 Broker 的长连接。发送请求时注入唯一 {@code requestId}，响应按该 ID 关联到
 * 对应的 {@link CompletableFuture}。
 */
public final class BrokerConnection implements AutoCloseable {

    private final String host;
    private final int port;
    private final EventLoopGroup group;
    private final Channel channel;
    private final ConcurrentHashMap<String, CompletableFuture<Frame>> pending;
    private final AtomicLong seq = new AtomicLong();

    private BrokerConnection(String host, int port, EventLoopGroup group, Channel channel,
                             ConcurrentHashMap<String, CompletableFuture<Frame>> pending) {
        this.host = host;
        this.port = port;
        this.group = group;
        this.channel = channel;
        this.pending = pending;
    }

    public static BrokerConnection connect(String host, int port, int connectTimeoutMs, int maxFrameSize)
            throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup(1);
        ConcurrentHashMap<String, CompletableFuture<Frame>> pending = new ConcurrentHashMap<>();
        String address = host + ":" + port;
        Bootstrap b = new Bootstrap();
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new FrameDecoder(maxFrameSize))
                                .addLast(new FrameEncoder())
                                .addLast(new ClientHandler(pending, address));
                    }
                });
        Channel channel = b.connect(host, port).sync().channel();
        return new BrokerConnection(host, port, group, channel, pending);
    }

    /** 发送请求帧并返回响应 Future（按 requestId 关联）。 */
    public CompletableFuture<Frame> send(Frame request) {
        String requestId = "req-" + seq.incrementAndGet();
        Map<String, String> header = new LinkedHashMap<>(request.header());
        header.put(Frame.REQUEST_ID, requestId);
        Frame framed = new Frame(request.opcode(), header, request.body());
        CompletableFuture<Frame> future = new CompletableFuture<>();
        pending.put(requestId, future);
        channel.writeAndFlush(framed);
        return future;
    }

    public boolean isActive() {
        return channel.isActive();
    }

    public String address() {
        return host + ":" + port;
    }

    @Override
    public void close() {
        channel.close();
        group.shutdownGracefully();
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<Frame> {

        private final ConcurrentHashMap<String, CompletableFuture<Frame>> pending;
        private final String address;

        ClientHandler(ConcurrentHashMap<String, CompletableFuture<Frame>> pending, String address) {
            this.pending = pending;
            this.address = address;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
            String requestId = frame.header(Frame.REQUEST_ID);
            if (requestId != null) {
                CompletableFuture<Frame> f = pending.remove(requestId);
                if (f != null) {
                    f.complete(frame);
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            failAll(new IOException("connection closed: " + address));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            failAll(cause);
            ctx.close();
        }

        private void failAll(Throwable cause) {
            for (CompletableFuture<Frame> f : pending.values()) {
                f.completeExceptionally(cause);
            }
            pending.clear();
        }
    }
}
