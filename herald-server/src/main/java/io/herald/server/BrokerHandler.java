package io.herald.server;

import io.herald.protocol.Frame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 每连接处理器：接收 {@link Frame}，交由 {@link Broker#handle(Frame)} 分发，并回写响应。 */
public final class BrokerHandler extends SimpleChannelInboundHandler<Frame> {

    private static final Logger log = LoggerFactory.getLogger(BrokerHandler.class);

    private final Broker broker;

    public BrokerHandler(Broker broker) {
        this.broker = broker;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) {
        Frame response = broker.handle(frame);
        if (response != null) {
            ctx.writeAndFlush(response);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.debug("connection error", cause);
        ctx.close();
    }
}
