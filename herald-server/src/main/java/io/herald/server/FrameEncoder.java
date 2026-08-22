package io.herald.server;

import io.herald.protocol.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/** 将 {@link Frame} 编码为字节流写入通道。 */
public final class FrameEncoder extends MessageToByteEncoder<Frame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) {
        out.writeBytes(frame.encode());
    }
}
