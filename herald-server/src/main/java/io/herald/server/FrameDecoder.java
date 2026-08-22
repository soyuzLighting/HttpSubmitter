package io.herald.server;

import io.herald.protocol.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 将字节流切分为 {@link Frame}：读取 8 字节定长头（magic/version/opcode/frameLen），
 * 再按 frameLen 读取完整帧，交由 {@link Frame#decode(byte[])} 解析。
 */
public final class FrameDecoder extends ByteToMessageDecoder {

    private final int maxFrameSize;

    public FrameDecoder(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < Frame.HEADER_SIZE) {
            return;
        }
        in.markReaderIndex();
        short magic = in.readShort();
        byte version = in.readByte();
        byte opcode = in.readByte();
        int frameLen = in.readInt();
        if (magic != Frame.MAGIC || version != Frame.VERSION) {
            ctx.close();
            return;
        }
        if (frameLen < 0 || frameLen > maxFrameSize) {
            ctx.close();
            return;
        }
        if (in.readableBytes() < frameLen) {
            in.resetReaderIndex();
            return;
        }
        byte[] bytes = new byte[Frame.HEADER_SIZE + frameLen];
        ByteBuffer.wrap(bytes)
                .putShort(magic).put(version).put(opcode).putInt(frameLen);
        in.readBytes(bytes, Frame.HEADER_SIZE, frameLen);
        out.add(Frame.decode(bytes));
    }
}
