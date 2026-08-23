package io.herald.consumer;

import io.herald.protocol.Frame;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * 到 Broker 的阻塞式长连接。消费端为「拉取-投递-提交」顺序模型，单线程逐请求交互，
 * 用阻塞 socket 即可，无需 Netty；投递并发由 HTTP 客户端承担。
 */
final class ConsumerConnection implements AutoCloseable {

    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private final Object writeLock = new Object();

    private ConsumerConnection(Socket socket, DataOutputStream out, DataInputStream in) {
        this.socket = socket;
        this.out = out;
        this.in = in;
    }

    static ConsumerConnection connect(String host, int port, int connectTimeoutMs) throws IOException {
        Socket socket = new Socket();
        socket.connect(new java.net.InetSocketAddress(host, port), connectTimeoutMs);
        socket.setTcpNoDelay(true);
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        return new ConsumerConnection(socket, out, in);
    }

    Frame send(Frame request) throws IOException {
        synchronized (writeLock) {
            byte[] bytes = request.encode();
            out.write(bytes);
            out.flush();
            return readFrame();
        }
    }

    boolean isClosed() {
        return socket.isClosed();
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private Frame readFrame() throws IOException {
        byte[] head = new byte[Frame.HEADER_SIZE];
        in.readFully(head);
        ByteBuffer hb = ByteBuffer.wrap(head);
        hb.getShort(); // magic
        hb.get();      // version
        hb.get();      // opcode
        int frameLen = hb.getInt();
        byte[] full = new byte[Frame.HEADER_SIZE + frameLen];
        System.arraycopy(head, 0, full, 0, Frame.HEADER_SIZE);
        in.readFully(full, Frame.HEADER_SIZE, frameLen);
        return Frame.decode(full);
    }
}
