package io.herald.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 阻塞 socket 传输：监听本地端口处理入站 RPC，出站每次建立短连接发送并读回响应。
 * 用于多进程部署；控制面消息低频，短连接足够。
 */
public final class SocketRaftTransport implements RaftTransport {

    private static final Logger log = LoggerFactory.getLogger(SocketRaftTransport.class);

    private final String bindHost;
    private final int bindPort;
    private final Map<Integer, String> peers; // nodeId -> host:port
    private final int rpcTimeoutMs;
    private final ExecutorService acceptor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "herald-raft-acceptor");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile ServerSocket serverSocket;
    private volatile MessageHandler handler;

    public SocketRaftTransport(String bindHost, int bindPort, Map<Integer, String> peers, int rpcTimeoutMs) {
        this.bindHost = bindHost;
        this.bindPort = bindPort;
        this.peers = peers;
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    @Override
    public void start(MessageHandler handler) {
        this.handler = handler;
        running.set(true);
        try {
            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(bindHost, bindPort));
        } catch (IOException e) {
            throw new IllegalStateException("raft transport bind failed on " + bindHost + ":" + bindPort, e);
        }
        acceptor.submit(this::acceptLoop);
    }

    public int localPort() {
        ServerSocket ss = serverSocket;
        return ss == null ? bindPort : ss.getLocalPort();
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                acceptor.submit(() -> handleConnection(socket));
            } catch (IOException e) {
                if (running.get()) {
                    log.warn("raft accept error", e);
                }
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (socket) {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            int len = in.readInt();
            byte[] body = new byte[len];
            in.readFully(body);
            RaftMessage resp = handler.handle(RaftMessage.decode(body));
            byte[] outBytes = resp.encode();
            out.writeInt(outBytes.length);
            out.write(outBytes);
            out.flush();
        } catch (Exception e) {
            log.debug("raft connection error", e);
        }
    }

    @Override
    public RaftMessage send(int nodeId, RaftMessage message) {
        String address = peers.get(nodeId);
        if (address == null) {
            throw new IllegalStateException("unknown raft peer: " + nodeId);
        }
        int idx = address.lastIndexOf(':');
        String host = address.substring(0, idx);
        int port = Integer.parseInt(address.substring(idx + 1));
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), rpcTimeoutMs);
            socket.setSoTimeout(rpcTimeoutMs);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            byte[] reqBytes = message.encode();
            out.writeInt(reqBytes.length);
            out.write(reqBytes);
            out.flush();
            int len = in.readInt();
            byte[] resp = new byte[len];
            in.readFully(resp);
            return RaftMessage.decode(resp);
        } catch (IOException e) {
            throw new IllegalStateException("raft send to " + nodeId + " failed", e);
        }
    }

    @Override
    public void close() {
        running.set(false);
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
        }
        acceptor.shutdownNow();
    }
}
