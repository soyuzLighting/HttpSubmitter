package io.herald.raft;

import io.herald.raft.RaftMessage.AppendEntries;
import io.herald.raft.RaftMessage.AppendEntriesResponse;
import io.herald.raft.RaftMessage.Propose;
import io.herald.raft.RaftMessage.ProposeResponse;
import io.herald.raft.RaftMessage.RequestVote;
import io.herald.raft.RaftMessage.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 内嵌 Raft 节点：Follower / Candidate / Leader 三态、随机选举超时、AppendEntries 日志复制、
 * 状态机按序应用。控制面消息低频，采用同步 RPC（见 {@link RaftTransport}）。
 */
public final class RaftNode implements AutoCloseable, RaftTransport.MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private enum Role { FOLLOWER, CANDIDATE, LEADER }

    private final int nodeId;
    private final RaftConfig config;
    private final RaftTransport transport;
    private final RaftStateMachine stateMachine;
    private final RaftEventListener listener;
    private final RaftLog logStore = new RaftLog();
    private final Map<Integer, PeerState> peers = new ConcurrentHashMap<>();

    private final Object lock = new Object();
    private final Object applyLock = new Object();

    private volatile long currentTerm = 0;
    private volatile int votedFor = -1;
    private volatile int leaderId = -1;
    private volatile Role role = Role.FOLLOWER;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    private final ScheduledExecutorService timer;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile long electionDeadline;
    private volatile long lastHeartbeatSent;

    public RaftNode(RaftConfig config, RaftTransport transport, RaftStateMachine stateMachine) {
        this(config, transport, stateMachine, null);
    }

    public RaftNode(RaftConfig config, RaftTransport transport, RaftStateMachine stateMachine,
                    RaftEventListener listener) {
        this.nodeId = config.nodeId();
        this.config = config;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.listener = listener;
        this.timer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "herald-raft-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        transport.start(this);
        if (config.peers().isEmpty()) {
            becomeLeader();
        } else {
            resetElectionDeadline();
        }
        timer.scheduleWithFixedDelay(this::tick, 10, 10, TimeUnit.MILLISECONDS);
    }

    public int nodeId() {
        return nodeId;
    }

    public boolean isLeader() {
        return role == Role.LEADER;
    }

    public int leaderId() {
        return leaderId;
    }

    public long term() {
        return currentTerm;
    }

    /** 提交一条命令并阻塞直到被提交并应用；无 leader 或超时抛异常。 */
    public void submit(byte[] command) {
        long deadline = System.currentTimeMillis() + config.rpcTimeoutMs() * 3;
        while (System.currentTimeMillis() < deadline) {
            if (isLeader()) {
                if (proposeAsLeader(command) >= 0) {
                    return;
                }
            }
            int lid = leaderId;
            if (lid >= 0 && lid != nodeId) {
                try {
                    RaftMessage resp = transport.send(lid, new Propose().term(currentTerm).command(command));
                    ProposeResponse r = (ProposeResponse) resp;
                    if (r.term() > currentTerm) {
                        stepDown(r.term());
                        continue;
                    }
                    if (r.success()) {
                        waitApplied(r.index());
                        return;
                    }
                    if (r.leaderId() >= 0) {
                        synchronized (lock) {
                            leaderId = r.leaderId();
                        }
                    }
                } catch (Exception ignored) {
                    // 对端不可达，下一轮重试
                }
            }
            sleep(10);
        }
        throw new IllegalStateException("raft submit timed out, no leader reachable");
    }

    // ---- 入站消息处理（由 transport 调用） ----

    @Override
    public RaftMessage handle(RaftMessage message) {
        return switch (message.type()) {
            case RaftMessage.REQUEST_VOTE -> handleRequestVote((RequestVote) message);
            case RaftMessage.APPEND_ENTRIES -> handleAppendEntries((AppendEntries) message);
            case RaftMessage.PROPOSE -> handlePropose((Propose) message);
            default -> throw new IllegalArgumentException("unexpected inbound type " + message.type());
        };
    }

    private RaftMessage handleRequestVote(RequestVote m) {
        stepDownIfStale(m.term());
        boolean grant;
        synchronized (lock) {
            boolean logUpToDate = m.lastLogTerm() > logStore.lastTerm()
                    || (m.lastLogTerm() == logStore.lastTerm() && m.lastLogIndex() >= logStore.lastIndex());
            boolean canVote = votedFor == -1 || votedFor == m.candidateId();
            grant = m.term() >= currentTerm && logUpToDate && canVote;
            if (grant) {
                votedFor = m.candidateId();
                resetElectionDeadline();
            }
        }
        return new RequestVoteResponse().term(currentTerm).voteGranted(grant);
    }

    private RaftMessage handleAppendEntries(AppendEntries m) {
        stepDownIfStale(m.term());
        if (m.term() < currentTerm) {
            return new AppendEntriesResponse().term(currentTerm).success(false).matchIndex(logStore.lastIndex());
        }
        synchronized (lock) {
            leaderId = m.leaderId();
            role = Role.FOLLOWER;
            resetElectionDeadline();
        }
        boolean ok;
        synchronized (lock) {
            ok = logStore.appendEntries(m.prevLogIndex(), m.prevLogTerm(), m.entries());
        }
        if (!ok) {
            return new AppendEntriesResponse().term(currentTerm).success(false).matchIndex(logStore.lastIndex());
        }
        synchronized (lock) {
            if (m.leaderCommit() > commitIndex) {
                commitIndex = Math.min(m.leaderCommit(), logStore.lastIndex());
            }
        }
        applyCommitted();
        return new AppendEntriesResponse().term(currentTerm).success(true).matchIndex(logStore.lastIndex());
    }

    private RaftMessage handlePropose(Propose m) {
        stepDownIfStale(m.term());
        if (isLeader()) {
            long idx = proposeAsLeader(m.command());
            return new ProposeResponse().term(currentTerm).success(idx >= 0).leaderId(nodeId).index(idx);
        }
        int lid = leaderId;
        if (lid < 0) {
            return new ProposeResponse().term(currentTerm).success(false).leaderId(-1);
        }
        try {
            return transport.send(lid, m);
        } catch (Exception e) {
            return new ProposeResponse().term(currentTerm).success(false).leaderId(-1);
        }
    }

    // ---- 定时器 ----

    private void tick() {
        if (role == Role.LEADER) {
            long now = System.currentTimeMillis();
            if (now - lastHeartbeatSent >= config.heartbeatIntervalMs()) {
                lastHeartbeatSent = now;
                sendHeartbeats();
                advanceCommit();
            }
        } else if (System.currentTimeMillis() >= electionDeadline) {
            startElection();
        }
    }

    private void startElection() {
        synchronized (lock) {
            role = Role.CANDIDATE;
            currentTerm++;
            votedFor = nodeId;
            resetElectionDeadline();
        }
        int votes = 1;
        long myLastIndex;
        long myLastTerm;
        synchronized (lock) {
            myLastIndex = logStore.lastIndex();
            myLastTerm = logStore.lastTerm();
        }
        for (int peerId : peers.keySet()) {
            RequestVote req = new RequestVote()
                    .term(currentTerm).candidateId(nodeId)
                    .lastLogIndex(myLastIndex).lastLogTerm(myLastTerm);
            try {
                RequestVoteResponse resp = (RequestVoteResponse) transport.send(peerId, req);
                if (resp.term() > currentTerm) {
                    stepDown(resp.term());
                    return;
                }
                if (resp.voteGranted()) {
                    votes++;
                }
            } catch (Exception ignored) {
                // 节点不可达
            }
        }
        if (votes >= majority() && role == Role.CANDIDATE) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        synchronized (lock) {
            role = Role.LEADER;
            leaderId = nodeId;
        }
        for (int peerId : config.peers().keySet()) {
            peers.put(peerId, new PeerState(logStore.lastIndex() + 1, 0, false));
        }
        if (listener != null) {
            listener.onLeaderElected(nodeId);
        }
        sendHeartbeats();
    }

    // ---- 复制 ----

    private void sendHeartbeats() {
        for (int peerId : peers.keySet()) {
            try {
                sendAppendEntries(peerId);
            } catch (Exception ignored) {
                markPeer(peerId, false);
            }
        }
    }

    private void sendAppendEntries(int peerId) {
        PeerState p = peers.get(peerId);
        if (p == null) {
            return;
        }
        long prevIndex = p.nextIndex - 1;
        long prevTerm;
        synchronized (lock) {
            prevTerm = prevIndex >= 0 ? logStore.termAt(prevIndex) : 0;
        }
        AppendEntries m = new AppendEntries()
                .term(currentTerm).leaderId(nodeId)
                .prevLogIndex(prevIndex).prevLogTerm(prevTerm)
                .leaderCommit(commitIndex);
        synchronized (lock) {
            m.entries(logStore.slice(p.nextIndex));
        }
        AppendEntriesResponse r = (AppendEntriesResponse) transport.send(peerId, m);
        if (r.term() > currentTerm) {
            stepDown(r.term());
            return;
        }
        if (r.success()) {
            p.matchIndex = r.matchIndex();
            p.nextIndex = p.matchIndex + 1;
            markPeer(peerId, true);
        } else {
            p.nextIndex = Math.max(1, p.nextIndex - 1);
        }
    }

    /** leader 追加命令并推进提交，返回已提交的日志下标；未提交返回 -1。 */
    private long proposeAsLeader(byte[] command) {
        long index;
        synchronized (lock) {
            index = logStore.append(currentTerm, command);
        }
        for (int peerId : peers.keySet()) {
            try {
                sendAppendEntries(peerId);
            } catch (Exception ignored) {
                markPeer(peerId, false);
            }
        }
        advanceCommit();
        return waitCommitted(index) ? index : -1;
    }

    private void advanceCommit() {
        synchronized (lock) {
            long target = commitIndex;
            for (long n = commitIndex + 1; n <= logStore.lastIndex(); n++) {
                int count = 1;
                for (PeerState p : peers.values()) {
                    if (p.matchIndex >= n) {
                        count++;
                    }
                }
                if (count >= majority()) {
                    target = n;
                } else {
                    break;
                }
            }
            if (target > commitIndex && logStore.termAt(target) == currentTerm) {
                commitIndex = target;
            }
        }
        applyCommitted();
    }

    private void applyCommitted() {
        synchronized (applyLock) {
            while (lastApplied < commitIndex) {
                long idx = lastApplied + 1;
                byte[] command;
                synchronized (lock) {
                    command = logStore.entryAt(idx).command();
                }
                stateMachine.apply(idx, command);
                lastApplied = idx;
            }
        }
    }

    private boolean waitCommitted(long index) {
        long deadline = System.currentTimeMillis() + config.rpcTimeoutMs();
        while (System.currentTimeMillis() < deadline) {
            if (lastApplied >= index) {
                return true;
            }
            sleep(2);
        }
        return false;
    }

    /** follower 等待本地状态机应用到 leader 已提交的日志下标（靠下一次心跳推进 commitIndex）。 */
    private void waitApplied(long index) {
        long deadline = System.currentTimeMillis() + config.rpcTimeoutMs();
        while (lastApplied < index && System.currentTimeMillis() < deadline) {
            sleep(2);
        }
    }

    // ---- 状态辅助 ----

    private void stepDownIfStale(long term) {
        if (term > currentTerm) {
            stepDown(term);
        }
    }

    private void stepDown(long term) {
        synchronized (lock) {
            if (term > currentTerm) {
                currentTerm = term;
                role = Role.FOLLOWER;
                votedFor = -1;
                leaderId = -1;
            }
            resetElectionDeadline();
        }
    }

    private void markPeer(int peerId, boolean alive) {
        PeerState p = peers.get(peerId);
        if (p == null) {
            return;
        }
        if (p.alive != alive) {
            p.alive = alive;
            if (listener != null) {
                listener.onPeerStatusChanged(peerId, alive);
            }
        }
    }

    private int majority() {
        return (peers.size() + 1) / 2 + 1;
    }

    private void resetElectionDeadline() {
        long range = config.electionTimeoutMaxMs() - config.electionTimeoutMinMs();
        long extra = range > 0 ? ThreadLocalRandom.current().nextLong(range) : 0;
        electionDeadline = System.currentTimeMillis() + config.electionTimeoutMinMs() + extra;
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
        timer.shutdownNow();
        transport.close();
    }

    private static final class PeerState {
        volatile long nextIndex;
        volatile long matchIndex;
        volatile boolean alive;

        PeerState(long nextIndex, long matchIndex, boolean alive) {
            this.nextIndex = nextIndex;
            this.matchIndex = matchIndex;
            this.alive = alive;
        }
    }
}
