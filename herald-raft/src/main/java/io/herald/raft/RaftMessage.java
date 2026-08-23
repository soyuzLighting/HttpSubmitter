package io.herald.raft;

import io.herald.protocol.ByteReader;
import io.herald.protocol.ByteWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Raft 内部消息基类。编码：1 字节类型 + 负载。统一用大端 long / varint 编码。
 */
public abstract class RaftMessage {

    public static final byte REQUEST_VOTE = 1;
    public static final byte REQUEST_VOTE_RESPONSE = 2;
    public static final byte APPEND_ENTRIES = 3;
    public static final byte APPEND_ENTRIES_RESPONSE = 4;
    public static final byte PROPOSE = 5;
    public static final byte PROPOSE_RESPONSE = 6;

    public abstract byte type();

    public abstract byte[] encode();

    public static RaftMessage decode(byte[] bytes) {
        return decode(ByteBuffer.wrap(bytes));
    }

    public static RaftMessage decode(ByteBuffer buf) {
        byte type = buf.get();
        return switch (type) {
            case REQUEST_VOTE -> RequestVote.decodeBody(buf);
            case REQUEST_VOTE_RESPONSE -> RequestVoteResponse.decodeBody(buf);
            case APPEND_ENTRIES -> AppendEntries.decodeBody(buf);
            case APPEND_ENTRIES_RESPONSE -> AppendEntriesResponse.decodeBody(buf);
            case PROPOSE -> Propose.decodeBody(buf);
            case PROPOSE_RESPONSE -> ProposeResponse.decodeBody(buf);
            default -> throw new IllegalArgumentException("unknown raft message type: " + type);
        };
    }

    /** 请求投票：候选者任期、候选者 ID、其日志最后下标与任期（用于日志新旧比较）。 */
    public static final class RequestVote extends RaftMessage {
        private long term;
        private int candidateId;
        private long lastLogIndex;
        private long lastLogTerm;

        public RequestVote term(long v) { this.term = v; return this; }
        public RequestVote candidateId(int v) { this.candidateId = v; return this; }
        public RequestVote lastLogIndex(long v) { this.lastLogIndex = v; return this; }
        public RequestVote lastLogTerm(long v) { this.lastLogTerm = v; return this; }

        public long term() { return term; }
        public int candidateId() { return candidateId; }
        public long lastLogIndex() { return lastLogIndex; }
        public long lastLogTerm() { return lastLogTerm; }

        @Override
        public byte type() { return REQUEST_VOTE; }

        @Override
        public byte[] encode() {
            ByteWriter w = new ByteWriter();
            w.putByte(type());
            w.putLong(term);
            w.putVarInt(candidateId);
            w.putLong(lastLogIndex);
            w.putLong(lastLogTerm);
            return w.toByteArray();
        }

        static RequestVote decodeBody(ByteBuffer b) {
            RequestVote m = new RequestVote();
            m.term = b.getLong();
            m.candidateId = ByteReader.readVarInt(b);
            m.lastLogIndex = b.getLong();
            m.lastLogTerm = b.getLong();
            return m;
        }
    }

    /** 投票响应。 */
    public static final class RequestVoteResponse extends RaftMessage {
        private long term;
        private boolean voteGranted;

        public RequestVoteResponse term(long v) { this.term = v; return this; }
        public RequestVoteResponse voteGranted(boolean v) { this.voteGranted = v; return this; }

        public long term() { return term; }
        public boolean voteGranted() { return voteGranted; }

        @Override
        public byte type() { return REQUEST_VOTE_RESPONSE; }

        @Override
        public byte[] encode() {
            ByteWriter w = new ByteWriter();
            w.putByte(type());
            w.putLong(term);
            w.putByte((byte) (voteGranted ? 1 : 0));
            return w.toByteArray();
        }

        static RequestVoteResponse decodeBody(ByteBuffer b) {
            RequestVoteResponse m = new RequestVoteResponse();
            m.term = b.getLong();
            m.voteGranted = b.get() == 1;
            return m;
        }
    }

    /** 追加日志 / 心跳（entries 为空即心跳）。 */
    public static final class AppendEntries extends RaftMessage {
        private long term;
        private int leaderId;
        private long prevLogIndex;
        private long prevLogTerm;
        private final List<RaftLog.Entry> entries = new ArrayList<>();
        private long leaderCommit;

        public AppendEntries term(long v) { this.term = v; return this; }
        public AppendEntries leaderId(int v) { this.leaderId = v; return this; }
        public AppendEntries prevLogIndex(long v) { this.prevLogIndex = v; return this; }
        public AppendEntries prevLogTerm(long v) { this.prevLogTerm = v; return this; }
        public AppendEntries addEntry(long t, byte[] c) { this.entries.add(new RaftLog.Entry(t, c)); return this; }
        public AppendEntries entries(List<RaftLog.Entry> v) { this.entries.clear(); this.entries.addAll(v); return this; }
        public AppendEntries leaderCommit(long v) { this.leaderCommit = v; return this; }

        public long term() { return term; }
        public int leaderId() { return leaderId; }
        public long prevLogIndex() { return prevLogIndex; }
        public long prevLogTerm() { return prevLogTerm; }
        public List<RaftLog.Entry> entries() { return entries; }
        public long leaderCommit() { return leaderCommit; }

        @Override
        public byte type() { return APPEND_ENTRIES; }

        @Override
        public byte[] encode() {
            ByteWriter w = new ByteWriter();
            w.putByte(type());
            w.putLong(term);
            w.putVarInt(leaderId);
            w.putLong(prevLogIndex);
            w.putLong(prevLogTerm);
            w.putVarInt(entries.size());
            for (RaftLog.Entry e : entries) {
                w.putLong(e.term());
                w.putBytes(e.command());
            }
            w.putLong(leaderCommit);
            return w.toByteArray();
        }

        static AppendEntries decodeBody(ByteBuffer b) {
            AppendEntries m = new AppendEntries();
            m.term = b.getLong();
            m.leaderId = ByteReader.readVarInt(b);
            m.prevLogIndex = b.getLong();
            m.prevLogTerm = b.getLong();
            int count = ByteReader.readVarInt(b);
            for (int i = 0; i < count; i++) {
                m.entries.add(new RaftLog.Entry(b.getLong(), ByteReader.readBytes(b)));
            }
            m.leaderCommit = b.getLong();
            return m;
        }
    }

    /** 追加日志响应。 */
    public static final class AppendEntriesResponse extends RaftMessage {
        private long term;
        private boolean success;
        private long matchIndex;

        public AppendEntriesResponse term(long v) { this.term = v; return this; }
        public AppendEntriesResponse success(boolean v) { this.success = v; return this; }
        public AppendEntriesResponse matchIndex(long v) { this.matchIndex = v; return this; }

        public long term() { return term; }
        public boolean success() { return success; }
        public long matchIndex() { return matchIndex; }

        @Override
        public byte type() { return APPEND_ENTRIES_RESPONSE; }

        @Override
        public byte[] encode() {
            ByteWriter w = new ByteWriter();
            w.putByte(type());
            w.putLong(term);
            w.putByte((byte) (success ? 1 : 0));
            w.putLong(matchIndex);
            return w.toByteArray();
        }

        static AppendEntriesResponse decodeBody(ByteBuffer b) {
            AppendEntriesResponse m = new AppendEntriesResponse();
            m.term = b.getLong();
            m.success = b.get() == 1;
            m.matchIndex = b.getLong();
            return m;
        }
    }

    /** 客户端提交命令（follower 转发给 leader）。 */
    public static final class Propose extends RaftMessage {
        private long term;
        private byte[] command = new byte[0];

        public Propose term(long v) { this.term = v; return this; }
        public Propose command(byte[] v) { this.command = v == null ? new byte[0] : v; return this; }

        public long term() { return term; }
        public byte[] command() { return command; }

        @Override
        public byte type() { return PROPOSE; }

        @Override
        public byte[] encode() {
            ByteWriter w = new ByteWriter();
            w.putByte(type());
            w.putLong(term);
            w.putBytes(command);
            return w.toByteArray();
        }

        static Propose decodeBody(ByteBuffer b) {
            Propose m = new Propose();
            m.term = b.getLong();
            m.command = ByteReader.readBytes(b);
            return m;
        }
    }

    /** 提交响应。 */
    public static final class ProposeResponse extends RaftMessage {
        private long term;
        private boolean success;
        private int leaderId = -1;

        public ProposeResponse term(long v) { this.term = v; return this; }
        public ProposeResponse success(boolean v) { this.success = v; return this; }
        public ProposeResponse leaderId(int v) { this.leaderId = v; return this; }

        public long term() { return term; }
        public boolean success() { return success; }
        public int leaderId() { return leaderId; }

        @Override
        public byte type() { return PROPOSE_RESPONSE; }

        @Override
        public byte[] encode() {
            ByteWriter w = new ByteWriter();
            w.putByte(type());
            w.putLong(term);
            w.putByte((byte) (success ? 1 : 0));
            w.putVarInt(leaderId);
            return w.toByteArray();
        }

        static ProposeResponse decodeBody(ByteBuffer b) {
            ProposeResponse m = new ProposeResponse();
            m.term = b.getLong();
            m.success = b.get() == 1;
            m.leaderId = ByteReader.readVarInt(b);
            return m;
        }
    }
}
