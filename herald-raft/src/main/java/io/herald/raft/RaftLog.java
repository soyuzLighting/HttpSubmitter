package io.herald.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * Raft 日志：以 1 为起始下标（index 0 为哨兵，term=0）。
 * 元数据规模低频，先内存实现；崩溃持久化在可靠性加固阶段补充。
 */
final class RaftLog {

    /** 一条日志项：任期与命令负载。 */
    record Entry(long term, byte[] command) {
    }

    private final List<Entry> entries = new ArrayList<>();

    RaftLog() {
        entries.add(new Entry(0, new byte[0])); // 哨兵
    }

    long lastIndex() {
        return entries.size() - 1L;
    }

    long lastTerm() {
        return termAt(lastIndex());
    }

    long termAt(long index) {
        return entries.get((int) index).term();
    }

    Entry entryAt(long index) {
        return entries.get((int) index);
    }

    /** 追加一条，返回其下标。 */
    long append(long term, byte[] command) {
        entries.add(new Entry(term, command));
        return lastIndex();
    }

    /**
     * 追加 AppendEntries 携带的日志：校验 {@code prevLogIndex/prevLogTerm} 是否匹配，
     * 匹配则截断冲突并追加，返回 true；否则 false。
     */
    boolean appendEntries(long prevLogIndex, long prevLogTerm, List<Entry> incoming) {
        if (prevLogIndex > lastIndex() || termAt(prevLogIndex) != prevLogTerm) {
            return false;
        }
        if (incoming.isEmpty()) {
            return true;
        }
        // 截断与已有日志冲突的尾部
        int firstNew = (int) prevLogIndex + 1;
        for (int i = 0; i < incoming.size(); i++) {
            long idx = firstNew + i;
            if (idx <= lastIndex() && termAt(idx) != incoming.get(i).term()) {
                while (entries.size() > idx) {
                    entries.remove(entries.size() - 1);
                }
                break;
            }
        }
        for (int i = 0; i < incoming.size(); i++) {
            long idx = firstNew + i;
            if (idx > lastIndex()) {
                entries.add(incoming.get(i));
            }
        }
        return true;
    }

    /** 从 {@code fromIndex} 起截断（leader 回退 follower 时丢弃未提交日志）。 */
    void truncateFrom(long fromIndex) {
        while (entries.size() > fromIndex) {
            entries.remove(entries.size() - 1);
        }
    }

    /** 复制 [{@code fromIndex}, {@code lastIndex}] 的日志项。 */
    List<Entry> slice(long fromIndex) {
        List<Entry> out = new ArrayList<>();
        for (long i = fromIndex; i <= lastIndex(); i++) {
            out.add(entryAt(i));
        }
        return out;
    }
}
