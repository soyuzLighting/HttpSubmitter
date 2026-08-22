package io.herald.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * 段内稀疏索引：{@code relativeOffset -> 文件位置}，按 relativeOffset 升序。
 * 查询时二分定位到不大于目标 offset 的最近条目，再顺序扫描到目标记录。
 */
final class OffsetIndex {

    private final List<Long> offsets = new ArrayList<>();
    private final List<Long> positions = new ArrayList<>();

    void append(long relativeOffset, long position) {
        offsets.add(relativeOffset);
        positions.add(position);
    }

    /** 返回相对 offset 不超过 {@code relativeOffset} 的最大条目对应的文件位置；无则返回 -1。 */
    long lookup(long relativeOffset) {
        if (offsets.isEmpty()) {
            return -1;
        }
        int lo = 0;
        int hi = offsets.size() - 1;
        int ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (offsets.get(mid) <= relativeOffset) {
                ans = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return ans >= 0 ? positions.get(ans) : -1;
    }

    boolean isEmpty() {
        return offsets.isEmpty();
    }

    int size() {
        return offsets.size();
    }

    void clear() {
        offsets.clear();
        positions.clear();
    }
}
