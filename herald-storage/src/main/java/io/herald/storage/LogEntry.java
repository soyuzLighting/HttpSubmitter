package io.herald.storage;

/** 一次批量读取的条目：offset 与原始负载字节。 */
public record LogEntry(long offset, byte[] payload) {
}
