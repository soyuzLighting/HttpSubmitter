package io.herald.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 微型写吞吐 benchmark（手动运行，非 JUnit 测试）。
 *
 * <p>用法：{@code java -cp herald-storage/target/test-classes:herald-storage/target/classes
 * io.herald.storage.StorageBenchmark [records] [payloadBytes] [async|sync]}</p>
 */
public final class StorageBenchmark {

    public static void main(String[] args) throws Exception {
        int records = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        int payloadBytes = args.length > 1 ? Integer.parseInt(args[1]) : 1024;
        FlushMode mode = args.length > 2 && "sync".equalsIgnoreCase(args[2])
                ? FlushMode.SYNC : FlushMode.ASYNC;

        Path dir = Files.createTempDirectory("herald-bench");
        LogConfig cfg = new LogConfig()
                .segmentBytes(512L * 1024 * 1024)
                .flushMode(mode)
                .flushIntervalMs(100);
        byte[] payload = new byte[payloadBytes];
        ThreadLocalRandom.current().nextBytes(payload);

        long start = System.nanoTime();
        long lastOffset = -1;
        try (PartitionLog log = PartitionLog.open(dir, cfg)) {
            for (int i = 0; i < records; i++) {
                lastOffset = log.append(payload);
            }
            log.flush();
        }
        double secs = (System.nanoTime() - start) / 1e9;
        double msgPerSec = records / secs;
        double mbPerSec = records * (double) LogRecord.sizeOf(payloadBytes) / secs / (1024 * 1024);

        System.out.printf("records=%d payload=%dB mode=%s%n", records, payloadBytes, mode);
        System.out.printf("elapsed=%.3fs  throughput=%.0f msg/s  %.1f MB/s%n", secs, msgPerSec, mbPerSec);
        System.out.printf("lastOffset=%d  dir=%s%n", lastOffset, dir);

        deleteRecursively(dir);
    }

    private static void deleteRecursively(Path dir) {
        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                    // 映射文件在 JVM 退出前可能无法删除，忽略
                }
            });
        } catch (Exception ignored) {
        }
    }
}
