package io.herald.server;

import io.herald.storage.FlushMode;

import java.nio.file.Path;

/**
 * 单机 Broker 启动入口。支持简单命令行参数：
 *
 * <pre>
 *   --port N          监听端口（默认 9092）
 *   --data-dir PATH   数据目录（默认 data）
 *   --node-id N       节点 ID（默认 0）
 *   --partitions N    自动建 topic 的默认分区数（默认 4）
 *   --flush-mode MODE async|sync（默认 async）
 * </pre>
 */
public final class BrokerMain {

    private BrokerMain() {
    }

    public static void main(String[] args) throws Exception {
        BrokerConfig config = new BrokerConfig();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port":
                    config.port(Integer.parseInt(args[++i]));
                    break;
                case "--data-dir":
                    config.dataDir(Path.of(args[++i]));
                    break;
                case "--node-id":
                    config.nodeId(Integer.parseInt(args[++i]));
                    break;
                case "--partitions":
                    config.defaultPartitions(Integer.parseInt(args[++i]));
                    break;
                case "--flush-mode":
                    config.logConfig().flushMode("sync".equalsIgnoreCase(args[++i])
                            ? FlushMode.SYNC : FlushMode.ASYNC);
                    break;
                default:
                    throw new IllegalArgumentException("unknown argument: " + args[i]);
            }
        }

        Broker broker = new Broker(config);
        broker.start();
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "herald-shutdown"));
        broker.await();
    }
}
