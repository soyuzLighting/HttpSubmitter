# Herald

可靠的外部 HTTP 通知投递服务。业务系统把「要发一条 HTTP 通知」交给 Herald，Herald 负责持久化、复制、重试，最终稳定地把请求投递到目标供应商 API。

- **解耦**：业务系统不感知供应商差异、网络抖动、限流重试
- **可靠**：消息持久化 + ISR 副本复制，宕机可恢复，at-least-once 语义
- **高吞吐**：Kafka 式顺序追加写 + 批量 + 零拷贝，单机数十万 msg/s
- **易接入**：生产端 / 消费端以 Spring Boot Starter 引入，调用极简

> 完整设计见 [design.md](design.md)，测试结果见 [docs/test-report.md](docs/test-report.md)。

---

## 1. 架构概览

```
 业务系统 (Producer SDK) ──send──▶  Broker 集群 (服务端)
                                        │ 二进制 TCP 协议 (Netty)
                                        ▼
                                   投递消费端 (Consumer SDK)
                                        │ HTTP(S)
                                        ▼
                                   供应商 API（广告 / CRM / 库存 …）
```

- **Broker**：Netty 数据面 + 存储引擎 + 内嵌 Raft 控制面（选主 / 元数据 / 消费组）
- **数据复制**：Kafka 式 ISR 拉取复制；`acks=0/1/-1` 三级确认
- **存储**：分区追加写日志 + 段文件 + mmap + 稀疏索引；双模式刷盘（`async` 高吞吐 / `sync` 零丢失）
- **协议**：自定义二进制 codec（VarInt 紧凑编码），非 HTTP
- **语义**：至少一次（at-least-once），消费端内置通用 HTTP 投递器，消息自带 url/method/headers/body

---

## 2. 工程结构

| 模块 | 说明 |
|------|------|
| `herald-common` | 消息模型、配置、snowflake、工具 |
| `herald-protocol` | 二进制编解码、帧、opcode |
| `herald-storage` | 存储引擎（段 / 索引 / 刷盘 / 恢复） |
| `herald-raft` | 内嵌 Raft（选举 / 日志 / 状态机 / 元数据） |
| `herald-server` | Broker（Netty 数据面 + 分区管理 + 复制） |
| `herald-producer` | 生产端 core |
| `herald-producer-spring-boot-starter` | 生产端 Starter |
| `herald-consumer` | 消费端 core + HTTP 投递器 |
| `herald-consumer-spring-boot-starter` | 消费端 Starter |
| `herald-examples` | server / producer / consumer 三个 Spring Boot 示例 |
| `herald-scripts` | Python 压测 / 可靠性测试脚本（直连二进制协议） |
| `docker` | Dockerfile / docker-compose 一键集群 |

---

## 3. 构建

依赖 JDK 17。本机 Maven 使用 IDEA 内置版本（未加入 PATH）：

```bash
MVN="/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven3/bin/mvn"

# 编译 + 打包（含三个可执行 fat jar）
"$MVN" -q -DskipTests package

# 跑全量单元/集成测试
"$MVN" test
```

产物：
- `herald-examples/server-example/target/server-example-1.0.0-SNAPSHOT.jar`
- `herald-examples/producer-example/target/producer-example-1.0.0-SNAPSHOT.jar`
- `herald-examples/consumer-example/target/consumer-example-1.0.0-SNAPSHOT.jar`

---

## 4. 本地运行

### 4.1 启动单节点 Broker

```bash
java -jar herald-examples/server-example/target/server-example-1.0.0-SNAPSHOT.jar
```

主要环境变量（前缀 `herald.broker.*`）：

| 变量 | 默认 | 说明 |
|------|------|------|
| `HERALD_NODE_ID` | `0` | 节点 ID |
| `HERALD_PORT` | `9092` | 数据面端口 |
| `HERALD_RAFT_PORT` | `0` | Raft 端口（集群需指定） |
| `HERALD_ADVERTISED_HOST` | `127.0.0.1` | 对外通告地址 |
| `HERALD_DATA_DIR` | `/data` | 数据目录 |
| `HERALD_DEFAULT_PARTITIONS` | `4` | 默认分区数 |
| `HERALD_REPLICATION_FACTOR` | `1` | 复制因子 |
| `HERALD_FLUSH_MODE` | `async` | 刷盘模式（`async` / `sync`） |
| `HERALD_PEERS` | 空 | 集群其余节点，格式 `id:host:dataPort:raftPort`（逗号分隔，不含自身） |

### 4.2 启动消费端（投递）

```bash
java -jar herald-examples/consumer-example/target/consumer-example-1.0.0-SNAPSHOT.jar
```

环境变量：`HERALD_BOOTSTRAP_SERVERS`、`HERALD_GROUP_ID`、`HERALD_TOPICS`。示例用自定义 `DeliveryHandler` 打印消息；生产环境可换成默认 `HttpDeliveryHandler`（真实发起 HTTP 调用）。

### 4.3 发送消息

```bash
HERALD_COUNT=5 HERALD_TARGET_URL="http://localhost:8080/callback" \
java -jar herald-examples/producer-example/target/producer-example-1.0.0-SNAPSHOT.jar
```

环境变量：`HERALD_BOOTSTRAP_SERVERS`、`HERALD_TOPIC`、`HERALD_TARGET_URL`、`HERALD_COUNT`。

---

## 5. Docker 一键起 3 节点集群

```bash
docker compose -f docker/docker-compose.yml up --build
```

- 3 个 Broker（node 0/1/2，Raft 端口 6000/6001/6002，数据面 9092，复制因子 3）
- `producer` 一次性发送 5 条消息，`consumer` 长驻消费投递
- 主机端口映射：`9092/9093/9094` → 三个 Broker

---

## 6. 压测与可靠性测试

脚本位于 `herald-scripts/`，纯 Python 直连二进制协议（无需 Java 客户端）：

```bash
# 吞吐 / 延迟压测（先启动 Broker）
python3 herald-scripts/benchmark.py --port 9092 --messages 100000 --acks 1

# 消息丢失测试（杀进程后重启统计）
python3 herald-scripts/message_loss_test.py \
  --broker-jar herald-examples/server-example/target/server-example-1.0.0-SNAPSHOT.jar
```

实测结果（单节点、1KB body、2 连接）：

| 模式 | acks | 吞吐 | ack 延迟 p50/p99 |
|------|------|------|------------------|
| async 高吞吐 | 0 | ~341k msg/s | — |
| async 高吞吐 | 1 | ~386k msg/s | 1.15 / 15.8 ms |
| sync 零丢失 | -1 | ~33k msg/s | 29.1 / 61.4 ms |

丢失测试：`sync` 档杀进程后 0 丢失；`async` 档因 mmap 页缓存，进程崩溃同样不丢（其刷盘丢失窗口仅体现在 OS/断电崩溃，详见报告）。

---

## 7. 配置与语义速览

| 维度 | 选项 |
|------|------|
| 刷盘 | `flush.mode=async`（高吞吐） / `sync`（零丢失） |
| 确认 | `acks=0`（不等）/ `1`（Leader）/ `-1`（ISR 全确认） |
| 语义 | 至少一次（at-least-once），下游按 `messageId` 幂等去重 |
| 死信 | 重试达上限写入 `{topic}.DLQ` |

---

## 8. 文档

- [design.md](design.md) — 完整设计文档（架构 / 存储 / 协议 / 一致性 / 可靠性 / 阶段划分）
- [docs/test-report.md](docs/test-report.md) — 测试报告与指标（单元测试、吞吐 / 延迟 / 丢失数）
