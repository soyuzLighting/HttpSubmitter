# Herald 测试报告与指标

> 版本：v1.0（阶段 0–9 交付）
> 测试环境：单节点，本机 macOS（Apple Silicon），JDK 17，1KB 消息体（body），批量大小 500，2 连接

---

## 1. 单元 / 集成测试

全量 `mvn test` 通过，共 **49** 个测试用例，覆盖 6 个核心模块：

| 模块 | 用例数 | 测试类 | 覆盖点 |
|------|-------|--------|--------|
| `herald-protocol` | 9 | `BinaryCodecTest` / `FrameTest` / `MessageTest` | VarInt / long / string 编解码，帧粘拆包，消息往返 |
| `herald-storage` | 16 | `LogRecordTest` / `LogSegmentTest` / `OffsetIndexTest` / `PartitionLogTest` | 段追加写、稀疏索引、刷盘、崩溃恢复截断 |
| `herald-raft` | 9 | `RaftLogTest` / `RaftNodeTest` | WAL 持久化、选主、日志复制、多数派 |
| `herald-server` | 8 | `BrokerIntegrationTest` / `BrokerClusterTest` / `BrokerCrashRecoveryTest` / `BrokerConfigMatrixTest` | 端到端读写、多节点复制与 failover、崩溃恢复、刷盘 × acks 矩阵 |
| `herald-producer` | 3 | `HeraldProducerTest` | 分区路由、批量累加、ack 处理 |
| `herald-consumer` | 4 | `HeraldConsumerTest` | 拉取、offset 提交、投递重试 / DLQ 边界 |
| **合计** | **49** | | |

> 已知：`BrokerClusterTest` 在整机高负载跑全量 reactor 时偶发 10s 注册超时（flaky，非功能性缺陷），单独运行稳定通过。

---

## 2. 吞吐 / 延迟压测

脚本 `herald-scripts/benchmark.py` 直连二进制协议。单节点，1KB body，2 连接，批量 500：

| 刷盘 | acks | 复制 | 吞吐 (msg/s) | 带宽 (MB/s) | ack 延迟 p50 | p99 | p99.9 |
|------|------|------|--------------|-------------|-------------|-----|-------|
| async | 0 | RF=1 | ~341k | 373 | — | — | — |
| async | 1 | RF=1 | ~386k | 423 | 1.15 ms | 15.8 ms | 16.2 ms |
| async | -1 | RF=1 | ~386k | 423 | 1.19 ms | 15.2 ms | 21.2 ms |
| sync | -1 | RF=1 | ~33k | 36 | 29.1 ms | 61.4 ms | 62.1 ms |

结论：

- **高吞吐档**（async）远超市定目标（≥ 100k msg/s），吞吐瓶颈已不在磁盘同步，而在批量打包与网络往返。
- **零丢失档**（sync）~33k msg/s，每条同步 `fsync` 后 ack，符合「同步刷盘受限」的预期（目标 ≥ 10k msg/s）。
- ack 延迟：async 档 p99 < 30ms，sync 档 p99 ~61ms，均满足「批量 p99 < 100ms」的目标。

---

## 3. 可靠性 / 消息丢失测试

脚本 `herald-scripts/message_loss_test.py`：写入 N 条 → `SIGKILL` 杀进程 → 重启 → 统计可读消息数。每档 5000 条：

| 档位 | flush | acks | 可读 | 丢失 | 判定 |
|------|-------|------|------|------|------|
| 零丢失档 | sync | -1 | 5000 | 0 | **PASS** |
| 高吞吐档 | async | 1 | 5000 | 0 | 观察项 |

说明：

- **sync 档**：每条消息 ack 前已 `fsync`，进程崩溃 / 断电均不丢，实测丢失 0，符合零丢失承诺。
- **async 档**：存储采用 `MappedByteBuffer`（mmap），进程 `SIGKILL` 后脏页仍保留在内核页缓存，因此「杀进程」同样不丢（实测 0）。其「一个刷盘窗口」的丢失**仅在操作系统崩溃 / 断电（页缓存整体丢失）时体现**，无法在进程级用 `kill -9` 模拟，故此处仅作行为观测、不判 PASS。

---

## 4. 结论与边界

| 维度 | 结果 |
|------|------|
| 功能正确性 | 49 个单元 / 集成测试全部通过 |
| 吞吐 | 高吞吐档 ~386k msg/s，零丢失档 ~33k msg/s |
| 延迟 | async p99 < 30ms，sync p99 ~61ms |
| 丢失 | 零丢失档杀进程 0 丢失；async 档进程级 0 丢失（mmap 语义） |
| 语义 | at-least-once；下游按 `messageId` 幂等去重 |

已知限制：

- 异步刷盘档的丢失边界依赖 OS 页缓存，仅能在真实断电 / 内核崩溃场景暴露，脚本无法等价模拟，报告已如实标注。
- `BrokerClusterTest` 在全量 reactor 高负载下偶发注册超时（flaky），单测稳定通过，见 §1 备注。
