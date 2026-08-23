#!/usr/bin/env python3
"""消息丢失测试：向 Broker 写入 N 条消息后 SIGKILL，重启后统计可读消息数，量化丢失边界。

两种刷盘档位（对应 design §5.3 / §10.3）：
  sync  同步刷盘 + acks=-1 —— 每条 ack 前已 fsync，进程崩溃/断电均不丢，期望丢失 0。
  async 异步刷盘 + acks=1  —— 依赖 mmap 页缓存，进程崩溃（SIGKILL）时脏页仍在内核页缓存中，
                              因此「杀进程」同样不丢（expected 0）；其「一个刷盘窗口」的丢失
                              仅在操作系统崩溃/断电（页缓存整体丢失）时体现，本脚本无法在
                              进程级模拟，仅作行为观测。

用法：
  python3 message_loss_test.py --broker-jar ../herald-examples/server-example/target/server-example-1.0.0-SNAPSHOT.jar

依赖：Herald server-example 可执行 fat jar（阶段 7 构建产物）。
"""
import argparse
import os
import signal
import socket
import subprocess
import time

from herald_protocol import Client, encode_message, produce, fetch, metadata


def wait_port(host, port, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=1.0)
            s.close()
            return True
        except OSError:
            time.sleep(0.2)
    return False


def start_broker(jar, data_dir, port, flush_mode):
    env = os.environ.copy()
    env.update({
        "HERALD_PORT": str(port),
        "HERALD_DATA_DIR": data_dir,
        "HERALD_FLUSH_MODE": flush_mode,
        "HERALD_DEFAULT_PARTITIONS": "1",
        "HERALD_REPLICATION_FACTOR": "1",
        "HERALD_ADVERTISED_HOST": "127.0.0.1",
    })
    proc = subprocess.Popen(["java", "-jar", jar], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_port("127.0.0.1", port):
        proc.kill()
        proc.wait()
        raise RuntimeError("broker did not start listening on %d" % port)
    return proc


def send_messages(port, topic, messages, acks):
    client = Client("127.0.0.1", port)
    msg = encode_message(url="http://loss.example/callback", method="POST", body=b"x" * 64)
    batch = [msg] * 1000
    sent = 0
    while sent < messages:
        n = min(1000, messages - sent)
        b = batch[:n]
        resp = produce(client, topic, 0, acks, b)
        if resp is not None and resp[0] != 0:
            raise RuntimeError("produce error code %d" % resp[0])
        sent += n
    client.close()


def count_messages(port, topic):
    client = Client("127.0.0.1", port)
    # 等 topic 元数据在重启后可见（Raft 重放 + 分区日志重开）
    for _ in range(100):
        try:
            md = metadata(client, topic)
            if md["topic_leaders"].get(topic):
                break
        except Exception:
            pass
        time.sleep(0.2)
    off = 0
    total = 0
    while True:
        code, next_off, msgs = fetch(client, topic, 0, off, max_count=100000)
        if code != 0:
            break
        total += len(msgs)
        if not msgs or next_off <= off:
            break
        off = next_off
    client.close()
    return total


def run_case(jar, data_dir, port, flush_mode, acks, messages):
    print("  flush=%s acks=%d messages=%d" % (flush_mode, acks, messages))
    proc = start_broker(jar, data_dir, port, flush_mode)
    send_messages(port, "loss-topic", messages, acks)
    # 最后一条 ack 返回后立即杀进程
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()

    proc2 = start_broker(jar, data_dir, port, flush_mode)
    readable = count_messages(port, "loss-topic")
    os.kill(proc2.pid, signal.SIGKILL)
    proc2.wait()

    lost = messages - readable
    print("  readable=%d lost=%d (%.2f%%)" % (readable, lost, lost * 100.0 / messages))
    return lost


def main():
    ap = argparse.ArgumentParser(description="Herald message loss test (kill -9 + restart)")
    ap.add_argument("--broker-jar", required=True,
                    help="path to server-example fat jar")
    ap.add_argument("--data-dir", default="/tmp/herald-loss-test")
    ap.add_argument("--port", type=int, default=19092)
    ap.add_argument("--messages", type=int, default=5000)
    ap.add_argument("--mode", choices=["sync", "async", "both"], default="both")
    args = ap.parse_args()

    if not os.path.isfile(args.broker_jar):
        print("broker jar not found: %s" % args.broker_jar)
        return 2

    print("== Herald message loss test ==")
    print("  broker jar : %s" % args.broker_jar)
    print("  data dir   : %s" % args.data_dir)
    print("  port       : %d" % args.port)

    results = {}
    if args.mode in ("sync", "both"):
        d = args.data_dir + "-sync"
        subprocess.run(["rm", "-rf", d], check=False)
        results["sync"] = run_case(args.broker_jar, d, args.port, "sync", -1, args.messages)

    if args.mode in ("async", "both"):
        d = args.data_dir + "-async"
        subprocess.run(["rm", "-rf", d], check=False)
        results["async"] = run_case(args.broker_jar, d, args.port, "async", 1, args.messages)

    print("== summary ==")
    ok = True
    if "sync" in results:
        lost = results["sync"]
        status = "PASS (0 lost)" if lost == 0 else "FAIL (lost %d)" % lost
        ok = ok and lost == 0
        print("  sync  (零丢失档): %s" % status)
    if "async" in results:
        lost = results["async"]
        # mmap 存储下进程崩溃不丢脏页；异步刷盘窗口仅在 OS/断电崩溃时体现
        print("  async (高吞吐档): lost=%d（mmap 页缓存不随进程退出而丢；刷盘窗口仅 OS/断电崩溃时体现）" % lost)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
