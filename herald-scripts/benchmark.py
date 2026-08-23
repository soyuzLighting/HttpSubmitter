#!/usr/bin/env python3
"""Herald Broker 吞吐/延迟压测（直连二进制协议）。

用法示例：
  # 高吞吐档（acks=0，异步刷盘，单分区）
  python3 benchmark.py --messages 100000 --message-size 1024 --acks 0 --connections 1

  # 零丢失档（acks=-1，同步刷盘）
  python3 benchmark.py --messages 100000 --message-size 1024 --acks -1

测量：总吞吐（msg/s）、每批 ack 延迟（p50/p99/p99.9，acks>=1 时）。
"""
import argparse
import statistics
import threading
import time

from herald_protocol import Client, encode_message, produce


def percentile(values, p):
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def run_worker(host, port, topic, partition, acks, batch_size, msg_bytes,
               count, barrier, drain_msg, results, errors):
    client = Client(host, port)
    try:
        barrier.wait()
        sent = 0
        latencies = []
        start = time.perf_counter()
        while sent < count:
            n = min(batch_size, count - sent)
            batch = [msg_bytes] * n
            if acks == 0:
                produce(client, topic, partition, 0, batch)
            else:
                t0 = time.perf_counter()
                error_code, _, _ = produce(client, topic, partition, acks, batch)
                if error_code != 0:
                    raise RuntimeError("produce error code %d" % error_code)
                latencies.append(time.perf_counter() - t0)
            sent += n
        if acks == 0:
            # 单连接 FIFO：以一条 acks=1 哨兵确认前面全部 fire-and-forget 已落盘
            error_code, _, _ = produce(client, topic, partition, 1, [drain_msg])
            if error_code != 0:
                raise RuntimeError("drain produce error code %d" % error_code)
        end = time.perf_counter()
        results.append((end - start, sent, latencies))
    except Exception as exc:  # noqa: BLE001
        errors.append(exc)
    finally:
        client.close()


def main():
    ap = argparse.ArgumentParser(description="Herald broker throughput/latency benchmark")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9092)
    ap.add_argument("--topic", default="benchmark")
    ap.add_argument("--partitions", type=int, default=1)
    ap.add_argument("--messages", type=int, default=100000)
    ap.add_argument("--message-size", type=int, default=1024, help="body bytes per message")
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--acks", type=int, default=1, choices=[0, 1, -1])
    ap.add_argument("--connections", type=int, default=1)
    args = ap.parse_args()

    payload = b"x" * args.message_size
    msg_bytes = encode_message(url="http://bench.example/callback", method="POST", body=payload)
    drain_msg = encode_message(url="http://bench.example/callback", method="POST", body=b"__drain__")

    # 将消息总数尽量均分到各连接（余数给前几个）
    base, rem = divmod(args.messages, args.connections)
    counts = [base + (1 if i < rem else 0) for i in range(args.connections)]

    print("== Herald benchmark ==")
    print("  host/port    : %s:%d" % (args.host, args.port))
    print("  topic        : %s (partitions=%d)" % (args.topic, args.partitions))
    print("  messages     : %d  (body %d B)" % (args.messages, args.message_size))
    print("  batch-size   : %d" % args.batch_size)
    print("  acks         : %d" % args.acks)
    print("  connections  : %d" % args.connections)
    print("  on-wire msg  : %d B" % len(msg_bytes))

    results = []
    errors = []
    barrier = threading.Barrier(args.connections)
    threads = []
    for i in range(args.connections):
        partition = i % args.partitions
        t = threading.Thread(
            target=run_worker,
            args=(args.host, args.port, args.topic, partition, args.acks,
                  args.batch_size, msg_bytes, counts[i], barrier, drain_msg, results, errors),
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    if errors:
        print("  FAILED: %d worker error(s): %s" % (len(errors), errors[0]))
        return 1

    total_sec = sum(r[0] for r in results)
    total_sent = sum(r[1] for r in results)
    elapsed = max(r[0] for r in results)  # 墙钟取最慢连接
    all_lat = [l for r in results for l in r[2]]

    print("== result ==")
    print("  total messages : %d" % total_sent)
    print("  wall time      : %.3f s" % elapsed)
    print("  throughput     : %.0f msg/s" % (total_sent / elapsed if elapsed else 0))
    print("  bandwidth      : %.2f MB/s" % (total_sent * len(msg_bytes) / elapsed / 1e6 if elapsed else 0))
    if all_lat:
        print("  ack latency p50/p99/p99.9 : %.3f / %.3f / %.3f ms" % (
            percentile(all_lat, 0.50) * 1e3,
            percentile(all_lat, 0.99) * 1e3,
            percentile(all_lat, 0.999) * 1e3,
        ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
