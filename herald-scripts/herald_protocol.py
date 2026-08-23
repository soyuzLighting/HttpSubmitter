#!/usr/bin/env python3
"""Herald 二进制协议的纯 Python 实现（压测/可靠性脚本直连 Broker 使用）。

线格式与 Java 端 `io.herald.protocol` 一一对应：

帧:      magic(2)=0x4845 | version(1)=1 | opcode(1) | frameLen(4) | header | body
header:  varint(条目数) + 每个 KV: varint(len)+utf8
整数:    无符号 LEB128 varint（int32 按 Java 二补码语义）；长整数为 8 字节大端有符号。
字符串:  varint(len) + utf8 字节。
"""
import socket
import struct

MAGIC = 0x4845
VERSION = 1
HEADER_SIZE = 8

# opcode
OP_PRODUCE = 1
OP_PRODUCE_ACK = 2
OP_FETCH = 3
OP_FETCH_RESPONSE = 4
OP_COMMIT_OFFSET = 5
OP_COMMIT_ACK = 6
OP_METADATA = 7
OP_METADATA_RESPONSE = 8
OP_HEARTBEAT = 9
OP_HEARTBEAT_RESPONSE = 10
OP_REPLICA_FETCH = 11
OP_REPLICA_RESPONSE = 12
OP_OFFSET_FETCH = 13
OP_OFFSET_FETCH_RESPONSE = 14

# error code
ERROR_OK = 0
ERROR_UNKNOWN_TOPIC_OR_PARTITION = 1
ERROR_OFFSET_OUT_OF_RANGE = 2
ERROR_INTERNAL = 3
ERROR_MESSAGE_TOO_LARGE = 4
ERROR_NOT_LEADER_OR_FOLLOWER = 5


# ---- 基本编解码 ----

def encode_varint(value):
    """Java `putVarInt` 语义：按 32 位二补码取无符号后 LEB128。"""
    value &= 0xFFFFFFFF
    out = bytearray()
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def decode_varint(buf, pos):
    result = 0
    shift = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
        if shift > 28:
            raise ValueError("malformed varint")
    return result, pos


def encode_long(value):
    return struct.pack(">q", value)


def decode_long(buf, pos):
    return struct.unpack(">q", buf[pos:pos + 8])[0], pos + 8


def encode_string(value):
    b = value.encode("utf-8") if isinstance(value, str) else value
    return encode_varint(len(b)) + b


def decode_string(buf, pos):
    n, pos = decode_varint(buf, pos)
    return buf[pos:pos + n].decode("utf-8"), pos + n


def encode_bytes(b):
    return encode_varint(len(b)) + b


def decode_bytes(buf, pos):
    n, pos = decode_varint(buf, pos)
    return buf[pos:pos + n], pos + n


# ---- Message ----

def encode_message(message_id=0, offset=0, partition=-1, topic="", key="",
                   create_time=0, retry_count=0, url="", method="POST",
                   headers=None, body=b"", flags=0):
    """编码一条消息。生产时 messageId/offset/topic/partition 由 Broker 填充，可留默认。"""
    headers = headers or {}
    out = bytearray()
    out += encode_long(message_id)
    out += encode_long(offset)
    out += encode_varint(partition)
    out += encode_string(topic)
    out += encode_string(key)
    out += encode_long(create_time)
    out += encode_varint(retry_count)
    out += encode_string(url)
    out += encode_string(method)
    out += encode_varint(len(headers))
    for k, v in headers.items():
        out += encode_string(k)
        out += encode_string(v)
    out += encode_bytes(body)
    out += encode_varint(flags)
    return bytes(out)


def decode_message(buf, pos):
    message_id, pos = decode_long(buf, pos)
    offset, pos = decode_long(buf, pos)
    partition, pos = decode_varint(buf, pos)
    topic, pos = decode_string(buf, pos)
    key, pos = decode_string(buf, pos)
    create_time, pos = decode_long(buf, pos)
    retry_count, pos = decode_varint(buf, pos)
    url, pos = decode_string(buf, pos)
    method, pos = decode_string(buf, pos)
    n, pos = decode_varint(buf, pos)
    headers = {}
    for _ in range(n):
        k, pos = decode_string(buf, pos)
        v, pos = decode_string(buf, pos)
        headers[k] = v
    body, pos = decode_bytes(buf, pos)
    flags, pos = decode_varint(buf, pos)
    return {
        "message_id": message_id, "offset": offset, "partition": partition,
        "topic": topic, "key": key, "create_time": create_time,
        "retry_count": retry_count, "url": url, "method": method,
        "headers": headers, "body": body, "flags": flags,
    }, pos


# ---- 请求/响应 ----

def encode_produce_request(topic, partition, acks, messages):
    out = bytearray()
    out += encode_string(topic)
    out += encode_varint(partition)
    out += encode_varint(acks)
    out += encode_varint(len(messages))
    for m in messages:
        out += m
    return bytes(out)


def decode_produce_response(body):
    pos = 0
    error_code, pos = decode_varint(body, pos)
    base_offset, pos = decode_long(body, pos)
    n, pos = decode_varint(body, pos)
    offsets = []
    for _ in range(n):
        off, pos = decode_long(body, pos)
        offsets.append(off)
    return error_code, base_offset, offsets


def encode_fetch_request(topic, partition, fetch_offset, max_bytes, max_count):
    out = bytearray()
    out += encode_string(topic)
    out += encode_varint(partition)
    out += encode_long(fetch_offset)
    out += encode_varint(max_bytes)
    out += encode_varint(max_count)
    return bytes(out)


def decode_fetch_response(body):
    pos = 0
    error_code, pos = decode_varint(body, pos)
    next_offset, pos = decode_long(body, pos)
    n, pos = decode_varint(body, pos)
    messages = []
    for _ in range(n):
        m, pos = decode_message(body, pos)
        messages.append(m)
    return error_code, next_offset, messages


def encode_metadata_request(topic=""):
    return encode_string(topic)


def decode_metadata_response(body):
    pos = 0
    error_code, pos = decode_varint(body, pos)
    broker_count, pos = decode_varint(body, pos)
    brokers = {}
    for _ in range(broker_count):
        node_id, pos = decode_varint(body, pos)
        host, pos = decode_string(body, pos)
        port, pos = decode_varint(body, pos)
        brokers[node_id] = (host, port)
    topic_count, pos = decode_varint(body, pos)
    topic_leaders = {}
    for _ in range(topic_count):
        topic, pos = decode_string(body, pos)
        pc, pos = decode_varint(body, pos)
        leaders = []
        for _ in range(pc):
            lid, pos = decode_varint(body, pos)
            leaders.append(lid)
        topic_leaders[topic] = leaders
    return {"error_code": error_code, "brokers": brokers, "topic_leaders": topic_leaders}


# ---- 帧 ----

def encode_frame(opcode, header=None, body=b""):
    header = header or {}
    hb = bytearray()
    hb += encode_varint(len(header))
    for k, v in header.items():
        hb += encode_string(k)
        hb += encode_string(v)
    hb = bytes(hb)
    frame_len = len(hb) + len(body)
    head = struct.pack(">HBBI", MAGIC, VERSION, opcode, frame_len)
    return head + hb + body


def decode_frame(payload):
    """解码帧中 header+body 部分（不含 8 字节定长头）。返回 (header, body)。"""
    pos = 0
    n, pos = decode_varint(payload, pos)
    header = {}
    for _ in range(n):
        k, pos = decode_string(payload, pos)
        v, pos = decode_string(payload, pos)
        header[k] = v
    return header, payload[pos:]


# ---- 客户端 ----

class Client:
    """阻塞式长连接客户端。"""

    def __init__(self, host, port, timeout=10.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def _recv_exact(self, n):
        data = bytearray()
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise ConnectionError("connection closed by broker")
            data += chunk
        return bytes(data)

    def send(self, opcode, header=None, body=b""):
        self.sock.sendall(encode_frame(opcode, header, body))

    def recv_frame(self):
        head = self._recv_exact(HEADER_SIZE)
        magic, version, opcode, frame_len = struct.unpack(">HBBI", head)
        if magic != MAGIC:
            raise ValueError("bad magic 0x%x" % magic)
        if version != VERSION:
            raise ValueError("unsupported version %d" % version)
        payload = self._recv_exact(frame_len)
        header, body = decode_frame(payload)
        return opcode, header, body

    def round_trip(self, opcode, body=b"", header=None):
        self.send(opcode, header, body)
        return self.recv_frame()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---- 高层操作 ----

def produce(client, topic, partition, acks, messages):
    """发送一批（已编码）消息；acks=0 无响应返回 None，否则返回 (error_code, base_offset, offsets)。"""
    body = encode_produce_request(topic, partition, acks, messages)
    if acks == 0:
        client.send(OP_PRODUCE, body=body)
        return None
    _, _, resp_body = client.round_trip(OP_PRODUCE, body=body)
    return decode_produce_response(resp_body)


def fetch(client, topic, partition, fetch_offset, max_count=1000, max_bytes=64 * 1024 * 1024):
    body = encode_fetch_request(topic, partition, fetch_offset, max_bytes, max_count)
    _, _, resp_body = client.round_trip(OP_FETCH, body=body)
    return decode_fetch_response(resp_body)


def metadata(client, topic=""):
    body = encode_metadata_request(topic)
    _, _, resp_body = client.round_trip(OP_METADATA, body=body)
    return decode_metadata_response(resp_body)
