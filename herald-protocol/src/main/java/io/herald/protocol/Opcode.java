package io.herald.protocol;

/** 协议操作码。 */
public final class Opcode {

    public static final byte PRODUCE = 1;
    public static final byte PRODUCE_ACK = 2;
    public static final byte FETCH = 3;
    public static final byte FETCH_RESPONSE = 4;
    public static final byte COMMIT_OFFSET = 5;
    public static final byte COMMIT_ACK = 6;
    public static final byte METADATA = 7;
    public static final byte METADATA_RESPONSE = 8;
    public static final byte HEARTBEAT = 9;
    public static final byte HEARTBEAT_RESPONSE = 10;
    public static final byte REPLICA_FETCH = 11;   // 阶段 5 使用
    public static final byte REPLICA_RESPONSE = 12; // 阶段 5 使用
    public static final byte OFFSET_FETCH = 13;          // 查询消费组已提交位点
    public static final byte OFFSET_FETCH_RESPONSE = 14; // 位点返回

    private Opcode() {
    }
}
