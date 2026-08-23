package io.herald.protocol;

/** 协议错误码。 */
public final class ErrorCode {

    public static final int OK = 0;
    public static final int UNKNOWN_TOPIC_OR_PARTITION = 1;
    public static final int OFFSET_OUT_OF_RANGE = 2;
    public static final int INTERNAL = 3;
    public static final int MESSAGE_TOO_LARGE = 4;
    public static final int NOT_LEADER_OR_FOLLOWER = 5;

    private ErrorCode() {
    }
}
