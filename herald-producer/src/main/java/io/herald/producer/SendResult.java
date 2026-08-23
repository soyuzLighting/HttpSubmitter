package io.herald.producer;

import io.herald.protocol.ErrorCode;

/** 一次发送的结果：消息在分区内的写入位置。 */
public record SendResult(String topic, int partition, long offset, long messageId, int errorCode) {

    public boolean ok() {
        return errorCode == ErrorCode.OK;
    }
}
