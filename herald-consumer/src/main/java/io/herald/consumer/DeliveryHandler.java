package io.herald.consumer;

import io.herald.protocol.Message;

import java.util.concurrent.CompletableFuture;

/**
 * 投递 SPI：将一条消息投递给外部系统。默认实现为 {@link HttpDeliveryHandler}（通用 HTTP 投递）。
 *
 * <p>实现需保证返回的 Future 总会完成（用 {@code handle}/异常捕获把失败也映射为结果），
 * 成功返回 {@code true}，失败返回 {@code false}。</p>
 */
@FunctionalInterface
public interface DeliveryHandler {

    CompletableFuture<Boolean> deliver(Message message);
}
