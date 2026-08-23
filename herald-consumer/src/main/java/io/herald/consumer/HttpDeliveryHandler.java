package io.herald.consumer;

import io.herald.protocol.Message;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 通用 HTTP 投递器：按消息的 url/method/headers/body 发起 HTTP 调用，
 * 状态码 {@code 2xx/3xx} 视为成功。
 */
public final class HttpDeliveryHandler implements DeliveryHandler {

    private final HttpClient client;
    private final int timeoutMs;

    public HttpDeliveryHandler(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(timeoutMs))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @Override
    public CompletableFuture<Boolean> deliver(Message message) {
        try {
            HttpRequest request = build(message);
            return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                    .handle((resp, err) -> err == null
                            && resp.statusCode() >= 200 && resp.statusCode() < 400);
        } catch (RuntimeException e) {
            return CompletableFuture.completedFuture(false);
        }
    }

    private HttpRequest build(Message message) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(message.url()))
                .timeout(Duration.ofMillis(timeoutMs));
        for (Map.Entry<String, String> e : message.headers().entrySet()) {
            b.header(e.getKey(), e.getValue());
        }
        String method = message.method() == null || message.method().isEmpty() ? "POST" : message.method();
        HttpRequest.BodyPublisher body = message.body() == null || message.body().length == 0
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(message.body());
        b.method(method, body);
        return b.build();
    }
}
