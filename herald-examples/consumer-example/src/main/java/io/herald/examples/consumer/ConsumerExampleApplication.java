package io.herald.examples.consumer;

import io.herald.consumer.DeliveryHandler;
import io.herald.consumer.spring.EnableHeraldConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * 最小消费端示例：消费 {@code notify} topic，通过自定义 {@link DeliveryHandler} 打印消息，
 * 演示投递 SPI（生产环境替换为真实 HTTP 投递器或直接使用默认 {@code HttpDeliveryHandler}）。
 */
@SpringBootApplication
@EnableHeraldConsumer
public class ConsumerExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(ConsumerExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ConsumerExampleApplication.class, args);
    }

    @Bean
    DeliveryHandler loggingDeliveryHandler() {
        return message -> {
            log.info("delivered messageId={} url={} method={} body={}",
                    message.messageId(), message.url(), message.method(),
                    new String(message.body(), StandardCharsets.UTF_8));
            return CompletableFuture.completedFuture(true);
        };
    }

    /** 消费循环为守护线程，此处阻塞主线程直至收到关闭信号。 */
    @Bean
    CommandLineRunner keepAlive() {
        return args -> {
            CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown, "herald-consumer-shutdown"));
            latch.await();
        };
    }
}
