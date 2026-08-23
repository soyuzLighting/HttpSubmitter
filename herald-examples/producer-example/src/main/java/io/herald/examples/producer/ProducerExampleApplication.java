package io.herald.examples.producer;

import io.herald.producer.HeraldProducer;
import io.herald.producer.SendResult;
import io.herald.producer.spring.EnableHeraldProducer;
import io.herald.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.nio.charset.StandardCharsets;

/**
 * 最小生产端示例：启动后向 {@code notify} topic 发送若干条通知，打印写入位点后退出。
 */
@SpringBootApplication
@EnableHeraldProducer
public class ProducerExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(ProducerExampleApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(ProducerExampleApplication.class, args);
    }

    @Bean
    CommandLineRunner sendSample(HeraldProducer producer) {
        return args -> {
            String topic = env("HERALD_TOPIC", "notify");
            String url = env("HERALD_TARGET_URL", "http://localhost:8080/callback");
            int count = Integer.parseInt(env("HERALD_COUNT", "3"));
            for (int i = 0; i < count; i++) {
                Message message = new Message()
                        .key("order-" + i)
                        .url(url)
                        .method("POST")
                        .addHeader("Content-Type", "application/json")
                        .body(("{\"order\":\"order-" + i + "\"}").getBytes(StandardCharsets.UTF_8));
                SendResult result = producer.send(topic, message);
                log.info("sent topic={} partition={} offset={} messageId={}",
                        result.topic(), result.partition(), result.offset(), result.messageId());
            }
        };
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isEmpty() ? defaultValue : v;
    }
}
