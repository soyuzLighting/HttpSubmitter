package io.herald.consumer.spring;

import io.herald.consumer.HeraldConsumer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 自动装配 {@link HeraldConsumer}，由 {@link EnableHeraldConsumer} 引入，并随容器启动。 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(HeraldConsumer.class)
@EnableConfigurationProperties(HeraldConsumerProperties.class)
public class HeraldConsumerAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public HeraldConsumer heraldConsumer(HeraldConsumerProperties properties) {
        HeraldConsumer consumer = HeraldConsumer.builder()
                .config(properties.toConsumerConfig())
                .deliveryConfig(properties.toDeliveryConfig())
                .build();
        consumer.start();
        return consumer;
    }
}
