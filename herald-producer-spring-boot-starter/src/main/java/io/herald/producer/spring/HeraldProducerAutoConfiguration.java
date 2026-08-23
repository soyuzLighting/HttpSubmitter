package io.herald.producer.spring;

import io.herald.producer.HeraldProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 自动装配 {@link HeraldProducer}，由 {@link EnableHeraldProducer} 引入。 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(HeraldProducer.class)
@EnableConfigurationProperties(HeraldProducerProperties.class)
public class HeraldProducerAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public HeraldProducer heraldProducer(HeraldProducerProperties properties) {
        return HeraldProducer.builder().config(properties.toProducerConfig()).build();
    }
}
