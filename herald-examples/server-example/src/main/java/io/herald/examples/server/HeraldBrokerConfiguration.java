package io.herald.examples.server;

import io.herald.server.Broker;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 将 {@link Broker} 装配为 Spring Bean 并随容器启动/关闭。 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(HeraldBrokerProperties.class)
public class HeraldBrokerConfiguration {

    @Bean(destroyMethod = "close")
    public Broker heraldBroker(HeraldBrokerProperties properties) throws Exception {
        Broker broker = new Broker(properties.toBrokerConfig());
        broker.start();
        return broker;
    }
}
