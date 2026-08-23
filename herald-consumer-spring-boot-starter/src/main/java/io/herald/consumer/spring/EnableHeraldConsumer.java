package io.herald.consumer.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 启用 Herald 消费端，注入并启动 {@code HeraldConsumer} Bean。 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(HeraldConsumerAutoConfiguration.class)
public @interface EnableHeraldConsumer {
}
