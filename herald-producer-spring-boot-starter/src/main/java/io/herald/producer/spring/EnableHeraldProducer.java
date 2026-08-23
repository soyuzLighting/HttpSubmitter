package io.herald.producer.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 启用 Herald 生产端，注入 {@code HeraldProducer} Bean。 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(HeraldProducerAutoConfiguration.class)
public @interface EnableHeraldProducer {
}
