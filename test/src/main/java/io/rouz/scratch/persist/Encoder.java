package io.rouz.scratch.persist;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.temporal.ChronoUnit;

/**
 * TODO: document.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Encoder {

  int ttl() default 30;
  ChronoUnit unit() default ChronoUnit.SECONDS;
}
