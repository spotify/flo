package io.rouz.task.processor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * TODO: document.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.CLASS)
public @interface RootTask {
}
