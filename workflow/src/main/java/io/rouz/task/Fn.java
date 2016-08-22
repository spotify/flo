package io.rouz.task;

import java.io.Serializable;
import java.util.function.Supplier;

@FunctionalInterface
public interface Fn<R> extends Supplier<R>, Serializable {
}
