package io.rouz.flo;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface Fn1<T, R> extends Function<T, R>, Serializable {
}
