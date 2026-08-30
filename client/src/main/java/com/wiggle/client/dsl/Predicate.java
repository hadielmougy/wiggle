package com.wiggle.client.dsl;

/** A branch condition evaluated on a worker. */
@FunctionalInterface
public interface Predicate<T> {
    boolean test(T context) throws Exception;
}
