package com.wiggle.client.dsl;

/** A step body. Receives the current context, returns the next one. */
@FunctionalInterface
public interface Activity<T> {
    T apply(T context) throws Exception;
}
