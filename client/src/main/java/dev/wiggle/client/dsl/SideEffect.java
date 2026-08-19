package dev.wiggle.client.dsl;

/** A step that observes the context without changing it -- the workflow analogue of Stream.peek. */
@FunctionalInterface
public interface SideEffect<T> {
    void accept(T context) throws Exception;
}
