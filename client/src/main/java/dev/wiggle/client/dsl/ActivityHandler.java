package dev.wiggle.client.dsl;

/**
 * The JSON-level view of a step, produced by the DSL and consumed by the worker.
 * The typed lambda and its codec are closed over here, so the worker runtime stays
 * completely generic.
 */
@FunctionalInterface
public interface ActivityHandler {
    /**
     * @param context the instance context as JSON
     * @return for a task, the JSON object to merge back into the context;
     *         for a predicate, a {@link Boolean}
     */
    Object invoke(Object context) throws Exception;
}
