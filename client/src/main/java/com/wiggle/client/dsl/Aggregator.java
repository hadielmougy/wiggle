package com.wiggle.client.dsl;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The mandatory merge that follows a {@link WorkflowStream#fork}. Each branch runs on its own
 * isolated copy of the context, so nothing is merged implicitly; instead every branch's accumulated
 * result is handed to this function at once, keyed by branch name, and you return exactly the fields
 * to merge back -- so the merge is explicit, order-independent, and yours to define.
 *
 * <p>It is deliberately codec-free: you work with raw JSON maps and return the fields to merge back
 * into the context. The engine merges that return value shallowly, as with any step.
 */
@FunctionalInterface
public interface Aggregator {

    /**
     * @param context       the shared instance context as JSON (treat as read-only)
     * @param branchOutputs each branch's result, keyed by its branch name
     * @return the fields to merge back into the context (may be empty, never {@code null})
     */
    Map<String, Object> merge(Map<String, Object> context, Map<String, Object> branchOutputs);

    /**
     * A ready-made combine for the common case where the branches write <em>disjoint</em> fields:
     * it folds every branch's result together into one map. If two branches did write the same key,
     * the last branch (in fork order) wins -- so reach for a hand-written {@code combine} instead
     * when a conflict needs real resolution. This is the explicit, opt-in equivalent of what a
     * blind shallow merge used to do silently.
     */
    static Aggregator union() {
        return (context, branchOutputs) -> {
            Map<String, Object> merged = new LinkedHashMap<>();
            for (Object branch : branchOutputs.values()) {
                if (branch instanceof Map<?, ?> m) {
                    m.forEach((k, v) -> merged.put(String.valueOf(k), v));
                }
            }
            return merged;
        };
    }
}