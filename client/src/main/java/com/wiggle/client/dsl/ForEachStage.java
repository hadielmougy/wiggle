package com.wiggle.client.dsl;

import java.util.function.UnaryOperator;

/**
 * The mandatory stage after a {@link WorkflowBuilder#forEach}: one isolated branch ran per element
 * of the collection, and the results must now be combined explicitly. There is no implicit merge —
 * an item's writes never touch the shared context, so the only way item results reach the flow is
 * through the {@link #combine}.
 *
 * <p>{@code combine} is topology only: it declares the combine node (by name). The merge itself is a
 * handler bound on the worker — a method named the same as the combine taking an optional
 * {@link com.wiggle.client.worker.Context @Context} parameter (the pre-forEach context) and one
 * collection parameter that receives every item's final context: a {@code List} (ordered by item
 * index) or {@code Set} when the input was a list, or a {@code Map} keyed like the input when the
 * input was a map. Its return is the COMPLETE post-join context — the engine replaces the context
 * with it. The forEach leaves the stream with no open end, so a forgotten combine fails at
 * {@code build()}.
 *
 * @see WorkflowBuilder#forEach
 */
public final class ForEachStage {

    private final WorkflowBuilder stream;
    private final String name;
    private final String itemsKey;
    private final String itemKey;
    private final UnaryOperator<WorkflowBuilder> body;
    private boolean combined;

    ForEachStage(WorkflowBuilder stream, String name, String itemsKey, String itemKey,
                 UnaryOperator<WorkflowBuilder> body) {
        this.stream = stream;
        this.name = name;
        this.itemsKey = itemsKey;
        this.itemKey = itemKey;
        this.body = body;
    }

    /**
     * The mandatory merge for the preceding forEach. Declares the combine node named {@code name};
     * the engine collects every item's final context and hands the collection (plus the pre-forEach
     * context) to the matching worker handler, whose return is the complete post-join context.
     *
     * @param name the step name of the combine node (must be unique in the workflow)
     * @return the stream, reopened after the combine node
     */
    public WorkflowBuilder combine(String name) {
        if (combined) throw new IllegalStateException("combine already applied to this forEach");
        combined = true;
        stream.buildForEachCombine(this.name, itemsKey, itemKey, body, name);
        return stream;
    }
}
