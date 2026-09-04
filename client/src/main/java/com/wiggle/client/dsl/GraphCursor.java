package com.wiggle.client.dsl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Tracks a workflow graph's <em>open ends</em> -- the node edges appended but not yet wired to a
 * successor -- and threads them as new nodes attach. It is the one place that knows how a stream's
 * first node is reported (the root reports the graph's entry, a branch captures its start for the
 * parent to wire) and how open edges are connected, so the fluent {@link WorkflowBuilder} operators
 * read as composition over it rather than hand-managing state.
 *
 * <p>Package-private and mutable: one cursor per {@link WorkflowBuilder} (the root and each nested
 * branch/case/loop sub-builder each own one).
 */
final class GraphCursor {

    /** Which outgoing edge of a node an open end occupies. */
    private enum Edge { NEXT, ALT }

    /** A dangling edge: node {@code node}'s {@code edge} has no successor yet. */
    private record OpenEnd(String node, Edge edge) {}

    private final Pipeline pipeline;
    /** What to do with the first node attached to this cursor's stream: the root reports the graph's
     *  entry ({@code pipeline::startAt}); a branch captures it for its parent to wire in. */
    private final Consumer<String> startSink;
    /** Non-null for a branch cursor: where a short-circuit (a false gate) must land. */
    private final String enclosingJoinId;
    private List<OpenEnd> openEnds = new ArrayList<>();

    private GraphCursor(Pipeline pipeline, Consumer<String> startSink, String enclosingJoinId) {
        this.pipeline = pipeline;
        this.startSink = startSink;
        this.enclosingJoinId = enclosingJoinId;
    }

    /** The root cursor: the first node attached to it is the workflow's entry point. */
    static GraphCursor root(Pipeline pipeline) {
        return new GraphCursor(pipeline, pipeline::startAt, null);
    }

    /** A branch cursor: its first node is handed to {@code startSink} for the parent to wire, and a
     *  short-circuiting gate falls back to {@code enclosingJoinId}. */
    static GraphCursor branch(Pipeline pipeline, String enclosingJoinId, Consumer<String> startSink) {
        return new GraphCursor(pipeline, startSink, enclosingJoinId);
    }

    Pipeline pipeline() { return pipeline; }

    String enclosingJoinId() { return enclosingJoinId; }

    /** Appends {@code id}: routes any pending open ends into it (or reports it as the start), then
     *  makes it the sole open end on its NEXT edge. */
    void attach(String id) {
        routeInto(id);
        openAtNext(id);
    }

    /** Routes the current open ends into {@code id} -- or reports {@code id} as the stream's start when
     *  there are none -- WITHOUT opening a new end. For callers that then manage the open set
     *  themselves (a loop wiring its body's entry, for example). */
    void routeInto(String id) {
        if (openEnds.isEmpty()) startSink.accept(id);
        else wireOpenEndsTo(id);
    }

    /** Makes {@code id}'s NEXT edge the sole open end. */
    void openAtNext(String id) { openAt(id, Edge.NEXT); }

    /** Makes {@code id}'s ALT (false / escalation) edge the sole open end. */
    void openAtAlt(String id) { openAt(id, Edge.ALT); }

    private void openAt(String id, Edge edge) {
        openEnds = new ArrayList<>(List.of(new OpenEnd(id, edge)));
    }

    /** Wires every open end to {@code target} and clears the set. */
    void wireOpenEndsTo(String target) {
        for (OpenEnd end : openEnds) wire(end, target);
        clearOpenEnds();
    }

    /** Wires one node's true (NEXT) edge directly to {@code target}, leaving the open set untouched. */
    void wireTrue(String from, String target) { pipeline.wireNext(from, target); }

    /** Wires one node's false (ALT) edge directly to {@code target}, leaving the open set untouched. */
    void wireFalse(String from, String target) { pipeline.wireAlt(from, target); }

    /** Drops all open ends without wiring them -- a caller is about to rebuild the set. */
    void clearOpenEnds() {
        openEnds = new ArrayList<>();
    }

    /** Adds {@code id}'s ALT edge as an additional open end (a guard's unmatched fall-through). */
    void addOpenEndAlt(String id) {
        openEnds.add(new OpenEnd(id, Edge.ALT));
    }

    /** Absorbs another cursor's open ends into this one -- used to merge branch/case tails. */
    void absorb(GraphCursor other) {
        openEnds.addAll(other.openEnds);
    }

    private void wire(OpenEnd end, String target) {
        switch (end.edge()) {
            case NEXT -> pipeline.wireNext(end.node(), target);
            case ALT -> pipeline.wireAlt(end.node(), target);
        }
    }
}
