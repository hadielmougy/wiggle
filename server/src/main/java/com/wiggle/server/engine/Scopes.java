package com.wiggle.server.engine;

import com.wiggle.core.Json;
import com.wiggle.server.store.Rows.Instance;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The nesting-scope stack a token carries in its payload, mirroring the join stack frame for
 * frame: a fork pushes an {@code arm} frame onto each branch child, a forEach pushes an
 * {@code item} frame onto each element child, and the join pops by restoration — its
 * continuation resumes from the fork token's own payload, which never held the frame.
 *
 * <p>The TOP frame's {@code view} is the token's complete current context; the shared instance
 * context is simply the view when the stack is empty. Only the top frame is ever mutated: the
 * frames beneath are copies frozen at spawn time, which is what makes an item's base context
 * (the view of the frame below it) stable for free. Because depth is a list, fork, forEach and
 * doWhile nest to any depth in any order.
 *
 * <p>Frame shape: {@code {"kind": "arm"|"item", "idx": n, "mapKey": k?, "view": <any JSON>}}.
 */
final class Scopes {

    /** Payload key holding the frame list. Reserved, like {@link WorkflowEngine#LOOP_COUNTS}. */
    static final String SCOPES = "__scopes__";

    static final String KIND = "kind";
    static final String IDX = "idx";
    static final String MAP_KEY = "mapKey";
    static final String VIEW = "view";
    static final String ARM = "arm";
    static final String ITEM = "item";

    private Scopes() {}

    /** The parsed payload, an empty map for a token that has none. */
    static Map<String, Object> payload(String payloadJson) {
        return payloadJson == null ? new LinkedHashMap<>() : Json.parseObject(payloadJson);
    }

    /** The frame stack inside {@code payload} (live reference), outermost first; empty if none. */
    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> frames(Map<String, Object> payload) {
        Object frames = payload.get(SCOPES);
        return frames == null ? List.of() : (List<Map<String, Object>>) frames;
    }

    /** The current complete context: the top frame's view, or the shared context outside scopes. */
    static Object view(Instance inst, Map<String, Object> payload) {
        List<Map<String, Object>> frames = frames(payload);
        return frames.isEmpty() ? Json.parse(inst.contextJson) : frames.getLast().get(VIEW);
    }

    /** Staged combine inputs riding beside the stack: every payload key the engine does not
     *  reserve. Non-empty only on a join's continuation, until the combine strips them. */
    static Map<String, Object> staged(Map<String, Object> payload) {
        Map<String, Object> staged = new LinkedHashMap<>(payload);
        staged.remove(SCOPES);
        staged.remove(WorkflowEngine.LOOP_COUNTS);
        return staged;
    }

    /** A child payload: {@code basePayloadJson}'s stack (copied) plus one pushed frame whose view
     *  starts as {@code view}. */
    static String push(String basePayloadJson, String kind, long idx, String mapKey, Object view) {
        Map<String, Object> payload = payload(basePayloadJson);
        List<Map<String, Object>> frames = new ArrayList<>(frames(payload));
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put(KIND, kind);
        frame.put(IDX, idx);
        if (mapKey != null) frame.put(MAP_KEY, mapKey);
        frame.put(VIEW, view);
        frames.add(frame);
        payload.put(SCOPES, frames);
        return Json.write(payload);
    }

    /** The position of the innermost {@code item} frame, or -1: where a step's ambient
     *  {@code Step.itemIndex()}/{@code itemMapKey()}/{@code base()} come from, at any depth. */
    static int innermostItem(List<Map<String, Object>> frames) {
        for (int i = frames.size() - 1; i >= 0; i--) {
            if (ITEM.equals(frames.get(i).get(KIND))) return i;
        }
        return -1;
    }
}
