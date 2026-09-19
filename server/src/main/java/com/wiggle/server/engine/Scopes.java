package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.Node;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.TokenPayload;

/**
 * Reading and writing the nesting stack a token carries, mirroring the join stack frame for frame:
 * a fork pushes an {@code ARM} frame onto each branch, a forEach an {@code ITEM} frame onto each
 * element, and the join pops by restoration -- its continuation resumes from the fork token's own
 * payload, which never held the frame.
 *
 * <p>The TOP frame's view is the token's complete current context; the shared instance context is
 * simply the view when the stack is empty. Only the top frame is ever replaced: the frames beneath
 * are copies frozen at spawn time, which is what makes an item's base context stable for free.
 * Because depth is a list, fork, forEach and doWhile nest to any depth in any order.
 *
 * <p>Applying a result lands on that same top frame, so neither lifecycle has to know where a
 * token's context actually lives.
 */
final class Scopes {

    private Scopes() {}

    /** The current complete context: the top frame's view, or the shared context outside scopes. */
    static Doc view(Instance inst, TokenPayload payload) {
        TokenPayload.Frame top = payload.top();
        return top == null ? inst.context : top.view();
    }

    /** The view an item's {@code Step.base()} sees: the frame enclosing the innermost item, or the
     *  shared context when that item is at the outermost level. */
    static Doc baseOf(Instance inst, TokenPayload payload, int itemAt) {
        return itemAt > 0 ? payload.scopes().get(itemAt - 1).view() : inst.context;
    }

    /** The context a worker sees: the token's current scope view, with any staged combine inputs
     *  overlaid. A non-object view with staged inputs dispatches the staged inputs alone -- the
     *  view then rides on the activation's base context instead. */
    static Doc dispatchContext(Instance inst, Token t) {
        return view(inst, t.payload).plusAll(t.payload.staged());
    }

    /** The token's complete current context: its top scope frame's view, or the shared instance
     *  context outside any scope. */
    static Doc currentView(Instance inst, Token t) {
        return view(inst, t.payload);
    }

    static boolean isCombineNode(Node node) {
        return node != null && node.isCombine();
    }

    /** Once a combine node has run, the inputs staged for it have served their purpose: drop them
     *  so they never leak downstream. A no-op for any other node. */
    static TokenPayload stripCombineScratch(Node node, TokenPayload payload) {
        return isCombineNode(node) ? payload.withoutStaged() : payload;
    }

    /**
     * Applies a step result to the token's current scope: the return REPLACES the scope's view —
     * the top frame's view, or the shared context outside any scope. It is the complete next
     * context, and keys it omits do not survive. Step execution never diffs or merges; the only
     * merges are signal payloads and a sub-workflow's result folding back ({@link #mergeIntoScope}).
     * A combine's return is taken verbatim (an arm NAME may legitimately double as a data key) with
     * only the engine's reserved keys removed defensively; any other return drops top-level nulls.
     * A null return leaves the context untouched.
     */
    static void applyStepResult(Instance inst, Token t, Object result) {
        if (result == null) return;
        Doc cleaned = Doc.of(result).withoutNulls();
        if (t.payload.top() == null) {
            inst.context = cleaned;
            return;
        }
        t.payload = t.payload.withTopView(cleaned);
    }

    /**
     * The one remaining merge: EXTERNAL inputs folding back into the flow — a delivered signal's
     * payload and a completed sub-workflow's result. The merge lands on the waiting token's current
     * scope view (the shared context outside any scope), where the waiting flow actually resumes.
     * A map merges key-by-key (a null value deletes its key); any other value replaces the view
     * wholesale. Returns the continuation's payload.
     */
    static TokenPayload mergeIntoScope(Instance inst, TokenPayload payload, Object result) {
        if (result == null) return payload;
        TokenPayload.Frame top = payload.top();
        if (top == null) {
            inst.context = inst.context.merge(result);
            return payload;
        }
        return payload.withTopView(top.view().merge(result));
    }
}
