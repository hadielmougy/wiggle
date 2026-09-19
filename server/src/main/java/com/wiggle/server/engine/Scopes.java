package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.TokenPayload;

/**
 * Reading the nesting stack a token carries, mirroring the join stack frame for frame: a fork
 * pushes an {@code ARM} frame onto each branch, a forEach an {@code ITEM} frame onto each element,
 * and the join pops by restoration -- its continuation resumes from the fork token's own payload,
 * which never held the frame.
 *
 * <p>The TOP frame's view is the token's complete current context; the shared instance context is
 * simply the view when the stack is empty. Only the top frame is ever replaced: the frames beneath
 * are copies frozen at spawn time, which is what makes an item's base context stable for free.
 * Because depth is a list, fork, forEach and doWhile nest to any depth in any order.
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
}
