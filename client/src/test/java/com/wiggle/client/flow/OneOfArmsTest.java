package com.wiggle.client.flow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The two shapes a {@link Wiggle#oneOf} cannot have, and the two it can.
 *
 * <p>Both rules follow from one fact: the arms' guards are evaluated in the order the arms are given
 * to {@code oneOf}, and the first to hold wins. So {@code otherwise()} -- the arm with no guard --
 * has to be last, or the arms after it are unreachable; and it needs a guarded arm beside it, or
 * there is no guard for it to be the alternative to and it simply always runs.
 *
 * <p>The rules held before this test existed; nothing covered them. That is the gap it fills: they
 * live in one private method that a refactor could drop without any other test noticing, and the
 * damage would be silent — a workflow that compiles, registers, and quietly never runs an arm.
 */
class OneOfArmsTest {

    interface S {
        boolean vip(Map<String, Object> c);
        boolean gold(Map<String, Object> c);
        Map<String, Object> premium(Map<String, Object> c);
        Map<String, Object> standard(Map<String, Object> c);
        Map<String, Object> plain(Map<String, Object> c);
    }

    @Test @DisplayName("otherwise() alone is rejected: no guard for it to be the alternative to")
    void otherwiseNeedsAGuardedArm() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                FlowSpec.define("oo-lone-otherwise", Map.class, S.class, (f, s) ->
                        Wiggle.oneOf(f.otherwise().thenApply(s::plain))));
        assertTrue(e.getMessage().contains("at least one when(...) arm"), e.getMessage());
    }

    @Test @DisplayName("otherwise() before a guarded arm is rejected: that arm could never run")
    void otherwiseMustBeLast() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                FlowSpec.define("oo-otherwise-first", Map.class, S.class, (f, s) ->
                        Wiggle.oneOf(f.otherwise().thenApply(s::plain),
                                     f.when(s::vip).thenApply(s::premium))));
        assertTrue(e.getMessage().contains("must be the last arm"), e.getMessage());
    }

    @Test @DisplayName("otherwise() in the middle is rejected too, not just in first position")
    void otherwiseInTheMiddle() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                FlowSpec.define("oo-otherwise-middle", Map.class, S.class, (f, s) ->
                        Wiggle.oneOf(f.when(s::vip).thenApply(s::premium),
                                     f.otherwise().thenApply(s::plain),
                                     f.when(s::gold).thenApply(s::standard))));
        assertTrue(e.getMessage().contains("must be the last arm"), e.getMessage());
        assertTrue(e.getMessage().contains("Arm 2 of 3"), "the message should say which arm: " + e.getMessage());
    }

    @Test @DisplayName("two otherwise() arms are rejected -- the same rule, since only one can be last")
    void onlyOneOtherwise() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                FlowSpec.define("oo-two-otherwise", Map.class, S.class, (f, s) ->
                        Wiggle.oneOf(f.otherwise().thenApply(s::plain),
                                     f.otherwise().thenApply(s::standard))));
        assertTrue(e.getMessage().contains("only be one"), e.getMessage());
    }

    @Test @DisplayName("guards with a trailing otherwise() is the ordinary shape")
    void guardsThenOtherwiseIsFine() {
        assertDoesNotThrow(() ->
                FlowSpec.define("oo-ok", Map.class, S.class, (f, s) ->
                        Wiggle.oneOf(f.when(s::vip).thenApply(s::premium),
                                     f.when(s::gold).thenApply(s::standard),
                                     f.otherwise().thenApply(s::plain))));
    }

    @Test @DisplayName("a guarded arm needs no otherwise(): unmatched simply falls past the choice")
    void otherwiseIsOptional() {
        assertDoesNotThrow(() ->
                FlowSpec.define("oo-no-default", Map.class, S.class, (f, s) ->
                        Wiggle.oneOf(f.when(s::vip).thenApply(s::premium))));
    }
}
