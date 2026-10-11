package com.wiggle.server.store;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;
import com.wiggle.server.store.Rows.Token;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClaimFilterTest {

    private static Token task(String queue, String workflow, String step) {
        Token t = new Token();
        t.queue = queue;
        t.workflow = workflow;
        t.version = 1;
        t.activity = workflow + "#" + step;
        return t;
    }

    @Test
    @DisplayName("a poll by queue names is criteria without a predicate, and no queues is every queue")
    void byName() {
        assertEquals(new ClaimFilter(List.of(QueueCriteria.from("a"), QueueCriteria.from("b")), null),
                ClaimFilter.byName(List.of("a", "b"), null));
        assertTrue(ClaimFilter.byName(List.of(), null).admits(task("anything", "w", "s")));
        assertFalse(ClaimFilter.byName(List.of("a"), null).admits(task("b", "w", "s")));
    }

    @Test
    @DisplayName("each queue's predicate applies to that queue's tasks alone")
    void perQueue() {
        ClaimFilter f = new ClaimFilter(List.of(
                QueueCriteria.from("payments").where(cb -> cb.equal(Task_.STEP, "charge")),
                QueueCriteria.from("refunds")), null);
        assertTrue(f.admits(task("payments", "w", "charge")));
        assertFalse(f.admits(task("payments", "w", "capture")));
        assertTrue(f.admits(task("refunds", "w", "capture")), "refunds has no predicate");
        assertFalse(f.admits(task("notify", "w", "charge")), "a queue it does not name");
    }

    @Test
    @DisplayName("versions narrow every queue, and a queue named twice is refused")
    void versionsAndDuplicates() {
        ClaimFilter v2 = ClaimFilter.byName(List.of("a"), Set.of(new WorkflowVersion("w", 2)));
        assertFalse(v2.admits(task("a", "w", "s")), "version 1");
        assertThrows(IllegalArgumentException.class, () -> new ClaimFilter(List.of(
                QueueCriteria.from("a"), QueueCriteria.from("a").where(cb -> cb.equal(Task_.STEP, "s"))), null));
    }
}
