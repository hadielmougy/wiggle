package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;
import com.wiggle.server.engine.PollerRegistry.Coverage.State;
import com.wiggle.server.store.ClaimFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PollerRegistryTest {

    private static final long NOW = 1_000_000;

    private static ClaimFilter criteria(QueueCriteria c) {
        return new ClaimFilter(List.of(c), null);
    }

    @Test
    @DisplayName("WGL-QC-081: no poller is UNCOVERED, a plain one COVERED")
    void plainPollers() {
        PollerRegistry r = new PollerRegistry(60_000);
        assertEquals(State.UNCOVERED, r.coverage(new WorkflowVersion("orders", 1), "payments", NOW).state());
        r.seen("w1", ClaimFilter.byName(Set.of("payments"), null), NOW);
        assertEquals(State.COVERED, r.coverage(new WorkflowVersion("orders", 1), "payments", NOW).state());
        assertEquals(State.UNCOVERED, r.coverage(new WorkflowVersion("orders", 1), "refunds", NOW).state());
    }

    @Test
    @DisplayName("WGL-QC-081: criteria the slice decides are COVERED or skipped; others make it CONDITIONAL")
    void criteriaPollers() {
        PollerRegistry r = new PollerRegistry(60_000);
        r.seen("billing-only", criteria(QueueCriteria.from("payments").where(cb -> cb.equal(Task_.WORKFLOW, "billing"))), NOW);
        assertEquals(State.UNCOVERED, r.coverage(new WorkflowVersion("orders", 1), "payments", NOW).state(), "false for this slice");

        QueueCriteria eu = QueueCriteria.from("payments").where(cb -> cb.equal(Task_.attr("region", String.class), "eu"));
        r.seen("eu", criteria(eu), NOW);
        PollerRegistry.Coverage c = r.coverage(new WorkflowVersion("orders", 1), "payments", NOW);
        assertEquals(State.CONDITIONAL, c.state(), "depends on each task's region");
        assertEquals(List.of(eu.where()), c.conditions());

        r.seen("orders", criteria(QueueCriteria.from("payments").where(cb -> cb.equal(Task_.WORKFLOW, "orders"))), NOW);
        assertEquals(State.COVERED, r.coverage(new WorkflowVersion("orders", 1), "payments", NOW).state(), "true for every task of it");
    }

    @Test
    @DisplayName("WGL-QC-081: a poller's criteria count for their own queue only")
    void criteriaPerQueue() {
        PollerRegistry r = new PollerRegistry(60_000);
        r.seen("w", new ClaimFilter(List.of(
                QueueCriteria.from("payments").where(cb -> cb.equal(Task_.attr("region", String.class), "eu")),
                QueueCriteria.from("refunds")), null), NOW);
        assertEquals(State.CONDITIONAL, r.coverage(new WorkflowVersion("orders", 1), "payments", NOW).state());
        assertEquals(State.COVERED, r.coverage(new WorkflowVersion("orders", 1), "refunds", NOW).state());
        assertEquals(State.UNCOVERED, r.coverage(new WorkflowVersion("orders", 1), "notify", NOW).state());
    }

    @Test
    @DisplayName("WGL-QC-081: a version-scoped poller covers only its versions")
    void scopedPollers() {
        PollerRegistry r = new PollerRegistry(60_000);
        r.seen("v2", ClaimFilter.byName(Set.of("payments"), Set.of(new WorkflowVersion("orders", 2))), NOW);
        assertEquals(State.UNCOVERED, r.coverage(new WorkflowVersion("orders", 1), "payments", NOW).state());
        assertEquals(State.COVERED, r.coverage(new WorkflowVersion("orders", 2), "payments", NOW).state());
    }
}
