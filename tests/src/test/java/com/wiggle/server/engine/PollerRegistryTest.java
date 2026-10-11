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
        return new ClaimFilter(Set.copyOf(c.queues()), null, c.where());
    }

    @Test
    @DisplayName("WGL-QC-081: no poller is UNCOVERED, a plain one COVERED")
    void plainPollers() {
        PollerRegistry r = new PollerRegistry(60_000);
        assertEquals(State.UNCOVERED, r.coverage("orders", 1, "payments", NOW).state());
        r.seen("w1", new ClaimFilter(Set.of("payments"), null, null), NOW);
        assertEquals(State.COVERED, r.coverage("orders", 1, "payments", NOW).state());
        assertEquals(State.UNCOVERED, r.coverage("orders", 1, "refunds", NOW).state());
    }

    @Test
    @DisplayName("WGL-QC-081: criteria the slice decides are COVERED or skipped; others make it CONDITIONAL")
    void criteriaPollers() {
        PollerRegistry r = new PollerRegistry(60_000);
        r.seen("billing-only", criteria(QueueCriteria.from("payments").where(cb -> cb.equal(Task_.WORKFLOW, "billing"))), NOW);
        assertEquals(State.UNCOVERED, r.coverage("orders", 1, "payments", NOW).state(), "false for this slice");

        QueueCriteria eu = QueueCriteria.from("payments").where(cb -> cb.equal(Task_.attr("region", String.class), "eu"));
        r.seen("eu", criteria(eu), NOW);
        PollerRegistry.Coverage c = r.coverage("orders", 1, "payments", NOW);
        assertEquals(State.CONDITIONAL, c.state(), "depends on each task's region");
        assertEquals(List.of(eu.where()), c.conditions());

        r.seen("orders", criteria(QueueCriteria.from("payments").where(cb -> cb.equal(Task_.WORKFLOW, "orders"))), NOW);
        assertEquals(State.COVERED, r.coverage("orders", 1, "payments", NOW).state(), "true for every task of it");
    }

    @Test
    @DisplayName("WGL-QC-081: a version-scoped poller covers only its versions")
    void scopedPollers() {
        PollerRegistry r = new PollerRegistry(60_000);
        r.seen("v2", new ClaimFilter(Set.of("payments"), Set.of(new WorkflowVersion("orders", 2)), null), NOW);
        assertEquals(State.UNCOVERED, r.coverage("orders", 1, "payments", NOW).state());
        assertEquals(State.COVERED, r.coverage("orders", 2, "payments", NOW).state());
    }
}
