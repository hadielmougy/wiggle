package com.wiggle.core.criteria;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CriteriaTest {

    private static final TaskFacts CHARGE =
            new TaskFacts("orders", 3, "payments", "charge", "n2", "TASK", 1, "ord-7", 1_000);

    @Test
    @DisplayName("WGL-QC-010: criteria name one non-blank queue, and a poll names each queue once")
    void needAQueue() {
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from(" "));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from(null));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.forPoll(java.util.List.of()));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.forPoll(java.util.List.of(
                QueueCriteria.from("payments"),
                QueueCriteria.from("payments").where(cb -> cb.equal(Task_.STEP, "charge")))));
    }

    @Test
    @DisplayName("WGL-QC-013: criteria are values")
    void areValues() {
        QueueCriteria a = QueueCriteria.from("payments").where(cb -> cb.equal(Task_.STEP, "charge"));
        QueueCriteria b = QueueCriteria.from("payments").where(cb -> cb.equal(Task_.STEP, "charge"));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    @DisplayName("matches tests the queue as well as the predicate")
    void matchesTestsTheQueue() {
        QueueCriteria c = QueueCriteria.from("payments").where(cb -> cb.equal(Task_.WORKFLOW, "orders"));
        assertTrue(c.matches(CHARGE));
        assertFalse(QueueCriteria.from("refunds").matches(CHARGE));
    }

    @Test
    @DisplayName("WGL-QC-051: depth, size, in-list and text limits are enforced")
    void limits() {
        Predicate deep = new Predicate.Comparison(Field.ATTEMPT, Op.EQUAL, List.of(1L));
        for (int i = 0; i < Criteria.MAX_DEPTH; i++) deep = new Predicate.Not(deep);
        Predicate tooDeep = deep;
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q").where(tooDeep));

        List<Predicate> many = new ArrayList<>(Collections.nCopies(Criteria.MAX_NODES,
                new Predicate.Comparison(Field.ATTEMPT, Op.EQUAL, List.of(1L))));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q").where(new Predicate.And(many)));

        List<Long> versions = new ArrayList<>();
        for (long v = 0; v <= Criteria.MAX_IN; v++) versions.add(v);
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(new Predicate.Comparison(Field.VERSION, Op.IN, new ArrayList<>(versions))));

        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(cb -> cb.equal(Task_.CORRELATION_ID, "x".repeat(Criteria.MAX_TEXT + 1))));
    }

    @Test
    @DisplayName("WGL-QC-051: a literal of the wrong type, a wrong argument count, or STEP ordering are refused")
    void typing() {
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(new Predicate.Comparison(Field.VERSION, Op.EQUAL, List.of("3"))));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(new Predicate.Comparison(Field.WORKFLOW, Op.BETWEEN, List.of("a"))));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(cb -> cb.lessThan(Task_.STEP, "m")));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(cb -> cb.like(Task_.CORRELATION_ID, "dangling\\")));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q").where(cb -> cb.and()));
    }

    @Test
    @DisplayName("evaluation over partial facts is three-valued")
    void partialEvaluation() {
        Predicate p = QueueCriteria.from("q").where(cb -> cb.and(
                cb.equal(Task_.WORKFLOW, "orders"), cb.equal(Task_.STEP, "charge"))).where();
        assertEquals(Criteria.Truth.UNKNOWN,
                Criteria.evaluate(p, (f, attribute) -> f == Field.WORKFLOW ? "orders" : Criteria.UNKNOWN));
        assertEquals(Criteria.Truth.FALSE,
                Criteria.evaluate(p, (f, attribute) -> f == Field.WORKFLOW ? "billing" : Criteria.UNKNOWN));
    }

    @Test
    @DisplayName("describe renders criteria the way an operator reads them")
    void describe() {
        Predicate p = QueueCriteria.from("q").where(cb -> cb.and(
                cb.equal(Task_.attr("region", String.class), "o'neil"),
                cb.or(cb.lessThan(Task_.ATTEMPT, 3), cb.isNull(Task_.CORRELATION_ID)),
                cb.not(cb.in(Task_.STEP, "a", "b")))).where();
        assertEquals("region = 'o''neil' and (attempt < 3 or correlation_id is null) and not (step in ('a', 'b'))",
                Criteria.describe(p));
    }
}
