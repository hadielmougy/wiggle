package com.wiggle.cli;

import com.wiggle.cli.WorkflowYaml.WorkflowYamlException;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Offline tests for the YAML loader: parse a spec and assert on the compiled graph + validations. */
class WorkflowYamlTest {

    private static Blueprint<Map<String, Object>> parse(String yaml) {
        return WorkflowYaml.parse(yaml);
    }

    private static Node node(Blueprint<Map<String, Object>> bp, String name) {
        return bp.definition().nodes().values().stream()
                .filter(n -> name.equals(n.name())).findFirst()
                .orElseThrow(() -> new AssertionError("no node named '" + name + "'"));
    }

    private static long count(Blueprint<Map<String, Object>> bp, NodeKind kind) {
        return bp.definition().nodes().values().stream().filter(n -> n.kind() == kind).count();
    }

    // ---------------------------------------------------------------- linear

    @Test
    @DisplayName("linear steps compile to TASK/PREDICATE nodes with activity ids and per-step queue")
    void linear() {
        var bp = parse("""
                workflow: order
                steps:
                  - task: validate
                  - gate: in-stock
                  - task: charge
                    queue: payments
                  - effect: notify
                """);
        assertEquals("order", bp.name());
        assertEquals(NodeKind.TASK, node(bp, "validate").kind());
        assertEquals("order#validate", node(bp, "validate").activity());
        assertEquals(NodeKind.PREDICATE, node(bp, "in-stock").kind());
        assertEquals("payments", node(bp, "charge").queue());
        assertEquals("order", node(bp, "validate").queue(), "default queue is the workflow name");
        assertEquals(NodeKind.TASK, node(bp, "notify").kind(), "effect is a TASK node");
    }

    @Test
    @DisplayName("retry presets and the map form both parse")
    void retry() {
        var bp = parse("""
                workflow: wf
                steps:
                  - task: a
                    retry: forever
                  - task: b
                    retry: { max: 5, backoff: 100ms, multiplier: 2, maxBackoff: 5m, jitter: 0.2 }
                """);
        assertEquals(Integer.MAX_VALUE, node(bp, "a").retry().maxAttempts());
        assertEquals(5, node(bp, "b").retry().maxAttempts());
        assertEquals(100, node(bp, "b").retry().initialBackoffMillis());
    }

    @Test
    @DisplayName("sleep accepts a bare duration or a named map")
    void sleep() {
        var bp = parse("""
                workflow: wf
                steps:
                  - sleep: 250ms
                  - sleep: { name: cool-off, for: 2s }
                """);
        assertEquals(2, count(bp, NodeKind.SLEEP));
        assertEquals(2000, node(bp, "cool-off").sleepMillis());
    }

    // ---------------------------------------------------------------- fork / join

    @Test
    @DisplayName("fork compiles to FORK + a JOIN expecting all branches, then continues")
    void forkJoin() {
        var bp = parse("""
                workflow: order
                steps:
                  - fork:
                      payment:
                        - task: charge
                      shipping:
                        - task: reserve
                        - task: label
                  - effect: notify
                """);
        assertEquals(1, count(bp, NodeKind.FORK));
        assertEquals(1, count(bp, NodeKind.JOIN));
        Node fork = bp.definition().nodes().values().stream()
                .filter(n -> n.kind() == NodeKind.FORK).findFirst().orElseThrow();
        assertEquals(2, fork.branches().size(), "two branch starts");
        Node join = bp.definition().nodes().values().stream()
                .filter(n -> n.kind() == NodeKind.JOIN).findFirst().orElseThrow();
        assertEquals(2, join.expected(), "join waits for both branches");
        assertEquals(node(bp, "notify").id(), join.next(), "flow continues after the join");
    }

    @Test
    @DisplayName("fork_each compiles to DYN_FORK over the list with a dynamic join")
    void forkEach() {
        var bp = parse("""
                workflow: invoice
                steps:
                  - fork_each:
                      over: lineItems
                      as: item
                      body:
                        - task: price-item
                  - task: total
                """);
        Node dyn = bp.definition().nodes().values().stream()
                .filter(n -> n.kind() == NodeKind.DYN_FORK).findFirst().orElseThrow();
        assertEquals("lineItems", dyn.itemsKey());
        assertEquals("item", dyn.itemKey());
        assertEquals(1, count(bp, NodeKind.JOIN));
    }

    // ---------------------------------------------------------------- loop

    @Test
    @DisplayName("do_while compiles to a cycle: condition.true loops back to the body start")
    void doWhile() {
        var bp = parse("""
                workflow: wf
                steps:
                  - do_while:
                      body:
                        - task: fetch-page
                        - task: process-page
                      while: has-more
                  - task: finalize
                """);
        Node cond = node(bp, "has-more");
        assertEquals(NodeKind.PREDICATE, cond.kind());
        assertEquals(node(bp, "fetch-page").id(), cond.next(), "true edge loops back to the body start");
        assertEquals(node(bp, "finalize").id(), cond.altNext(), "false edge continues");
    }

    // ---------------------------------------------------------------- signals / choose

    @Test
    @DisplayName("await_signal with escalation wires next=delivery, altNext=escalation branch")
    void awaitSignalEscalation() {
        var bp = parse("""
                workflow: wf
                steps:
                  - task: submit
                  - await_signal: approval
                    timeout: 24h
                    escalation:
                      - task: auto-approve
                  - task: finish
                """);
        Node sig = node(bp, "approval");
        assertEquals(NodeKind.SIGNAL, sig.kind());
        assertEquals(node(bp, "finish").id(), sig.next());
        assertEquals(node(bp, "auto-approve").id(), sig.altNext());
    }

    @Test
    @DisplayName("choose compiles to a predicate cascade")
    void choose() {
        var bp = parse("""
                workflow: wf
                steps:
                  - choose:
                      - when: is-vip
                        then:
                          - effect: concierge
                      - otherwise:
                        then:
                          - effect: thanks
                """);
        assertEquals(NodeKind.PREDICATE, node(bp, "is-vip").kind());
        assertNotNull(node(bp, "concierge"));
        assertNotNull(node(bp, "thanks"));
    }

    // ---------------------------------------------------------------- versioning

    @Test
    @DisplayName("an explicit version is honoured; otherwise a content hash is used")
    void version() {
        assertEquals(7, parse("workflow: wf\nversion: 7\nsteps:\n  - task: a\n").version());
        assertTrue(parse("workflow: wf\nsteps:\n  - task: a\n").version() > 0);
    }

    // ---------------------------------------------------------------- validation

    @Test
    void rejectsMissingWorkflowName() {
        var e = assertThrows(WorkflowYamlException.class, () -> parse("steps:\n  - task: a\n"));
        assertTrue(e.getMessage().contains("workflow"), e.getMessage());
    }

    @Test
    void rejectsEmptySteps() {
        assertThrows(WorkflowYamlException.class, () -> parse("workflow: wf\nsteps: []\n"));
    }

    @Test
    void rejectsUnknownOperator() {
        var e = assertThrows(WorkflowYamlException.class,
                () -> parse("workflow: wf\nsteps:\n  - frobnicate: a\n"));
        assertTrue(e.getMessage().contains("no operator"), e.getMessage());
    }

    @Test
    void rejectsMultipleOperatorsInOneStep() {
        var e = assertThrows(WorkflowYamlException.class,
                () -> parse("workflow: wf\nsteps:\n  - task: a\n    gate: b\n"));
        assertTrue(e.getMessage().contains("multiple operators"), e.getMessage());
    }

    @Test
    void rejectsForkWithOneBranch() {
        var e = assertThrows(Exception.class, () -> parse("""
                workflow: wf
                steps:
                  - fork:
                      only:
                        - task: a
                """));
        assertTrue(e.getMessage().toLowerCase().contains("fork"), e.getMessage());
    }

    @Test
    void rejectsEscalationWithoutTimeout() {
        var e = assertThrows(WorkflowYamlException.class, () -> parse("""
                workflow: wf
                steps:
                  - await_signal: approval
                    escalation:
                      - task: auto
                """));
        assertTrue(e.getMessage().contains("timeout"), e.getMessage());
    }

    @Test
    void rejectsEmptyDoWhileBody() {
        assertThrows(WorkflowYamlException.class, () -> parse("""
                workflow: wf
                steps:
                  - do_while:
                      body: []
                      while: cond
                """));
    }

    @Test
    void rejectsBadDuration() {
        var e = assertThrows(WorkflowYamlException.class,
                () -> parse("workflow: wf\nsteps:\n  - sleep: 5 seconds\n"));
        assertTrue(e.getMessage().contains("duration"), e.getMessage());
    }

    @Test
    void rejectsDuplicateStepNames() {
        // enforced by the underlying DSL builder (reserved names)
        assertThrows(Exception.class,
                () -> parse("workflow: wf\nsteps:\n  - task: a\n  - task: a\n"));
    }
}