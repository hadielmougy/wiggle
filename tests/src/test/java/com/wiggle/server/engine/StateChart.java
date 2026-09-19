package com.wiggle.server.engine;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The engine's two state machines rendered as documentation: an instance FSM with many concurrent
 * token FSMs beneath it.
 *
 * <p>The states and the legal edges between them are NOT written here -- they are read straight
 * off {@link TokenState} and {@link InstanceState}, which the engine itself enforces. What is
 * written here is the part no compiler can derive: the name of the event that causes each move,
 * and the guard that decides between two moves with the same event. {@link StateChartTest} holds
 * those to the enums in both directions, so a charted edge that the code forbids, or a legal edge
 * nothing documents, fails the build.
 *
 * <p>A transition whose {@code entryPoint} is null is internal: it happens inside a drive pass,
 * not because a caller asked for it.
 */
final class StateChart {

    private StateChart() {}

    /** {@code kind} is read from the state enum, never written here. */
    record State(String name, String kind, String note) {}

    record Transition(String from, String event, String to, String guard, String entryPoint) {}

    record Chart(String title, String owner, List<State> states, List<Transition> transitions) {}

    /** Prose only: the name and the classification come from {@link InstanceState}. */
    private static final Map<String, String> INSTANCE_NOTES = Map.of(
            "RUNNING", "Born here, with one token at the start node.",
            "COMPENSATING", "The saga reverse pass owns it; undo tasks are still being dispatched.",
            "COMPLETED", "A token reached a successful END and nothing was left running.",
            "FAILED", "Something unrecoverable, with nothing recorded to undo.",
            "CANCELLED", "Cancelled by a caller. Never compensates.",
            "COMPENSATED", "The reverse pass undid every recorded step.",
            "COMPENSATION_FAILED", "A compensator ran out of retries. Stuck, and deliberately loud.");

    /** Prose only: the name and the classification come from {@link TokenState}. */
    private static final Map<String, String> TOKEN_NOTES = Map.of(
            "READY", "Dispatchable. A retry waits here too, behind availableAt.",
            "RUNNING", "Leased. Implies a non-null leaseOwner and an expiry.",
            "WAITING", "Parked on the clock until availableAt.",
            "AWAITING", "Parked on an external actor: a signal, or a child instance.",
            "JOINED", "Parked at a join barrier, waiting on its siblings.",
            "DONE", "Consumed. Covers a completed step, a spent fork, and a satisfied barrier.",
            "FAILED", "Out of retries, or waiting on something that can no longer arrive.",
            "CANCELLED", "Abandoned because the instance stopped running.");

    static List<State> instanceStates() {
        return Arrays.stream(InstanceState.values())
                .map(s -> new State(s.name(), s.live() ? "live" : "terminal", INSTANCE_NOTES.get(s.name())))
                .toList();
    }

    static List<State> tokenStates() {
        return Arrays.stream(TokenState.values())
                .map(s -> new State(s.name(), s.active() ? "active" : "settled", TOKEN_NOTES.get(s.name())))
                .toList();
    }

    static Chart instances() {
        return new Chart("Instance", "InstanceLifecycle", instanceStates(),
        List.of(
                new Transition("(none)", "START", "RUNNING",
                        "the workflow version resolves", "start"),
                new Transition("(none)", "START_SUB_WORKFLOW", "RUNNING",
                        "a SUB_WORKFLOW token spawned it; parentTokenId links them", null),
                new Transition("(none)", "SCHEDULE_DUE", "RUNNING",
                        "the fire-time compare-and-set won", "fireDueSchedules"),
                new Transition("RUNNING", "TOKEN_REACHED_SUCCESSFUL_END", "COMPLETED",
                        "no token of the instance is still active", null),
                new Transition("RUNNING", "UNRECOVERABLE_FAILURE", "FAILED",
                        "the comp-log holds no uncompensated entry", null),
                new Transition("RUNNING", "UNRECOVERABLE_FAILURE", "COMPENSATING",
                        "the comp-log holds an uncompensated entry", null),
                new Transition("RUNNING", "CANCEL_REQUESTED", "CANCELLED",
                        "still RUNNING; a terminal instance ignores it", "cancel"),
                new Transition("COMPENSATING", "COMPENSATOR_COMPLETED", "COMPENSATING",
                        "another uncompensated entry remains; the next undo is dispatched", "complete"),
                new Transition("COMPENSATING", "COMPENSATOR_COMPLETED", "COMPENSATED",
                        "no uncompensated entry remains", "complete"),
                new Transition("COMPENSATING", "COMPENSATOR_EXHAUSTED", "COMPENSATION_FAILED",
                        "a compensator ran out of retries", "fail")));
    }

    static Chart tokens() {
        return new Chart("Token", "TokenLifecycle", tokenStates(),
        List.of(
                new Transition("(none)", "MINT", "READY",
                        "a continuation, a fork branch, or the start node", null),
                new Transition("(none)", "ADVANCE_CHAIN", "RUNNING",
                        "the reported run continues locally; leased straight back, never polled", "advance"),
                new Transition("READY", "DRIVE_TASK", "READY",
                        "TASK or PREDICATE node; sets the queue and wakes pollers post-commit", null),
                new Transition("READY", "DRIVE_SLEEP", "WAITING", "SLEEP node", null),
                new Transition("READY", "DRIVE_SIGNAL", "AWAITING",
                        "SIGNAL node; availableAt carries the optional deadline", null),
                new Transition("READY", "DRIVE_SUB_WORKFLOW", "AWAITING",
                        "SUB_WORKFLOW node; the child instance starts in the same transaction", null),
                new Transition("READY", "DRIVE_JOIN", "JOINED", "JOIN node, barrier not yet satisfied", null),
                new Transition("READY", "DRIVE_FORK", "DONE",
                        "FORK or DYN_FORK node; the branches are minted, this token is spent", null),
                new Transition("READY", "DRIVE_END", "DONE", "END node", null),
                new Transition("READY", "POLL_CLAIMED", "RUNNING",
                        "availableAt has passed and the instance is RUNNING (COMPENSATING for a compensator)",
                        "poll"),
                new Transition("RUNNING", "TASK_COMPLETED", "DONE",
                        "the lease matches; the continuation is minted and driven", "complete"),
                new Transition("RUNNING", "STEP_REPORTED", "DONE",
                        "one step of a locally-executed run", "advance"),
                new Transition("RUNNING", "TASK_FAILED", "READY",
                        "retryable and attempt < maxAttempts; availableAt = now + backoff", "fail"),
                new Transition("RUNNING", "TASK_FAILED", "FAILED",
                        "not retryable, or attempts exhausted", "fail"),
                new Transition("RUNNING", "LEASE_EXPIRED", "READY",
                        "same retry policy as an explicit failure; the attempt is spent", "reclaimExpiredLeases"),
                new Transition("RUNNING", "LEASE_EXPIRED", "FAILED",
                        "attempts exhausted", "reclaimExpiredLeases"),
                new Transition("WAITING", "TIMER_DUE", "DONE",
                        "availableAt has passed and the instance is RUNNING", "fireDueTimers"),
                new Transition("AWAITING", "SIGNAL_DELIVERED", "DONE",
                        "the name matches and the instance is RUNNING", "signal"),
                new Transition("AWAITING", "SIGNAL_DEADLINE", "DONE",
                        "the deadline passed; continues at altNext, or the instance fails",
                        "fireDueSignalDeadlines"),
                new Transition("AWAITING", "SUB_WORKFLOW_TERMINAL", "DONE",
                        "the child COMPLETED; its context merges back into the parent's scope", null),
                new Transition("AWAITING", "SUB_WORKFLOW_TERMINAL", "FAILED",
                        "the child ended any other way; the parent fails with it", null),
                new Transition("JOINED", "BARRIER_SATISFIED", "DONE",
                        "every expected sibling has arrived; one continuation is minted for them all", null),
                new Transition("READY", "CANCEL_PROPAGATED", "CANCELLED",
                        "the instance stopped running; applies to every active token", "cancel"),
                new Transition("RUNNING", "CANCEL_PROPAGATED", "CANCELLED",
                        "the lease is dropped; a late report is rejected as a conflict", "cancel"),
                new Transition("WAITING", "CANCEL_PROPAGATED", "CANCELLED", "", "cancel"),
                new Transition("AWAITING", "CANCEL_PROPAGATED", "CANCELLED", "", "cancel"),
                new Transition("JOINED", "CANCEL_PROPAGATED", "CANCELLED", "", "cancel")));
    }

    /** The sources of the instance FSM's UNRECOVERABLE_FAILURE event, each a call to
     *  {@code InstanceLifecycle.fail}. Checked for count, not for wording. */
    static List<String> failureSources() {
        return List.of(
                "a token exhausted its retry policy (reported, or through an expired lease)",
                "a loop guard exceeded its doWhile budget",
                "a token reached an END node marked unsuccessful",
                "a forEach found something other than a list or map at its itemsKey",
                "a SUB_WORKFLOW node named a workflow that is not registered",
                "a sub-workflow instance ended in any state but COMPLETED",
                "a signal deadline passed on a node with no altNext to escalate to");
    }

    static String render() {
        StringBuilder md = new StringBuilder();
        md.append("""
                # State machines

                <!-- Generated by tests/src/test/java/com/wiggle/server/engine/StateChart.java.
                     Do not edit by hand: StateChartTest fails when this file and the chart disagree.
                     Regenerate with ./gradlew :tests:test -Dwiggle.statechart.write=true -->

                The engine is one instance state machine with many concurrent token state machines
                beneath it. The instance owns the question "is this workflow still going"; each token
                owns one unit of execution moving over the graph.

                Each state is a constant on `InstanceState` or `TokenState`, and that constant owns
                the state's own rules: whether it is still live, whether a worker may claim it,
                whether it holds a lease, and which states it may move to. `InstanceLifecycle` and
                `TokenLifecycle` funnel every status write through `moveTo`, so a move no state
                permits throws instead of being persisted. The tables below are read off those two
                enums rather than written by hand — only the event names and guards are prose.

                The parent does not recompute itself from its children. No code scans tokens to decide
                an instance is finished; a token *arriving* at a node fires the parent transition, and
                the aggregate only appears as a guard on it.

                One transition is not policed here: `READY -> RUNNING` is performed by the store,
                because a claim has to be atomic with the `SKIP LOCKED` select that finds the token.
                """);
        for (Chart c : List.of(instances(), tokens())) {
            md.append("\n## ").append(c.title()).append(" states\n\n");
            md.append("Owned by `").append(c.owner()).append("`.\n\n");
            md.append("| State | | Meaning |\n|---|---|---|\n");
            for (State s : c.states()) {
                md.append("| `").append(s.name()).append("` | ").append(s.kind())
                        .append(" | ").append(s.note()).append(" |\n");
            }
            md.append("\n### ").append(c.title()).append(" transitions\n\n");
            md.append("| From | Event | To | Guard | Entry point |\n|---|---|---|---|---|\n");
            for (Transition t : c.transitions()) {
                md.append("| `").append(t.from()).append("` | `").append(t.event()).append("` | `")
                        .append(t.to()).append("` | ").append(t.guard().isEmpty() ? "as above" : t.guard())
                        .append(" | ").append(t.entryPoint() == null
                                ? "*internal*" : "`" + t.entryPoint() + "`").append(" |\n");
            }
        }
        md.append("\n## What raises UNRECOVERABLE_FAILURE\n\n")
                .append("The instance FSM's failure event is not only a token running out of retries.\n")
                .append("Every path below lands in `InstanceLifecycle.fail`:\n\n");
        for (String s : failureSources()) md.append("- ").append(s).append("\n");
        md.append("""

                ## How the two are coupled

                - **Downward.** A token is only claimable while its instance is `RUNNING` — or
                  `COMPENSATING`, for a compensator. `TokenLifecycle` reads the instance status as a
                  dispatch guard, but never assigns one.
                - **Upward.** A token transition that means something for the instance reports it
                  rather than applying it: `TokenLifecycle.retryOrFail` returns `Retried` or
                  `Exhausted`, and the caller decides whether that is the comp-log or the instance.
                - **Retries are internal to the token.** Only exhaustion crosses the boundary.
                - **Cancellation is structural, not a priority rule.** `cancel` takes the instance
                  write lock and settles every active token; every other transition re-reads under
                  that same lock and bails. Nothing needs to rank cancel against a lease renewal —
                  a cancelled token is no longer `RUNNING`, so the renewal fails its lease check.
                - **LOCAL_SYNC/LOCAL_ASYNC are not a third machine.** A reported run walks the same
                  transitions as `complete`; the only difference is that the continuation is leased
                  back to the same worker instead of being parked for the next poll.

                ## Invariants

                - A terminal instance has no active token. `fail`, `cancel` and the saga pass all
                  settle the instance's tokens; `COMPLETED` is guarded on there being none left.
                - `RUNNING` (token) implies a non-null `leaseOwner` and a future expiry. Settling,
                  retrying and cancelling all clear both.
                - A retry is not a distinct state. It is `READY` with `availableAt` in the future,
                  indistinguishable from a freshly minted token except in time.
                - An instance is terminal when it is neither `RUNNING` nor `COMPENSATING`. Three
                  places encode that rule — `InstanceView.isTerminal`, the JDBC purge query, and
                  the in-memory purge — and the test holds them to each other.
                - Repeated delivery is conflict-safe rather than idempotent: a second `complete` or
                  `fail` for the same token fails its lease check and is rejected as a conflict.
                """);
        return md.toString();
    }
}
