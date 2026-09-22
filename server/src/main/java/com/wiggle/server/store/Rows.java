package com.wiggle.server.store;

import com.wiggle.core.Doc;
import com.wiggle.core.NodeKind;

/** Mutable storage rows. Deliberately dumb structs -- all invariants live in the engine. */
public final class Rows {
    private Rows() {}

    public enum InstanceStatus { RUNNING, COMPLETED, FAILED, CANCELLED,
        COMPENSATING, COMPENSATED, COMPENSATION_FAILED }

    public enum TokenStatus {
        /** Dispatchable to a worker. */           READY,
        /** Leased by a worker. */                 RUNNING,
        /** Sleeping until availableAt. */         WAITING,
        /** Awaiting an external/user completion. */ AWAITING,
        /** Parked at a join barrier. */           JOINED,
        /** Consumed. */                           DONE,
        /** Terminally failed. */                  FAILED,
        /** Abandoned because a sibling failed. */ CANCELLED
    }

    public static final class Instance implements Cloneable {
        public String id;
        public String workflow;
        public int version;
        public String correlationId;
        public InstanceStatus status = InstanceStatus.RUNNING;
        public String terminationReason;
        public String error;
        public Doc context = Doc.EMPTY;
        /** When this instance is a sub-workflow: the parent's waiting token; null otherwise. */
        public String parentTokenId;
        /** Observed runs only: when the run is due to be judged. Every report pushes it out by the
         *  stall threshold; reaching END pulls it in to a short grace. Null on every other instance. */
        public Long settleAt;
        public long createdAt;
        public long updatedAt;
        public long revision;

        @Override public Instance clone() {
            try { return (Instance) super.clone(); } catch (CloneNotSupportedException e) { throw new AssertionError(e); }
        }
    }

    public static final class Token implements Cloneable {
        public String id;
        public String instanceId;
        public String workflow;
        public int version;
        public String nodeId;
        public NodeKind kind;
        public TokenStatus status = TokenStatus.READY;
        public String activity;
        public String queue;
        public int attempt;
        /** Earliest time this token may be dispatched (or the timer fire time). */
        public long availableAt;
        public String leaseOwner;
        public long leaseExpiresAt;
        /** Comma separated stack of enclosing fork groups; last element is innermost. */
        public String joinStack = "";
        /**
         * The engine's bookkeeping for this token: the nesting stack (each frame carrying that
         * branch's private view of the context), loop counts, and any inputs staged for a combine.
         * Never null -- {@link TokenPayload#EMPTY} outside every scope. Stores encode it through
         * {@link PayloadCodec}; nothing above the store sees its serialised form.
         */
        public TokenPayload payload = TokenPayload.EMPTY;
        /** Set on a compensation token: the comp-log entry its undo settles. Null on forward work,
         *  which is what tells the two apart. */
        public Long compSeq;
        public String lastError;
        /** When the step ran where it ran, as its reporter measured it: a locally-chained or
         *  observed step carries its own clock, since the server only sees the flush. Null when
         *  the step was not timed. */
        public Long startedAt;
        public Long finishedAt;
        /** Observed steps only: the order they were reported in, which breaks ties between steps
         *  whose clocks agree to the millisecond. Null elsewhere. */
        public Long seq;
        public long createdAt;
        public long updatedAt;

        public String currentJoinGroup() {
            if (joinStack == null || joinStack.isEmpty()) return null;
            int i = joinStack.lastIndexOf(',');
            return i < 0 ? joinStack : joinStack.substring(i + 1);
        }

        public String popJoinStack() {
            if (joinStack == null || joinStack.isEmpty()) return "";
            int i = joinStack.lastIndexOf(',');
            return i < 0 ? "" : joinStack.substring(0, i);
        }

        public String pushJoinStack(String group) {
            return (joinStack == null || joinStack.isEmpty()) ? group : joinStack + "," + group;
        }

        public boolean isActive() {
            return status == TokenStatus.READY || status == TokenStatus.RUNNING
                    || status == TokenStatus.WAITING || status == TokenStatus.AWAITING
                    || status == TokenStatus.JOINED;
        }

        @Override public Token clone() {
            try { return (Token) super.clone(); } catch (CloneNotSupportedException e) { throw new AssertionError(e); }
        }
    }

    public static final class ServerNode implements Cloneable {
        public String id;
        public String name;
        public long firstHeartbeat;
        public long lastHeartbeat;
        public int workers;
        public boolean leader;

        @Override public ServerNode clone() {
            try { return (ServerNode) super.clone(); } catch (CloneNotSupportedException e) { throw new AssertionError(e); }
        }
    }

    /**
     * A recurring start, leader-driven. Cadence is either a fixed interval
     * ({@code intervalMillis > 0}) or a cron expression ({@code cron != null}, evaluated in UTC).
     */
    public static final class Schedule implements Cloneable {
        public String id;
        public String workflow;
        public long intervalMillis;
        public String cron;
        public Doc context = Doc.EMPTY;
        public long nextFireAt;
        public long createdAt;

        @Override public Schedule clone() {
            try { return (Schedule) super.clone(); } catch (CloneNotSupportedException e) { throw new AssertionError(e); }
        }
    }

    /** One compensable step's completion record: the reverse pass runs these newest-first.
     *  {@code input}/{@code result} are the step's snapshots (as received / as left), captured
     *  atomically with the completion — see docs/saga-compensation.md §4. */
    public static class CompLog implements Cloneable {
        public String instanceId;
        public long seq;
        public String nodeId;
        public String activity;
        public String queue;
        public Doc input;
        public Doc result;
        public boolean compensated;

        @Override public CompLog clone() {
            try { return (CompLog) super.clone(); } catch (CloneNotSupportedException e) { throw new AssertionError(e); }
        }
    }

    /**
     * A snapshot of the dispatchable backlog: how many worker-dispatched tokens (TASK/PREDICATE)
     * are READY and due right now, and the {@code availableAt} of the oldest of them (0 if none).
     * Read-only, so unlike the row classes above this is a plain record.
     */
    public record QueueDepth(int readyCount, long oldestAvailableAt) { }

    /**
     * One slice of the dispatchable backlog, grouped by what decides who may claim it: the queue a
     * token sits on, and the (workflow, version) a worker must serve to be allowed it. The console
     * uses this to show work that no running worker can pick up -- a queue nobody polls, or a version
     * every worker has scoped itself out of.
     */
    public record BacklogSlice(String workflow, int version, String queue,
                               int readyCount, long oldestAvailableAt) { }

    /**
     * One departure of an observed run from its topology, written once and never updated.
     * {@code kind} is one of the names {@link com.wiggle.core.AnomalyView} lists.
     */
    public record Anomaly(String id, String instanceId, String workflow, int version, String kind,
                          String expectedNode, String reportedNode, String detail, long at) { }

    /** One settled, timed step: what the duration statistics are computed from. */
    public record StepDuration(String nodeId, long millis) { }
}
