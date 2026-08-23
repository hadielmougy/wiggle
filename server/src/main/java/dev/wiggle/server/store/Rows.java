package dev.wiggle.server.store;

import dev.wiggle.core.NodeKind;

/** Mutable storage rows. Deliberately dumb structs -- all invariants live in the engine. */
public final class Rows {
    private Rows() {}

    public enum InstanceStatus { RUNNING, COMPLETED, FAILED, CANCELLED }

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
        public String contextJson = "{}";
        /** When this instance is a sub-workflow: the parent's waiting token; null otherwise. */
        public String parentTokenId;
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
         * Branch-scoped JSON overlaid on the instance context when this token is dispatched --
         * how a dynamic-fork child carries its item. Inherited along the branch, restored from
         * the fork token after a join; null outside dynamic branches.
         */
        public String payloadJson;
        public String lastError;
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

    /** A server node in the cluster -- the JobRunr "background job server" analogue. */
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

    /** A recurring start: fires the workflow every {@code intervalMillis}, leader-driven. */
    public static final class Schedule implements Cloneable {
        public String id;
        public String workflow;
        public long intervalMillis;
        public String contextJson = "{}";
        public long nextFireAt;
        public long createdAt;

        @Override public Schedule clone() {
            try { return (Schedule) super.clone(); } catch (CloneNotSupportedException e) { throw new AssertionError(e); }
        }
    }

    /**
     * A snapshot of the dispatchable backlog: how many worker-dispatched tokens (TASK/PREDICATE)
     * are READY and due right now, and the {@code availableAt} of the oldest of them (0 if none).
     * Read-only, so unlike the row classes above this is a plain record.
     */
    public record QueueDepth(int readyCount, long oldestAvailableAt) { }
}
