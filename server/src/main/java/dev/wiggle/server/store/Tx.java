package dev.wiggle.server.store;

import dev.wiggle.core.Node;
import dev.wiggle.core.WorkflowDefinition;
import dev.wiggle.server.store.Rows.Instance;
import dev.wiggle.server.store.Rows.InstanceStatus;
import dev.wiggle.server.store.Rows.ServerNode;
import dev.wiggle.server.store.Rows.Token;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Tx {

    void putDefinition(String name, int version, String json);
    Optional<String> definition(String name, int version);
    /** Most recently registered version for a name. */
    Optional<Integer> latestVersion(String name);
    List<String> definitionNames();

    /**
     * Normalises a definition's graph into per-node and per-edge rows so the runtime can
     * fetch a single node's neighbourhood without materialising the whole graph. Idempotent:
     * the version is a content hash, so re-registering the same graph is a no-op.
     */
    void putGraph(WorkflowDefinition def);
    /** One node plus its outgoing edges, reconstructed from the normalised rows. */
    Optional<Node> graphNode(String workflow, int version, String nodeId);
    /** The graph's entry node, without loading any other node. */
    Optional<String> graphStartNode(String workflow, int version);

    void insertInstance(Instance instance);
    /** Acquires the instance write-lock for the remainder of this transaction. */
    Optional<Instance> lockInstance(String id);
    Optional<Instance> findInstance(String id);
    void updateInstance(Instance instance);
    List<Instance> listInstances(String workflow, InstanceStatus status, int limit);
    int countInstances(InstanceStatus status);

    void insertToken(Token token);
    Optional<Token> findToken(String id);
    List<Token> tokensOf(String instanceId);
    void updateToken(Token token);

    /**
     * Atomically leases up to {@code max} dispatchable tokens. Implementations must
     * guarantee a token is handed to exactly one worker.
     */
    List<Token> claimTasks(String workerId, Set<String> queues, int max, long now, long leaseUntil);

    /** WAITING timer tokens whose fire time has passed. */
    List<Token> dueTimers(long now, int max);

    /** AWAITING user-task tokens, oldest first -- what the task list shows. */
    List<Token> pendingUserTasks(int max);

    /** AWAITING user-task tokens with a deadline (availableAt > 0) that has passed. */
    List<Token> dueUserTasks(long now, int max);

    /** RUNNING tokens whose lease has expired (worker died or partitioned away). */
    List<Token> expiredLeases(long now, int max);

    /** Snapshot of the dispatchable backlog, for lag monitoring. */
    Rows.QueueDepth queueDepth(long now);

    /**
     * Worker-dispatched tokens (TASK/PREDICATE) that finished (DONE) since {@code since} --
     * the throughput signal for lag monitoring. DB-driven rather than an in-process counter,
     * so it reflects consumption across every node in the cluster, not just this one.
     */
    int countProcessedSince(long since);

    void upsertNode(ServerNode node);
    List<ServerNode> nodes();
    void deleteNodesOlderThan(long lastHeartbeatBefore);
    void setLeader(String nodeId, boolean leader);

    int deleteTerminalInstancesBefore(long updatedBefore, int limit);
}
