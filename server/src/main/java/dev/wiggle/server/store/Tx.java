package dev.wiggle.server.store;

import dev.wiggle.server.store.Rows.Instance;
import dev.wiggle.server.store.Rows.InstanceStatus;
import dev.wiggle.server.store.Rows.ServerNode;
import dev.wiggle.server.store.Rows.Token;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface Tx {

    // -- definitions ------------------------------------------------------
    void putDefinition(String name, int version, String json);
    Optional<String> definition(String name, int version);
    /** Most recently registered version for a name. */
    Optional<Integer> latestVersion(String name);
    List<String> definitionNames();

    // -- instances --------------------------------------------------------
    void insertInstance(Instance instance);
    /** Acquires the instance write-lock for the remainder of this transaction. */
    Optional<Instance> lockInstance(String id);
    Optional<Instance> findInstance(String id);
    void updateInstance(Instance instance);
    List<Instance> listInstances(String workflow, InstanceStatus status, int limit);
    int countInstances(InstanceStatus status);

    // -- tokens -----------------------------------------------------------
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

    /** RUNNING tokens whose lease has expired (worker died or partitioned away). */
    List<Token> expiredLeases(long now, int max);

    // -- cluster ----------------------------------------------------------
    void upsertNode(ServerNode node);
    List<ServerNode> nodes();
    void deleteNodesOlderThan(long lastHeartbeatBefore);
    void setLeader(String nodeId, boolean leader);

    // -- retention --------------------------------------------------------
    int deleteTerminalInstancesBefore(long updatedBefore, int limit);
}
