package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;


final class Tokens {

    private final DefinitionRegistry definitions;
    private final QueueWake queueWake;

    Tokens(DefinitionRegistry definitions, QueueWake queueWake) {
        this.definitions = definitions;
        this.queueWake = queueWake;
    }

    void wake(String queue) {
        queueWake.ready(queue);
    }

    void markReady(Tx tx, Token t, Node node, long now) {
        TokenState.markReady(tx, t, node, now);
        wake(node.queue());
    }

    static void cancelAll(Tx tx, String instanceId, long now) {
        for (Token t : tx.tokensOf(instanceId)) {
            if (!TokenState.of(t.status).active() || t.id == null) continue;
            TokenState.cancel(tx, t, now);
        }
    }

    static boolean anyActive(Tx tx, String instanceId) {
        return tx.tokensOf(instanceId).stream().anyMatch(t -> TokenState.of(t.status).active());
    }

    record LockedTask(Instance inst, Token token) {}

    static LockedTask lock(Tx tx, String taskId) {
        Token probe   = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        Instance inst = tx.lockInstance(probe.instanceId).orElseThrow(() -> EngineException.notFound("instance"));
        Token token   = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        return new LockedTask(inst, token);
    }

    static void requireLease(Token t, String leaseOwner) {
        TokenState.of(t.status).requireLeasedBy(t, leaseOwner);
    }

    static long extendLease(Tx tx, String taskId, String leaseOwner, long extraMillis) {
        Token t = tx.findToken(taskId).orElseThrow(() -> EngineException.notFound("task"));
        requireLease(t, leaseOwner);
        long now = System.currentTimeMillis();
        TokenState.of(t.status).renewLease(t, now + extraMillis);
        t.updatedAt = now;
        tx.updateToken(t);
        return t.leaseExpiresAt;
    }

    List<TaskActivation> claim(Tx tx, String workerId, Set<String> queues, Set<WorkflowVersion> versions,
                               int max, long now, long until) {
        List<Token> claimed = tx.claimTasks(workerId, queues, versions, max, now, until);
        List<TaskActivation> activations = new ArrayList<>(claimed.size());
        for (Token t : claimed) {
            activationFor(tx, t, workerId, until).ifPresent(activations::add);
        }
        return activations;
    }

    private Optional<TaskActivation> activationFor(Tx tx, Token t, String workerId, long until) {
        Instance inst = tx.findInstance(t.instanceId).orElse(null);
        boolean comp = Sagas.isCompensation(t);
        if (inst == null || !InstanceState.of(inst.status).dispatches(comp)) {
            return Optional.empty();
        }
        if (comp) return Optional.of(Sagas.activation(inst, t, workerId, until));
        Node node = definitions.graph(tx, t.workflow, t.version).node(t.nodeId);
        ExecutionMode mode = resolveMode(definitions.executionMode(tx, t.workflow, t.version));
        Doc base = null;
        long itemIndex = 0;
        String itemMapKey = null;
        TokenPayload payload = t.payload;
        int at = payload.innermostItem();
        if (at >= 0) {
            TokenPayload.Frame item = payload.scopes().get(at);
            itemIndex = item.idx();
            itemMapKey = item.mapKey();
            base = Scopes.baseOf(inst, payload, at);
        }
        if (Scopes.isCombineNode(node) && payload.top() != null) {
            base = payload.top().view();
        }
        return Optional.of(new TaskActivation(t.id, inst.id, inst.workflow, inst.version, node.id(), node.name(),
                node.activity(), node.kind(), t.attempt + 1, until, workerId, Scopes.dispatchContext(inst, t).raw(),
                base == null ? null : base.raw(), itemIndex, itemMapKey, mode));
    }

    private static ExecutionMode resolveMode(ExecutionMode mode) {
        return mode == null || mode == ExecutionMode.DEFAULT ? ExecutionMode.SERVER : mode;
    }

}
