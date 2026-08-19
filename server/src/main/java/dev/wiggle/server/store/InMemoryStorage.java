package dev.wiggle.server.store;

import dev.wiggle.core.NodeKind;
import dev.wiggle.server.store.Rows.Instance;
import dev.wiggle.server.store.Rows.InstanceStatus;
import dev.wiggle.server.store.Rows.ServerNode;
import dev.wiggle.server.store.Rows.Token;
import dev.wiggle.server.store.Rows.TokenStatus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Single-process store for development and tests. Uses one global lock, which is a
 * coarse but honest way to get the same serialisation guarantees the JDBC store gets
 * from row locks. Not suitable for multi-node clusters -- use {@link JdbcStorage}.
 */
public final class InMemoryStorage implements Storage {

    private final Map<String, Instance> instances = new ConcurrentHashMap<>();
    private final Map<String, Token> tokens = new ConcurrentHashMap<>();
    private final Map<String, String> definitions = new ConcurrentHashMap<>();
    private final Map<String, Integer> latest = new ConcurrentHashMap<>();
    private final Map<String, ServerNode> nodes = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    @Override public void migrate() { /* nothing to do */ }

    @Override public <R> R inTx(Function<Tx, R> work) {
        lock.lock();
        try {
            return work.apply(new MemTx());
        } finally {
            lock.unlock();
        }
    }

    @Override public void close() { }

    private final class MemTx implements Tx {

        @Override public void putDefinition(String name, int version, String json) {
            definitions.put(name + ":" + version, json);
            latest.put(name, version);
        }

        @Override public Optional<String> definition(String name, int version) {
            return Optional.ofNullable(definitions.get(name + ":" + version));
        }

        @Override public Optional<Integer> latestVersion(String name) {
            return Optional.ofNullable(latest.get(name));
        }

        @Override public List<String> definitionNames() {
            return new ArrayList<>(new TreeSet<>(latest.keySet()));
        }

        @Override public void insertInstance(Instance i) { instances.put(i.id, i.clone()); }

        @Override public Optional<Instance> lockInstance(String id) { return findInstance(id); }

        @Override public Optional<Instance> findInstance(String id) {
            Instance i = instances.get(id);
            return Optional.ofNullable(i == null ? null : i.clone());
        }

        @Override public void updateInstance(Instance i) {
            i.revision++;
            instances.put(i.id, i.clone());
        }

        @Override public List<Instance> listInstances(String workflow, InstanceStatus status, int limit) {
            return instances.values().stream()
                    .filter(i -> workflow == null || workflow.equals(i.workflow))
                    .filter(i -> status == null || status == i.status)
                    .sorted(Comparator.comparingLong((Instance i) -> i.createdAt).reversed())
                    .limit(limit)
                    .map(Instance::clone)
                    .toList();
        }

        @Override public int countInstances(InstanceStatus status) {
            return (int) instances.values().stream().filter(i -> i.status == status).count();
        }

        @Override public void insertToken(Token t) { tokens.put(t.id, t.clone()); }

        @Override public Optional<Token> findToken(String id) {
            Token t = tokens.get(id);
            return Optional.ofNullable(t == null ? null : t.clone());
        }

        @Override public List<Token> tokensOf(String instanceId) {
            return tokens.values().stream()
                    .filter(t -> t.instanceId.equals(instanceId))
                    .sorted(Comparator.comparing(t -> t.id))
                    .map(Token::clone)
                    .toList();
        }

        @Override public void updateToken(Token t) { tokens.put(t.id, t.clone()); }

        @Override public List<Token> claimTasks(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            List<Token> claimed = new ArrayList<>();
            tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.READY)
                    .filter(t -> t.kind == NodeKind.TASK || t.kind == NodeKind.PREDICATE)
                    .filter(t -> t.availableAt <= now)
                    .filter(t -> queues == null || queues.isEmpty() || queues.contains(t.queue))
                    .sorted(Comparator.comparingLong((Token t) -> t.availableAt).thenComparing(t -> t.id))
                    .limit(max)
                    .forEach(t -> {
                        Token live = tokens.get(t.id);
                        live.status = TokenStatus.RUNNING;
                        live.leaseOwner = workerId;
                        live.leaseExpiresAt = leaseUntil;
                        live.updatedAt = now;
                        claimed.add(live.clone());
                    });
            return claimed;
        }

        @Override public List<Token> dueTimers(long now, int max) {
            return tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.WAITING && t.kind == NodeKind.SLEEP && t.availableAt <= now)
                    .limit(max)
                    .map(Token::clone)
                    .toList();
        }

        @Override public List<Token> expiredLeases(long now, int max) {
            return tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.RUNNING && t.leaseExpiresAt > 0 && t.leaseExpiresAt < now)
                    .limit(max)
                    .map(Token::clone)
                    .toList();
        }

        @Override public void upsertNode(ServerNode n) {
            ServerNode existing = nodes.get(n.id);
            if (existing != null) n.firstHeartbeat = existing.firstHeartbeat;
            nodes.put(n.id, n.clone());
        }

        @Override public List<ServerNode> nodes() {
            return nodes.values().stream()
                    .sorted(Comparator.comparingLong((ServerNode n) -> n.firstHeartbeat).thenComparing(n -> n.id))
                    .map(ServerNode::clone)
                    .toList();
        }

        @Override public void deleteNodesOlderThan(long before) {
            nodes.values().removeIf(n -> n.lastHeartbeat < before);
        }

        @Override public void setLeader(String nodeId, boolean leader) {
            ServerNode n = nodes.get(nodeId);
            if (n != null) n.leader = leader;
        }

        @Override public int deleteTerminalInstancesBefore(long updatedBefore, int limit) {
            List<String> victims = instances.values().stream()
                    .filter(i -> i.status != InstanceStatus.RUNNING && i.updatedAt < updatedBefore)
                    .limit(limit)
                    .map(i -> i.id)
                    .toList();
            victims.forEach(id -> {
                instances.remove(id);
                tokens.values().removeIf(t -> t.instanceId.equals(id));
            });
            return victims.size();
        }
    }
}
