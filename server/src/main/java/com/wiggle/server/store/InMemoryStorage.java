package com.wiggle.server.store;

import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.Rows.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Single-process store for development and tests. Uses one global lock, which is a
 * coarse but honest way to get the same serialisation guarantees the JDBC store gets
 * from row locks. Not suitable for multi-node clusters -- use a JDBC-backed store.
 */
public final class InMemoryStorage implements Storage {

    private final Map<String, Instance> instances = new ConcurrentHashMap<>();
    private final Map<String, Token> tokens = new ConcurrentHashMap<>();
    /** Per-instance token index (id-ordered), so tokensOf/childInstanceIds/purge never scan the world. */
    private final Map<String, NavigableMap<String, Token>> tokensByInstance = new ConcurrentHashMap<>();
    /** Claimable tokens (READY task/predicate) ordered by (availableAt, id): claimTasks walks the
     *  head instead of sorting every live token on every poll — the sort-per-poll this replaces was
     *  ~90% of engine CPU in the embedded throughput benchmark. Guarded by the global lock. */
    private final NavigableSet<Token> readyTasks = new TreeSet<>(
            Comparator.comparingLong((Token t) -> t.availableAt).thenComparing(t -> t.id));

    private static boolean claimable(Token t) {
        return t.status == TokenStatus.READY && (t.kind == NodeKind.TASK || t.kind == NodeKind.PREDICATE);
    }

    /** Registers the STORED copy in both indexes. Call with the object that lives in {@link #tokens}. */
    private void indexToken(Token stored) {
        tokensByInstance.computeIfAbsent(stored.instanceId, k -> new TreeMap<>()).put(stored.id, stored);
        if (claimable(stored)) readyTasks.add(stored);
    }

    /** Removes the stored copy from both indexes; must run BEFORE its ordering fields change. */
    private void unindexToken(Token stored) {
        NavigableMap<String, Token> byId = tokensByInstance.get(stored.instanceId);
        if (byId != null) {
            byId.remove(stored.id);
            if (byId.isEmpty()) tokensByInstance.remove(stored.instanceId);
        }
        readyTasks.remove(stored);
    }
    private final Map<String, String> definitions = new ConcurrentHashMap<>();
    private final Map<String, Integer> latest = new ConcurrentHashMap<>();
    // Normalised graph rows, keyed by "name:version": one node id -> node, plus the entry node.
    private final Map<String, Map<String, Node>> graphNodes = new ConcurrentHashMap<>();
    private final Map<String, String> graphStart = new ConcurrentHashMap<>();
    private final Map<String, ServerNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, Rows.Schedule> schedules = new ConcurrentHashMap<>();
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

        @Override public void putGraph(WorkflowDefinition def) {
            String key = def.name() + ":" + def.version();
            graphNodes.put(key, Map.copyOf(def.nodes()));
            graphStart.put(key, def.startNode());
        }

        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            Map<String, Node> ns = graphNodes.get(workflow + ":" + version);
            return Optional.ofNullable(ns == null ? null : ns.get(nodeId));
        }

        @Override public Optional<String> graphStartNode(String workflow, int version) {
            return Optional.ofNullable(graphStart.get(workflow + ":" + version));
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

        @Override public List<Instance> findByCorrelation(String correlationId, int limit) {
            return instances.values().stream()
                    .filter(i -> correlationId.equals(i.correlationId))
                    .sorted(Comparator.comparingLong((Instance i) -> i.createdAt).reversed())
                    .limit(limit)
                    .map(Instance::clone)
                    .toList();
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

        @Override public void insertToken(Token t) {
            Token stored = t.clone();
            tokens.put(stored.id, stored);
            indexToken(stored);
        }

        @Override public Optional<Token> findToken(String id) {
            Token t = tokens.get(id);
            return Optional.ofNullable(t == null ? null : t.clone());
        }

        @Override public List<Token> tokensOf(String instanceId) {
            NavigableMap<String, Token> byId = tokensByInstance.get(instanceId);
            if (byId == null) return List.of();
            List<Token> out = new ArrayList<>(byId.size());
            for (Token t : byId.values()) out.add(t.clone());   // TreeMap: already id-ordered
            return out;
        }

        @Override public void updateToken(Token t) {
            Token old = tokens.get(t.id);
            if (old != null) unindexToken(old);
            Token stored = t.clone();
            tokens.put(stored.id, stored);
            indexToken(stored);
        }

        @Override public List<Token> claimTasks(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            List<Token> claimed = new ArrayList<>();
            Iterator<Token> it = readyTasks.iterator();
            while (it.hasNext() && claimed.size() < max) {
                Token live = it.next();
                if (live.availableAt > now) break;   // ordered by availableAt: the rest are future
                if (queues != null && !queues.isEmpty() && !queues.contains(live.queue)) continue;
                it.remove();                          // READY -> RUNNING leaves the claimable index
                live.status = TokenStatus.RUNNING;
                live.leaseOwner = workerId;
                live.leaseExpiresAt = leaseUntil;
                live.updatedAt = now;
                claimed.add(live.clone());
            }
            return claimed;
        }

        @Override public List<Token> dueTimers(long now, int max) {
            return tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.WAITING && t.kind == NodeKind.SLEEP && t.availableAt <= now)
                    .limit(max)
                    .map(Token::clone)
                    .toList();
        }

        @Override public List<Token> pendingSignals(int max) {
            return tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.AWAITING && t.kind == NodeKind.SIGNAL)
                    .sorted(Comparator.comparingLong((Token t) -> t.createdAt).thenComparing(t -> t.id))
                    .limit(max)
                    .map(Token::clone)
                    .toList();
        }

        @Override public List<Token> dueSignals(long now, int max) {
            return tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.AWAITING && t.kind == NodeKind.SIGNAL
                            && t.availableAt > 0 && t.availableAt <= now)
                    .sorted(Comparator.comparingLong((Token t) -> t.availableAt))
                    .limit(max)
                    .map(Token::clone)
                    .toList();
        }

        @Override public List<String> childInstanceIds(String parentInstanceId) {
            NavigableMap<String, Token> byId = tokensByInstance.get(parentInstanceId);
            Set<String> parentTokens = byId == null ? Set.of() : byId.keySet();
            return instances.values().stream()
                    .filter(i -> i.parentTokenId != null && parentTokens.contains(i.parentTokenId))
                    .map(i -> i.id)
                    .sorted()
                    .toList();
        }

        @Override public void putSchedule(Rows.Schedule schedule) {
            schedules.put(schedule.id, schedule.clone());
        }

        @Override public void deleteSchedule(String id) {
            schedules.remove(id);
        }

        @Override public List<Rows.Schedule> schedules() {
            return schedules.values().stream()
                    .sorted(Comparator.comparing(sch -> sch.id))
                    .map(Rows.Schedule::clone)
                    .toList();
        }

        @Override public java.util.Optional<Rows.Schedule> scheduleByWorkflow(String workflow) {
            return schedules.values().stream()
                    .filter(sch -> sch.workflow.equals(workflow))
                    .findFirst()
                    .map(Rows.Schedule::clone);
        }

        @Override public List<Rows.Schedule> dueSchedules(long now, int max) {
            return schedules.values().stream()
                    .filter(sch -> sch.nextFireAt <= now)
                    .sorted(Comparator.comparingLong(sch -> sch.nextFireAt))
                    .limit(max)
                    .map(Rows.Schedule::clone)
                    .toList();
        }

        @Override public boolean claimSchedule(String id, long expectedFireAt, long nextFireAt) {
            Rows.Schedule live = schedules.get(id);
            if (live == null || live.nextFireAt != expectedFireAt) return false;
            live.nextFireAt = nextFireAt;
            return true;
        }

        @Override public List<Token> expiredLeases(long now, int max) {
            return tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.RUNNING && t.leaseExpiresAt > 0 && t.leaseExpiresAt < now)
                    .limit(max)
                    .map(Token::clone)
                    .toList();
        }

        @Override public Rows.QueueDepth queueDepth(long now) {
            int count = 0;
            long oldest = 0;
            for (Token t : readyTasks) {              // ordered by availableAt
                if (t.availableAt > now) break;
                if (count == 0) oldest = t.availableAt;
                count++;
            }
            return new Rows.QueueDepth(count, oldest);
        }

        @Override public int countProcessedSince(long since) {
            return (int) tokens.values().stream()
                    .filter(t -> t.kind == NodeKind.TASK || t.kind == NodeKind.PREDICATE)
                    .filter(t -> t.status == TokenStatus.DONE)
                    .filter(t -> t.updatedAt > since)
                    .count();
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
                NavigableMap<String, Token> byId = tokensByInstance.remove(id);
                if (byId != null) {
                    for (Token t : byId.values()) {
                        tokens.remove(t.id);
                        readyTasks.remove(t);
                    }
                }
            });
            return victims.size();
        }
    }
}
