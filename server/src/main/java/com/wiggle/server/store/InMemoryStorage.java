package com.wiggle.server.store;

import com.wiggle.core.InstanceStatus;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.WorkflowVersion;
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

    /** A worker-run step's queue wait: ready to claimed. An observed step (reported, with a seq)
     *  was never queued, so it waited for nothing. */
    private static long waitOf(Token t) {
        return t.seq != null ? 0 : Math.max(0, t.startedAt - t.availableAt);
    }

    private static boolean claimable(Token t) {
        return t.status == TokenStatus.READY && (t.kind == NodeKind.TASK || t.kind == NodeKind.PREDICATE);
    }

    /**
     * Stores a token the way a database does: its payload encoded, so this store and a JDBC one
     * agree on what survives a round trip (a JSON number comes back a Long either way) and the
     * codec is exercised by every test that runs in memory.
     */
    private static Token encoded(Token t) {
        Token copy = t.clone();
        copy.payload = PayloadCodec.decode(PayloadCodec.encode(t.payload));
        return copy;
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
    /** "name:version" -> the fingerprint of the graph registered there. */
    private final Map<String, GraphStore.StoredFingerprint> fingerprints = new ConcurrentHashMap<>();
    /** Every version registered per name; the latest is the highest. */
    private final Map<String, NavigableSet<Integer>> versions = new ConcurrentHashMap<>();
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

    /** instanceId -> compensation log entries (seq-ordered append). */
    private final Map<String, List<Rows.CompLog>> compLogs = new ConcurrentHashMap<>();
    /** Insertion-ordered, so newest-first is a reverse walk. Guarded by the global lock. */
    private final List<Rows.Anomaly> anomalies = new ArrayList<>();
    /** The event log in seq order; seq is assigned on append. Guarded by the global lock. */
    private final List<Rows.Event> events = new ArrayList<>();
    private long eventSeq;
    /** consumer -> its place in the log. */
    private final Map<String, Rows.EventCursor> eventCursors = new ConcurrentHashMap<>();

    private final class MemTx implements Tx {

        /** Writes apply directly under the storage lock; a throw undoes nothing. */
        @Override public boolean transactional() { return false; }


        @Override public void putDefinition(String name, int version, String json,
                                            String fingerprint, String fingerprintAlgo) {
            if (definitions.containsKey(name + ":" + version)) return;
            write(name, version, json, fingerprint, fingerprintAlgo);
        }

        @Override public void replaceDefinition(String name, int version, String json,
                                                String fingerprint, String fingerprintAlgo) {
            write(name, version, json, fingerprint, fingerprintAlgo);
        }

        private void write(String name, int version, String json, String fingerprint, String fingerprintAlgo) {
            definitions.put(name + ":" + version, json);
            fingerprints.put(name + ":" + version, new GraphStore.StoredFingerprint(fingerprint, fingerprintAlgo));
            versions.computeIfAbsent(name, k -> new TreeSet<>()).add(version);
        }

        @Override public Optional<String> definition(String name, int version) {
            return Optional.ofNullable(definitions.get(name + ":" + version));
        }

        @Override public Optional<GraphStore.StoredFingerprint> definitionFingerprint(String name, int version) {
            if (!definitions.containsKey(name + ":" + version)) return Optional.empty();
            return Optional.of(fingerprints.getOrDefault(name + ":" + version,
                    new GraphStore.StoredFingerprint(null, null)));
        }

        @Override public void putGraph(WorkflowDefinition def) {
            String key = def.name() + ":" + def.version();
            graphNodes.put(key, Map.copyOf(def.nodes()));
            graphStart.put(key, def.startNode());
        }

        @Override public void deleteGraph(String workflow, int version) {
            String key = workflow + ":" + version;
            graphNodes.remove(key);
            graphStart.remove(key);
        }

        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            Map<String, Node> ns = graphNodes.get(workflow + ":" + version);
            return Optional.ofNullable(ns == null ? null : ns.get(nodeId));
        }

        @Override public Optional<String> graphStartNode(String workflow, int version) {
            return Optional.ofNullable(graphStart.get(workflow + ":" + version));
        }

        @Override public Optional<Integer> latestVersion(String name) {
            NavigableSet<Integer> vs = versions.get(name);
            return vs == null || vs.isEmpty() ? Optional.empty() : Optional.of(vs.last());
        }

        @Override public List<String> definitionNames() {
            return new ArrayList<>(new TreeSet<>(versions.keySet()));
        }

        @Override public void insertInstance(Instance i) { instances.put(i.id, i.clone()); }

        @Override public boolean insertInstanceIfAbsent(Instance i) {
            return instances.putIfAbsent(i.id, i.clone()) == null;
        }

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
            Token stored = encoded(t);
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
            Token stored = encoded(t);
            tokens.put(stored.id, stored);
            indexToken(stored);
        }

        @Override
        public boolean hasActiveTokens(String instanceId) {
            NavigableMap<String, Token> byId = tokensByInstance.get(instanceId);
            if (byId == null) return false;
            for (Token t : byId.values()) {
                if (t.isActive()) return true;
            }
            return false;
        }

        /**
         * Mirrors the JDBC bulk UPDATE. Goes through {@link #updateToken} rather than mutating in
         * place so both indexes stay right -- a cancelled token must leave the claimable set, or a
         * worker keeps being handed work for a dead instance. The snapshot is taken first because
         * updateToken re-indexes, which structurally modifies the very map being walked.
         */
        @Override
        public void cancelActiveTokens(String instanceId, long now) {
            NavigableMap<String, Token> byId = tokensByInstance.get(instanceId);
            if (byId == null) return;
            for (Token stored : List.copyOf(byId.values())) {
                if (!stored.isActive()) continue;
                Token next = stored.clone();
                next.status = TokenStatus.CANCELLED;
                next.leaseOwner = null;
                next.leaseExpiresAt = 0;
                next.updatedAt = now;
                updateToken(next);
            }
        }

        @Override
        public List<String> joinStacksAt(String instanceId, String nodeId) {
            NavigableMap<String, Token> byId = tokensByInstance.get(instanceId);
            if (byId == null) return List.of();
            List<String> out = new ArrayList<>();
            for (Token t : byId.values()) {
                if (t.status == TokenStatus.JOINED && nodeId.equals(t.nodeId)) out.add(t.joinStack);
            }
            return out;
        }

        @Override public List<Token> claimTasks(String workerId, Set<String> queues,
                                                Set<WorkflowVersion> versions, int max, long now,
                                                long leaseUntil) {
            List<Token> claimed = new ArrayList<>();
            Iterator<Token> it = readyTasks.iterator();
            while (it.hasNext() && claimed.size() < max) {
                Token live = it.next();
                if (live.availableAt > now) break;   // ordered by availableAt: the rest are future
                if (queues != null && !queues.isEmpty() && !queues.contains(live.queue)) continue;
                if (versions != null && !versions.isEmpty()
                        && !versions.contains(new WorkflowVersion(live.workflow, live.version))) continue;
                it.remove();                          // READY -> RUNNING leaves the claimable index
                live.status = TokenStatus.RUNNING;
                live.leaseOwner = workerId;
                live.leaseExpiresAt = leaseUntil;
                live.startedAt = now;        // the step's clock starts when a worker takes it
                live.finishedAt = null;
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

        @Override public List<Instance> dueSettle(long now, int max) {
            return instances.values().stream()
                    .filter(i -> i.status == InstanceStatus.RUNNING && i.settleAt != null && i.settleAt <= now)
                    .sorted(Comparator.comparingLong((Instance i) -> i.settleAt))
                    .limit(max)
                    .map(Instance::clone)
                    .toList();
        }

        @Override public List<Token> expiredLeases(long now, int max) {
            return tokens.values().stream()
                    .filter(t -> t.hasExpiredLeaseAt(now))
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

        @Override public List<Rows.BacklogSlice> backlogByVersion(long now, int max) {
            record Key(String workflow, int version, String queue) {}
            Map<Key, int[]> counts = new LinkedHashMap<>();      // key -> {count}
            Map<Key, Long> oldest = new LinkedHashMap<>();
            for (Token t : readyTasks) {                          // ordered by availableAt
                if (t.availableAt > now) break;
                Key k = new Key(t.workflow, t.version, t.queue);
                counts.computeIfAbsent(k, x -> new int[1])[0]++;
                oldest.putIfAbsent(k, t.availableAt);             // first seen is the oldest
            }
            return counts.entrySet().stream()
                    .sorted((a, b) -> Integer.compare(b.getValue()[0], a.getValue()[0]))
                    .limit(max)
                    .map(e -> new Rows.BacklogSlice(e.getKey().workflow(), e.getKey().version(),
                            e.getKey().queue(), e.getValue()[0], oldest.get(e.getKey())))
                    .toList();
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
                    .filter(i -> !i.status.live() && i.updatedAt < updatedBefore)
                    .limit(limit)
                    .map(i -> i.id)
                    .toList();
            victims.forEach(id -> {
                instances.remove(id);
                compLogs.remove(id);
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

        @Override public void appendCompensation(Rows.CompLog entry) {
            compLogs.computeIfAbsent(entry.instanceId, k -> new java.util.concurrent.CopyOnWriteArrayList<>())
                    .add(entry.clone());
        }

        @Override public List<Rows.CompLog> compensationLog(String instanceId) {
            List<Rows.CompLog> log = compLogs.get(instanceId);
            if (log == null) return List.of();
            List<Rows.CompLog> out = new ArrayList<>(log.size());
            for (Rows.CompLog e : log) out.add(e.clone());
            out.sort(java.util.Comparator.comparingLong(e -> e.seq));
            return out;
        }

        @Override public void insertAnomaly(Rows.Anomaly anomaly) {
            anomalies.add(anomaly);
        }

        @Override public List<Rows.Anomaly> anomalies(String workflow, String instanceId, int limit) {
            List<Rows.Anomaly> out = new ArrayList<>();
            for (int k = anomalies.size() - 1; k >= 0 && out.size() < limit; k--) {
                Rows.Anomaly a = anomalies.get(k);
                if (workflow != null && !workflow.equals(a.workflow())) continue;
                if (instanceId != null && !instanceId.equals(a.instanceId())) continue;
                out.add(a);
            }
            return out;
        }

        @Override public long appendEvent(Rows.Event e) {
            long seq = ++eventSeq;
            events.add(new Rows.Event(seq, e.instanceId(), e.workflow(), e.version(), e.correlationId(),
                    e.type(), e.nodeId(), e.payloadVer(), e.payload(), e.createdAt()));
            return seq;
        }

        @Override public List<Rows.Event> eventsAfter(long afterSeq, long createdBefore, int max) {
            List<Rows.Event> out = new ArrayList<>();
            for (Rows.Event e : events) {
                if (e.seq() > afterSeq && e.createdAt() < createdBefore) out.add(e);
                if (out.size() >= max) break;
            }
            return out;
        }

        @Override public long latestEventSeq() {
            return eventSeq;
        }

        @Override public Rows.EventCursor eventCursor(String consumer) {
            return eventCursors.get(consumer);
        }

        @Override public void createEventCursorIfAbsent(Rows.EventCursor cursor) {
            eventCursors.putIfAbsent(cursor.consumer(), cursor);
        }

        @Override public void advanceEventCursor(String consumer, long ackedSeq, long now) {
            eventCursors.compute(consumer, (k, cur) -> cur == null
                    ? new Rows.EventCursor(k, ackedSeq, now, now)
                    : new Rows.EventCursor(k, Math.max(cur.ackedSeq(), ackedSeq), now, cur.createdAt()));
        }

        @Override public Long oldestAckedSeq() {
            return eventCursors.values().stream().mapToLong(Rows.EventCursor::ackedSeq).min().stream()
                    .boxed().findFirst().orElse(null);
        }

        @Override public int deleteEvents(long createdBefore, Long upToSeq, int max) {
            int n = 0;
            Iterator<Rows.Event> it = events.iterator();
            while (it.hasNext() && n < max) {
                Rows.Event e = it.next();
                if (e.createdAt() >= createdBefore || (upToSeq != null && e.seq() > upToSeq)) break;
                it.remove();
                n++;
            }
            return n;
        }

        @Override public List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max) {
            return tokens.values().stream()
                    .filter(t -> t.status == TokenStatus.DONE && t.startedAt != null && t.finishedAt != null)
                    .filter(t -> workflow.equals(t.workflow) && t.version == version && t.finishedAt > since)
                    .sorted(Comparator.comparingLong((Token t) -> t.finishedAt).reversed())
                    .limit(max)
                    .map(t -> new Rows.StepDuration(t.nodeId, Math.max(0, t.finishedAt - t.startedAt), waitOf(t)))
                    .toList();
        }

        @Override public void markCompensated(String instanceId, long seq) {
            List<Rows.CompLog> log = compLogs.get(instanceId);
            if (log == null) return;
            for (int k = 0; k < log.size(); k++) {
                if (log.get(k).seq == seq) {
                    Rows.CompLog e = log.get(k).clone();
                    e.compensated = true;
                    log.set(k, e);
                }
            }
        }
    }
}
