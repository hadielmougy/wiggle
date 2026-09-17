package com.wiggle.server.engine;

import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * The single-writer engine core: resident instances are authoritative in memory, every mutation is
 * journalled, and a flush loop mirrors the coalesced final state to the database in one fenced
 * transaction per cycle. A caller's ack completes only after the flush carrying its writes commits,
 * so an acknowledged operation is exactly as durable as on the per-operation path; a crash loses
 * only un-acked work, which lease expiry re-runs -- the same story a worker crash already has.
 *
 * <p>Engine bodies run unchanged: {@link JTx} implements {@link Tx} over resident memory for
 * instance/token/comp-log state, delegates immutable reads (graphs, definitions) and control-plane
 * rows (schedules) to short real transactions, and refuses cross-instance scans -- those stay on
 * the database as hints that every operation revalidates against memory.
 *
 * <p>The fence: a single-row lease ({@code wf_journal_lease}) claimed at construction with a fresh
 * generation, renewed by every flush under {@code WHERE owner=? AND generation=?}. Zero rows means
 * ownership moved while this node was paused: the flush aborts, memory is dropped, and every
 * subsequent operation is refused -- a zombie can stall, never corrupt.
 */
final class Journal implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Journal.class.getName());

    /** One row's pending mirror write: the latest clone, and whether it still needs INSERT. */
    private record Snap(Object row, boolean insert) {}

    private static final class Cycle {
        final Map<String, Snap> instances = new LinkedHashMap<>();
        final Map<String, Snap> tokens = new LinkedHashMap<>();
        final List<Rows.CompLog> compAppends = new ArrayList<>();
        final List<Object[]> compMarks = new ArrayList<>();          // {instanceId, seq}
        final Set<String> wakeQueues = new HashSet<>();
        final CompletableFuture<Void> flushed = new CompletableFuture<>();
        boolean isEmpty() {
            return instances.isEmpty() && tokens.isEmpty() && compAppends.isEmpty() && compMarks.isEmpty();
        }
    }

    private static final class Resident {
        final ReentrantLock lock = new ReentrantLock();
        Instance inst;
        final Map<String, Token> tokens = new LinkedHashMap<>();
        List<Rows.CompLog> compLog;               // null until first needed
        long touched;
    }

    private final Storage storage;
    private final DispatchNotifier notifier;
    private final ThreadLocal<Set<String>> readyQueues;
    private final String owner;
    private final long lingerNanos;
    private final int maxOps;
    private final long leaseMillis;
    private final int residentMax;

    private final ConcurrentHashMap<String, Resident> residents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> tokenOwner = new ConcurrentHashMap<>();

    /** Ops hold the read side for their whole body so a flush can never split one op's writes. */
    private final ReentrantReadWriteLock cycleGate = new ReentrantReadWriteLock();
    private Cycle current = new Cycle();
    private int opsInCycle;
    private final Object flushSignal = new Object();

    private volatile boolean active;
    private volatile boolean closing;
    private final long generation;
    private final Thread flusher;

    Journal(Storage storage, DispatchNotifier notifier, ThreadLocal<Set<String>> readyQueues,
            String owner, long lingerMillis, int maxOps, long leaseMillis, int residentMax) {
        this.storage = storage;
        this.notifier = notifier;
        this.readyQueues = readyQueues;
        this.owner = owner;
        this.lingerNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, lingerMillis));
        this.maxOps = Math.max(1, maxOps);
        this.leaseMillis = Math.max(1_000, leaseMillis);
        this.residentMax = Math.max(16, residentMax);
        this.generation = storage.inTx(tx -> tx.claimJournalLease(owner, this.leaseMillis));
        this.active = generation > 0;
        if (!active) {
            LOG.log(System.Logger.Level.ERROR, "journal lease is held elsewhere; this node will "
                    + "refuse the data plane (run exactly one journal-mode node per database)");
        }
        this.flusher = new Thread(this::loop, "wiggle-journal");
        this.flusher.setDaemon(true);
        this.flusher.start();
    }

    boolean active() { return active; }

    long generation() { return generation; }

    /** Runs {@code body} against resident memory and returns after the flush carrying its writes
     *  commits. A body that wrote nothing returns immediately. */
    <T> T run(Function<Tx, T> body) {
        if (!active) throw EngineException.conflict("journal inactive (lease lost or node closing); "
                + "this node no longer serves the data plane");
        JTx jtx = new JTx();
        T result;
        CompletableFuture<Void> ack = null;
        Set<String> outerQueues = readyQueues.get();
        Set<String> myQueues = new HashSet<>();
        readyQueues.set(myQueues);
        cycleGate.readLock().lock();
        try {
            try {
                result = body.apply(jtx);
            } finally {
                readyQueues.set(outerQueues);
            }
            if (jtx.dirty) {
                Cycle c = current;                     // safe: swap needs the write lock
                c.wakeQueues.addAll(myQueues);
                ack = c.flushed;
                opsInCycle++;
                synchronized (flushSignal) { flushSignal.notifyAll(); }
            }
        } finally {
            jtx.unlockAll();
            cycleGate.readLock().unlock();
        }
        if (ack != null) await(ack);
        else if (!myQueues.isEmpty()) notifier.signal(myQueues);   // read-only body, stray wakes
        return result;
    }

    private static void await(CompletableFuture<Void> ack) {
        try {
            ack.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw e;
        }
    }

    private void loop() {
        while (!closing || pendingWork()) {
            try {
                synchronized (flushSignal) {
                    if (!closing && opsInCycle == 0) flushSignal.wait(Math.max(1, leaseMillis / 3));
                }
                if (!closing && opsInCycle > 0 && lingerNanos > 0 && opsInCycle < maxOps) {
                    // Sit out the whole linger so concurrent ops coalesce; wake early only for
                    // close() or a full cycle -- not for every arriving op.
                    long lingerDeadline = System.nanoTime() + lingerNanos;
                    synchronized (flushSignal) {
                        while (!closing && opsInCycle < maxOps) {
                            long remaining = lingerDeadline - System.nanoTime();
                            if (remaining <= 0) break;
                            flushSignal.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
                        }
                    }
                }
                Cycle cycle = swap();
                flush(cycle);
                if (!active) return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOG.log(System.Logger.Level.ERROR, "journal loop error", t);
            }
        }
    }

    private boolean pendingWork() {
        cycleGate.readLock().lock();
        try {
            return !current.isEmpty();
        } finally {
            cycleGate.readLock().unlock();
        }
    }

    private Cycle swap() {
        cycleGate.writeLock().lock();
        try {
            Cycle out = current;
            current = new Cycle();
            opsInCycle = 0;
            return out;
        } finally {
            cycleGate.writeLock().unlock();
        }
    }

    /** Mirrors one cycle in one fenced transaction; retries a failing flush until the fence itself
     *  says ownership moved. Acks complete only after the commit. */
    private void flush(Cycle cycle) throws InterruptedException {
        long until = System.currentTimeMillis() + leaseMillis;
        while (true) {
            try {
                storage.inTx(tx -> {
                    if (!tx.fenceJournalLease(owner, generation, until)) throw new Demoted();
                    for (Map.Entry<String, Snap> e : cycle.instances.entrySet()) {
                        Instance i = (Instance) e.getValue().row();
                        if (e.getValue().insert()) tx.insertInstance(i); else tx.updateInstance(i);
                    }
                    for (Map.Entry<String, Snap> e : cycle.tokens.entrySet()) {
                        Token t = (Token) e.getValue().row();
                        if (e.getValue().insert()) tx.insertToken(t); else tx.updateToken(t);
                    }
                    for (Rows.CompLog entry : cycle.compAppends) tx.appendCompensation(entry);
                    for (Object[] m : cycle.compMarks) tx.markCompensated((String) m[0], (Long) m[1]);
                    return null;
                });
                break;
            } catch (Demoted d) {
                demote(cycle);
                return;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "journal flush failed; retrying: " + e);
                Thread.sleep(100);
            }
        }
        notifier.signal(cycle.wakeQueues);
        cycle.flushed.complete(null);
        evictAfterFlush(cycle);
    }

    private static final class Demoted extends RuntimeException {}

    private void demote(Cycle cycle) {
        active = false;
        cycle.flushed.completeExceptionally(EngineException.conflict(
                "journal ownership moved (generation " + generation + " fenced out); this node "
                + "dropped its memory and no longer serves the data plane"));
        cycleGate.writeLock().lock();
        try {
            current.flushed.completeExceptionally(EngineException.conflict("journal demoted"));
            current = new Cycle();
            opsInCycle = 0;
            residents.clear();
            tokenOwner.clear();
        } finally {
            cycleGate.writeLock().unlock();
        }
        LOG.log(System.Logger.Level.ERROR, "journal demoted: another node holds the lease");
    }

    /** Terminal instances leave memory once mirrored; past the cap, clean idle residents do too. */
    private void evictAfterFlush(Cycle flushedCycle) {
        for (String id : flushedCycle.instances.keySet()) {
            Resident r = residents.get(id);
            if (r != null && r.inst.status != InstanceStatus.RUNNING
                    && r.inst.status != InstanceStatus.COMPENSATING) {
                evict(id, r);
            }
        }
        if (residents.size() <= residentMax) return;
        cycleGate.readLock().lock();      // no swap while judging cleanliness against `current`
        try {
            List<Map.Entry<String, Resident>> idle = new ArrayList<>(residents.entrySet());
            idle.sort((a, b) -> Long.compare(a.getValue().touched, b.getValue().touched));
            for (Map.Entry<String, Resident> e : idle) {
                if (residents.size() <= residentMax) break;
                if (current.instances.containsKey(e.getKey())) continue;   // dirty: not evictable
                boolean dirtyToken = false;
                for (String tid : e.getValue().tokens.keySet()) {
                    if (current.tokens.containsKey(tid)) { dirtyToken = true; break; }
                }
                if (!dirtyToken) evict(e.getKey(), e.getValue());
            }
        } finally {
            cycleGate.readLock().unlock();
        }
    }

    private void evict(String id, Resident r) {
        for (String tid : r.tokens.keySet()) tokenOwner.remove(tid);
        residents.remove(id);
    }

    @Override public void close() {
        if (!active) return;
        closing = true;
        synchronized (flushSignal) { flushSignal.notifyAll(); }
        try {
            flusher.join(TimeUnit.MILLISECONDS.toMillis(30_000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        active = false;
        try {
            storage.inTx(tx -> { tx.releaseJournalLease(owner, generation); return null; });
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "journal lease release failed (expires on its own): " + e);
        }
    }

    private Resident resident(String instanceId) {
        Resident r = residents.get(instanceId);
        if (r != null) return r;
        Resident fresh = new Resident();
        Resident raced = residents.putIfAbsent(instanceId, fresh);
        return raced != null ? raced : fresh;
    }

    /** The memory-backed transaction engine bodies run on. Reads and writes of instance, token and
     *  comp-log state hit resident memory; immutable and control-plane reads delegate; anything
     *  cross-instance throws, because those scans stay on the database as hints. */
    private final class JTx implements Tx {
        private final List<Resident> locked = new ArrayList<>();
        boolean dirty;

        void unlockAll() {
            for (int i = locked.size() - 1; i >= 0; i--) locked.get(i).lock.unlock();
            locked.clear();
        }

        private Resident acquire(String instanceId) {
            Resident r = resident(instanceId);
            if (!r.lock.isHeldByCurrentThread()) {
                r.lock.lock();
                locked.add(r);
            }
            if (r.inst == null) {                       // first touch: load the mirror state
                boolean loaded = storage.inTx(tx -> {
                    Optional<Instance> inst = tx.findInstance(instanceId);
                    if (inst.isEmpty()) return false;
                    r.inst = inst.get();
                    for (Token t : tx.tokensOf(instanceId)) {
                        r.tokens.put(t.id, t);
                        tokenOwner.put(t.id, instanceId);
                    }
                    return true;
                });
                if (!loaded) {
                    residents.remove(instanceId, r);
                    return null;
                }
            }
            r.touched = System.nanoTime();
            return r;
        }

        private void dirtyInstance(Instance inst, boolean insert) {
            dirty = true;
            current.instances.merge(inst.id, new Snap(inst.clone(), insert),
                    (a, b) -> new Snap(b.row(), a.insert() || b.insert()));
        }

        private void dirtyToken(Token t, boolean insert) {
            dirty = true;
            current.tokens.merge(t.id, new Snap(t.clone(), insert),
                    (a, b) -> new Snap(b.row(), a.insert() || b.insert()));
        }

        // ---- instances

        @Override public void insertInstance(Instance instance) {
            Resident r = resident(instance.id);
            if (!r.lock.isHeldByCurrentThread()) { r.lock.lock(); locked.add(r); }
            r.inst = instance;
            r.touched = System.nanoTime();
            dirtyInstance(instance, true);
        }

        @Override public Optional<Instance> lockInstance(String id) {
            Resident r = acquire(id);
            return r == null ? Optional.empty() : Optional.of(r.inst);
        }

        @Override public Optional<Instance> findInstance(String id) {
            Resident r = residents.get(id);
            if (r != null && r.inst != null) return Optional.of(r.inst);
            return storage.inTx(tx -> tx.findInstance(id));
        }

        @Override public List<Instance> findInstances(java.util.Collection<String> ids) {
            List<Instance> out = new ArrayList<>(ids.size());
            for (String id : ids) findInstance(id).ifPresent(out::add);
            return out;
        }

        @Override public Optional<Instance> lockInstanceOfTask(String taskId) {
            String instanceId = tokenOwner.get(taskId);
            if (instanceId == null) {
                instanceId = storage.inTx(tx -> tx.findToken(taskId)).map(t -> t.instanceId).orElse(null);
            }
            return instanceId == null ? Optional.empty() : lockInstance(instanceId);
        }

        @Override public void updateInstance(Instance instance) {
            dirtyInstance(instance, false);
        }

        // ---- tokens

        @Override public void insertToken(Token token) {
            Resident r = resident(token.instanceId);
            r.tokens.put(token.id, token);
            tokenOwner.put(token.id, token.instanceId);
            dirtyToken(token, true);
        }

        @Override public Optional<Token> findToken(String id) {
            String instanceId = tokenOwner.get(id);
            if (instanceId != null) {
                Resident r = residents.get(instanceId);
                if (r != null) {
                    Token t = r.tokens.get(id);
                    if (t != null) return Optional.of(t);
                }
            }
            return storage.inTx(tx -> tx.findToken(id));
        }

        @Override public List<Token> tokensOf(String instanceId) {
            Resident r = residents.get(instanceId);
            if (r != null && r.inst != null) return List.copyOf(r.tokens.values());
            return storage.inTx(tx -> tx.tokensOf(instanceId));
        }

        @Override public void updateToken(Token token) {
            dirtyToken(token, false);
        }

        // ---- compensation log

        @Override public void appendCompensation(Rows.CompLog entry) {
            comp(entry.instanceId).add(entry);
            dirty = true;
            current.compAppends.add(entry);
        }

        @Override public List<Rows.CompLog> compensationLog(String instanceId) {
            return comp(instanceId);
        }

        @Override public void markCompensated(String instanceId, long seq) {
            for (Rows.CompLog e : comp(instanceId)) {
                if (e.seq == seq) e.compensated = true;
            }
            dirty = true;
            current.compMarks.add(new Object[]{instanceId, seq});
        }

        private List<Rows.CompLog> comp(String instanceId) {
            Resident r = resident(instanceId);
            if (r.compLog == null) {
                r.compLog = new ArrayList<>(storage.inTx(tx -> tx.compensationLog(instanceId)));
            }
            return r.compLog;
        }

        // ---- children: the database plus residents the mirror has not seen yet

        @Override public List<String> childInstanceIds(String parentInstanceId) {
            Set<String> out = new java.util.LinkedHashSet<>(
                    storage.inTx(tx -> tx.childInstanceIds(parentInstanceId)));
            for (Map.Entry<String, Resident> e : residents.entrySet()) {
                Instance i = e.getValue().inst;
                if (i != null && i.parentTokenId != null
                        && parentInstanceId.equals(tokenOwner.get(i.parentTokenId))) {
                    out.add(i.id);
                }
            }
            return List.copyOf(out);
        }

        // ---- immutable / control-plane delegates (cold paths)

        @Override public void putDefinition(String name, int version, String json) {
            storage.inTxVoid(tx -> tx.putDefinition(name, version, json));
        }
        @Override public Optional<String> definition(String name, int version) {
            return storage.inTx(tx -> tx.definition(name, version));
        }
        @Override public Optional<Integer> latestVersion(String name) {
            return storage.inTx(tx -> tx.latestVersion(name));
        }
        @Override public List<String> definitionNames() {
            return storage.inTx(Tx::definitionNames);
        }
        @Override public void putGraph(WorkflowDefinition def) {
            storage.inTxVoid(tx -> tx.putGraph(def));
        }
        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            return storage.inTx(tx -> tx.graphNode(workflow, version, nodeId));
        }
        @Override public List<Node> graphNodes(String workflow, int version) {
            return storage.inTx(tx -> tx.graphNodes(workflow, version));
        }
        @Override public Optional<String> graphStartNode(String workflow, int version) {
            return storage.inTx(tx -> tx.graphStartNode(workflow, version));
        }
        @Override public void putSchedule(Rows.Schedule schedule) {
            storage.inTxVoid(tx -> tx.putSchedule(schedule));
        }
        @Override public void deleteSchedule(String id) {
            storage.inTxVoid(tx -> tx.deleteSchedule(id));
        }
        @Override public List<Rows.Schedule> schedules() {
            return storage.inTx(Tx::schedules);
        }
        @Override public Optional<Rows.Schedule> scheduleByWorkflow(String workflow) {
            return storage.inTx(tx -> tx.scheduleByWorkflow(workflow));
        }
        @Override public List<Rows.Schedule> dueSchedules(long now, int max) {
            return storage.inTx(tx -> tx.dueSchedules(now, max));
        }
        @Override public boolean claimSchedule(String id, long expectedFireAt, long nextFireAt) {
            return storage.inTx(tx -> tx.claimSchedule(id, expectedFireAt, nextFireAt));
        }

        // ---- cross-instance scans stay on the database, as hints

        @Override public List<Token> claimTasks(String w, Set<String> q, Set<WorkflowVersion> v, int m, long n, long l) {
            throw new IllegalStateException("claims go through Journal.claim, not through an op body");
        }
        @Override public List<Token> readyTasks(Set<String> q, Set<WorkflowVersion> v, int m, long n) {
            throw new IllegalStateException("scan not routed through the journal");
        }
        @Override public List<Token> dueTimers(long now, int max) { throw scans(); }
        @Override public List<Token> pendingSignals(int max) { throw scans(); }
        @Override public List<Token> dueSignals(long now, int max) { throw scans(); }
        @Override public List<Token> expiredLeases(long now, int max) { throw scans(); }
        @Override public List<Instance> listInstances(String w, InstanceStatus s, int l) { throw scans(); }
        @Override public List<Instance> findByCorrelation(String c, int l) { throw scans(); }
        @Override public int countInstances(InstanceStatus status) { throw scans(); }
        @Override public Rows.QueueDepth queueDepth(long now) { throw scans(); }
        @Override public List<Rows.BacklogSlice> backlogByVersion(long now, int max) { throw scans(); }
        @Override public int countProcessedSince(long since) { throw scans(); }
        @Override public void upsertNode(Rows.ServerNode node) { throw scans(); }
        @Override public List<Rows.ServerNode> nodes() { throw scans(); }
        @Override public void deleteNodesOlderThan(long before) { throw scans(); }
        @Override public void setLeader(String nodeId, boolean leader) { throw scans(); }
        @Override public int deleteTerminalInstancesBefore(long before, int limit) { throw scans(); }
        @Override public long claimJournalLease(String owner, long leaseMillis) { throw scans(); }
        @Override public boolean fenceJournalLease(String owner, long generation, long leaseUntil) { throw scans(); }
        @Override public void releaseJournalLease(String owner, long generation) { throw scans(); }

        private IllegalStateException scans() {
            return new IllegalStateException("cross-instance/cluster access is not routed through the journal");
        }
    }
}
