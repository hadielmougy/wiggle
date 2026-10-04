package com.wiggle.server.store;

import com.wiggle.core.Doc;
import com.wiggle.core.InstanceStatus;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows.CompLog;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Schedule;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Rows.Token;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The contract every {@link Storage} backend has to satisfy, asserted once and inherited by one
 * subclass per backend. A new backend adds that subclass and gets the whole suite; what it cannot
 * do fails here, by name, rather than silently in production.
 *
 * <p>It exists because each backend used to be tested by its own tests -- {@code GraphStoreTest}
 * against the in-memory store, {@code JdbcGraphTest} against SQL -- and two implementations of one
 * rule is two chances to drift, silently, because each passes its own tests. The spec these
 * assertions follow is {@code docs/spec/80-storage.md}.
 *
 * <p>Two chapters of the contract live elsewhere on purpose. The event log is
 * {@code tests/EventStoreTest}, which already runs against both backends and carries the feed's
 * retention semantics with it. The dialect-specific claim statement -- {@code FOR UPDATE SKIP
 * LOCKED} with {@code RETURNING}, which H2 cannot execute -- is {@code postgres/PostgresClaimTest};
 * what the claim must <em>mean</em>, on any dialect and either path, is here.
 *
 * <p><b>Isolation.</b> A backend may be a live database shared with the rest of the suite and
 * holding rows from earlier runs, so no assertion here may assume an empty store. Two disciplines
 * keep that honest: every row is named per run and per call ({@link #id}), and every read with a
 * global reach is pinned to a timestamp no other test writes. The due-work sweeps, the backlog and
 * the retention pass all filter on a time, so this suite backdates its own rows to the epoch's
 * first seconds ({@link #ANCIENT}) or forward past any real clock ({@link #FUTURE}) and asks the
 * sweep about that window. Where the contract offers no such filter -- {@code countInstances} --
 * the assertion is a delta.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class StorageContract {

    /** The backend under test, migrated and ready. Called once per subclass. */
    protected abstract Storage newStorage();

    /** A time no other test's rows occupy: before any real clock, so a sweep asked about this
     *  window sees only what this suite backdated into it. */
    private static final long ANCIENT = 1_000L;

    /** The same trick the other way, for the one sweep whose filter is a lower bound
     *  ({@code countProcessedSince}): past any clock a concurrent test could stamp. */
    private static final long FUTURE = 4_000_000_000_000L;

    private Storage storage;

    /** Unique across runs against a shared database, and short: token ids are {@code VARCHAR(64)}. */
    private final String run = Long.toHexString(System.nanoTime());
    private final AtomicLong seq = new AtomicLong();

    private long now;

    @BeforeAll
    void openStore() {
        storage = newStorage();
        now = System.currentTimeMillis();
    }

    @AfterAll
    void closeStore() {
        if (storage != null) storage.close();
    }

    // -- fixtures --

    private String id(String prefix) {
        return prefix + "-" + run + "-" + seq.incrementAndGet();
    }

    /** A one-key context. Built through a map so the JSON text is stable, which is what the two
     *  backends agree on: a number read back from SQL is a Long, one still held in memory is
     *  whatever the caller wrote, so {@link Doc#equals} is not the comparison to make. */
    private static Doc doc(String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(key, value);
        return Doc.of(m);
    }

    /** A context's counter, whatever numeric type it came back as. */
    private static int counter(Doc context) {
        Object n = context.get("n");
        return n == null ? 0 : ((Number) n).intValue();
    }

    /** {@code inTx} with its answer's type fixed. The assertion overloads take a
     *  {@code BooleanSupplier} and a {@code long}, so a transaction returning a primitive has to
     *  land in a typed slot before it reaches one. */
    private boolean inTxBoolean(Function<Tx, Boolean> work) {
        return storage.inTx(work);
    }

    private int inTxInt(Function<Tx, Integer> work) {
        return storage.inTx(work);
    }

    private Instance instance(String workflow) {
        Instance i = new Instance();
        i.id = id("wfi");
        i.workflow = workflow;
        i.version = 1;
        i.context = doc("stage", "start");
        i.createdAt = now;
        i.updatedAt = now;
        return i;
    }

    private Token token(Instance of, NodeKind kind, TokenStatus status, String queue) {
        Token t = new Token();
        t.id = id("tok");
        t.instanceId = of.id;
        t.workflow = of.workflow;
        t.version = of.version;
        t.nodeId = "n1";
        t.kind = kind;
        t.status = status;
        t.activity = of.workflow + "#step";
        t.queue = queue;
        t.availableAt = now;
        t.createdAt = now;
        t.updatedAt = now;
        return t;
    }

    /** Persists an instance and its tokens the way the engine does: in one transaction. */
    private void store(Instance i, Token... tokens) {
        storage.inTxVoid(tx -> {
            tx.insertInstance(i);
            for (Token t : tokens) tx.insertToken(t);
        });
    }

    /** {@code start -> end}, enough graph to normalise and read back a node. */
    private static WorkflowDefinition definition(String name, int version) {
        Map<String, Node> nodes = new LinkedHashMap<>();
        nodes.put("n1", Node.task("n1", "start", name + "#start", "q", null).withNext("end"));
        nodes.put("end", Node.end("end", true, null));
        return new WorkflowDefinition(name, version, "n1", nodes, Set.of("q"));
    }

    private void register(WorkflowDefinition d) {
        storage.inTxVoid(tx -> {
            tx.putDefinition(d.name(), d.version(), Json.write(d.toJson()),
                    d.fingerprint(), WorkflowDefinition.FINGERPRINT_ALGO);
            tx.putGraph(d);
        });
    }

    /** The ids of {@code tokens}, for an assertion that has to pick its own rows out of a shared store. */
    private static List<String> idsOf(List<Token> tokens) {
        return tokens.stream().map(t -> t.id).toList();
    }

    /** Runs {@code body} on {@code threads} threads, all released at once, and waits for every one
     *  of them. A failure inside one surfaces from {@code get()}. */
    private static void race(int threads, Runnable body) throws Exception {
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> running = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                running.add(pool.submit(() -> {
                    go.await();
                    body.run();
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : running) f.get();
        } finally {
            pool.shutdownNow();
        }
    }

    // -- WGL-STOR-002/003/084: the transaction is a unit, and an instance can be locked --

    @Test
    @DisplayName("inTx returns the body's value, and its writes are visible to the next transaction")
    void transactionsCommitAndReturn() {
        Instance i = instance(id("wf"));
        String returned = storage.inTx(tx -> {
            tx.insertInstance(i);
            return tx.findInstance(i.id).orElseThrow().id;
        });
        assertEquals(i.id, returned, "a write is visible to the transaction that made it");
        assertEquals(i.id, storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow().id);
    }

    @Test
    @DisplayName("a transaction that throws rolls back exactly as far as the store declares it can")
    void rollbackMatchesTheDeclaredTransactionality() {
        Instance i = instance(id("wf"));
        assertThrows(IllegalStateException.class, () -> storage.inTxVoid(tx -> {
            tx.insertInstance(i);
            throw new IllegalStateException("abandoning this transaction");
        }));
        boolean transactional = storage.inTx(Tx::transactional);
        boolean kept = storage.inTx(tx -> tx.findInstance(i.id)).isPresent();
        // Both answers are contractual; what is not allowed is a store whose declaration and its
        // behaviour disagree, because write-buffering keys off the declaration (see Tx#transactional).
        assertEquals(transactional, !kept, transactional
                ? "a store that declares itself transactional discards the writes of a throw"
                : "a store that declares it cannot roll back must leave the writes it already applied");
    }

    @Test
    @DisplayName("lockInstance serialises a read-modify-write across threads")
    void lockInstanceSerialisesReadModifyWrite() throws Exception {
        Instance seed = instance(id("wf"));
        seed.context = doc("n", 0);
        store(seed);

        int threads = 2;
        int each = 25;
        race(threads, () -> {
            for (int k = 0; k < each; k++) {
                storage.inTxVoid(tx -> {
                    Instance live = tx.lockInstance(seed.id).orElseThrow();
                    live.context = doc("n", counter(live.context) + 1);
                    live.updatedAt = System.currentTimeMillis();
                    tx.updateInstance(live);
                });
            }
        });

        Instance done = storage.inTx(tx -> tx.findInstance(seed.id)).orElseThrow();
        assertEquals(threads * each, counter(done.context),
                "every increment taken under the instance lock survived: no read-modify-write was lost");
        assertTrue(done.revision >= (long) threads * each, "and each one advanced the revision");
    }

    @Test
    @DisplayName("lockInstances is lockInstance over a set: missing ids are simply absent")
    void lockInstancesIsTheBatchedLock() {
        String wf = id("wf");
        Instance a = instance(wf);
        Instance b = instance(wf);
        store(a);
        store(b);
        String missing = id("wfi");

        List<String> asked = new ArrayList<>(List.of(a.id, b.id, missing));
        asked.sort(null);                                  // the contract takes ids already ascending
        List<Instance> locked = storage.inTx(tx -> tx.lockInstances(asked));

        assertEquals(Set.of(a.id, b.id), Set.copyOf(locked.stream().map(i -> i.id).toList()),
                "both rows that exist come back, the one that does not is absent");
        assertTrue(storage.inTx(tx -> tx.lockInstances(List.of())).isEmpty(), "no ids, no rows");
    }

    @Test
    @DisplayName("lockInstanceOf locks the instance owning a token; a missing token or instance is empty")
    void lockInstanceOfFindsTheOwner() {
        Instance owner = instance(id("wf"));
        Token task = token(owner, NodeKind.TASK, TokenStatus.RUNNING, "q");
        store(owner, task);
        Instance gone = instance(id("wf"));
        Token orphan = token(gone, NodeKind.TASK, TokenStatus.RUNNING, "q");
        storage.inTxVoid(tx -> tx.insertToken(orphan));

        assertEquals(owner.id, storage.inTx(tx -> tx.lockInstanceOf(task.id)).orElseThrow().id);
        assertTrue(storage.inTx(tx -> tx.lockInstanceOf(id("tok"))).isEmpty(), "no such token");
        assertTrue(storage.inTx(tx -> tx.lockInstanceOf(orphan.id)).isEmpty(), "a token whose instance is gone");
    }

    @Test
    @DisplayName("lockTask locks a token's instance and reads the token; a missing token or instance is empty")
    void lockTaskFindsBothRows() {
        Instance owner = instance(id("wf"));
        Token task = token(owner, NodeKind.TASK, TokenStatus.RUNNING, "q");
        store(owner, task);
        Instance gone = instance(id("wf"));
        Token orphan = token(gone, NodeKind.TASK, TokenStatus.RUNNING, "q");
        storage.inTxVoid(tx -> tx.insertToken(orphan));

        Rows.LockedTask locked = storage.inTx(tx -> tx.lockTask(task.id)).orElseThrow();
        assertEquals(owner.id, locked.inst().id);
        assertEquals(owner.revision, locked.inst().revision);
        assertEquals(task.id, locked.token().id);
        assertEquals(TokenStatus.RUNNING, locked.token().status);
        assertTrue(storage.inTx(tx -> tx.lockTask(id("tok"))).isEmpty(), "no such token");
        assertTrue(storage.inTx(tx -> tx.lockTask(orphan.id)).isEmpty(), "a token whose instance is gone");
    }

    @Test
    @DisplayName("lockInstanceOf serialises a read-modify-write across threads, as lockInstance does")
    void lockInstanceOfSerialisesReadModifyWrite() throws Exception {
        Instance seed = instance(id("wf"));
        seed.context = doc("n", 0);
        Token task = token(seed, NodeKind.TASK, TokenStatus.RUNNING, "q");
        store(seed, task);

        int threads = 2;
        int each = 25;
        race(threads, () -> {
            for (int k = 0; k < each; k++) {
                storage.inTxVoid(tx -> {
                    Instance live = tx.lockInstanceOf(task.id).orElseThrow();
                    live.context = doc("n", counter(live.context) + 1);
                    live.updatedAt = System.currentTimeMillis();
                    tx.updateInstance(live);
                });
            }
        });

        Instance done = storage.inTx(tx -> tx.findInstance(seed.id)).orElseThrow();
        assertEquals(threads * each, counter(done.context), "no increment taken under the lock was lost");
    }

    @Test
    @DisplayName("joinedAt is the instance's JOINED tokens at one node, in id order")
    void joinedAtIsTheBarrier() {
        Instance inst = instance(id("wf"));
        Token a = token(inst, NodeKind.JOIN, TokenStatus.JOINED, null);
        Token b = token(inst, NodeKind.JOIN, TokenStatus.JOINED, null);
        Token elsewhere = token(inst, NodeKind.JOIN, TokenStatus.JOINED, null);
        elsewhere.nodeId = "n2";
        Token settled = token(inst, NodeKind.JOIN, TokenStatus.DONE, null);
        Instance other = instance(id("wf"));
        Token foreign = token(other, NodeKind.JOIN, TokenStatus.JOINED, null);
        store(inst, a, b, elsewhere, settled);
        store(other, foreign);

        assertEquals(List.of(a.id, b.id), idsOf(storage.inTx(tx -> tx.joinedAt(inst.id, "n1"))));
        assertEquals(List.of(elsewhere.id), idsOf(storage.inTx(tx -> tx.joinedAt(inst.id, "n2"))));
        assertTrue(storage.inTx(tx -> tx.joinedAt(inst.id, "n3")).isEmpty());
    }

    @Test
    @DisplayName("awaitingSignal finds the token waiting on that signal, and nothing else")
    void awaitingSignalFindsTheWait() {
        Instance inst = instance(id("wf"));
        Token approve = token(inst, NodeKind.SIGNAL, TokenStatus.AWAITING, null);
        approve.activity = "approve";
        Token reject = token(inst, NodeKind.SIGNAL, TokenStatus.AWAITING, null);
        reject.activity = "reject";
        Token delivered = token(inst, NodeKind.SIGNAL, TokenStatus.DONE, null);
        delivered.activity = "ship";
        store(inst, approve, reject, delivered);

        assertEquals(approve.id, storage.inTx(tx -> tx.awaitingSignal(inst.id, "approve")).orElseThrow().id);
        assertEquals(reject.id, storage.inTx(tx -> tx.awaitingSignal(inst.id, "reject")).orElseThrow().id);
        assertTrue(storage.inTx(tx -> tx.awaitingSignal(inst.id, "ship")).isEmpty(), "already delivered");
        assertTrue(storage.inTx(tx -> tx.awaitingSignal(inst.id, "unknown")).isEmpty());
    }

    // -- WGL-STOR-030/031/032: definitions and their normalised graphs --

    @Test
    @DisplayName("the definition blob is write-once, and replaceDefinition is what overwrites it")
    void definitionBlobIsWriteOnceThenReplaceable() {
        WorkflowDefinition d = definition(id("wf"), 1);
        register(d);
        assertEquals(Json.write(d.toJson()), storage.inTx(tx -> tx.definition(d.name(), 1)).orElseThrow());

        storage.inTxVoid(tx -> tx.putDefinition(d.name(), 1, "{\"other\":true}", "fp2", "algo2"));
        assertEquals(Json.write(d.toJson()), storage.inTx(tx -> tx.definition(d.name(), 1)).orElseThrow(),
                "putDefinition leaves an existing version alone, so two nodes cannot collide on it");

        storage.inTxVoid(tx -> tx.replaceDefinition(d.name(), 1, "{\"other\":true}", "fp2", "algo2"));
        assertEquals("{\"other\":true}", storage.inTx(tx -> tx.definition(d.name(), 1)).orElseThrow());
        assertEquals(new GraphStore.StoredFingerprint("fp2", "algo2"),
                storage.inTx(tx -> tx.definitionFingerprint(d.name(), 1)).orElseThrow());
    }

    @Test
    @DisplayName("an unregistered version has no fingerprint at all")
    void fingerprintIsAbsentForAnUnknownVersion() {
        assertTrue(storage.inTx(tx -> tx.definitionFingerprint(id("wf"), 1)).isEmpty());
        assertTrue(storage.inTx(tx -> tx.definition(id("wf"), 1)).isEmpty());
        assertTrue(storage.inTx(tx -> tx.latestVersion(id("wf"))).isEmpty());
    }

    @Test
    @DisplayName("the latest version is the highest, not the most recently written")
    void latestVersionIsTheHighest() {
        String wf = id("wf");
        register(definition(wf, 2));
        register(definition(wf, 1));
        assertEquals(2, storage.inTx(tx -> tx.latestVersion(wf)).orElseThrow(),
                "versions are author-declared and ordered");
        assertTrue(storage.inTx(tx -> tx.definitionNames()).contains(wf));
    }

    @Test
    @DisplayName("a graph is read one node at a time, and deleteGraph drops the rows")
    void graphNodesAreReadableOneAtATime() {
        WorkflowDefinition d = definition(id("wf"), 1);
        register(d);

        assertEquals("n1", storage.inTx(tx -> tx.graphStartNode(d.name(), 1)).orElseThrow(),
                "the entry node, without loading any other");
        Node n1 = storage.inTx(tx -> tx.graphNode(d.name(), 1, "n1")).orElseThrow();
        assertEquals(NodeKind.TASK, n1.kind());
        assertEquals(d.name() + "#start", n1.activity());
        assertEquals("end", n1.next(), "a node arrives with its outgoing edges");
        assertTrue(storage.inTx(tx -> tx.graphNode(d.name(), 1, "nope")).isEmpty());

        storage.inTxVoid(tx -> tx.deleteGraph(d.name(), 1));
        assertTrue(storage.inTx(tx -> tx.graphNode(d.name(), 1, "n1")).isEmpty());
        assertTrue(storage.inTx(tx -> tx.graphStartNode(d.name(), 1)).isEmpty());
        assertTrue(storage.inTx(tx -> tx.definition(d.name(), 1)).isPresent(),
                "the blob outlives the normalised rows: it is the audit copy");
    }

    // -- instances --

    @Test
    @DisplayName("an instance round-trips every field it was stored with")
    void instanceRoundTrips() {
        Instance i = instance(id("wf"));
        i.correlationId = "order-" + i.id;
        i.status = InstanceStatus.COMPENSATION_FAILED;
        i.terminationReason = "undo refused";
        i.error = "boom";
        i.context = doc("stage", "paid");
        i.parentTokenId = id("tok");
        i.settleAt = now + 60_000;
        i.revision = 7;
        store(i);

        Instance back = storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow();
        assertEquals(i.id, back.id);
        assertEquals(i.workflow, back.workflow);
        assertEquals(1, back.version);
        assertEquals(i.correlationId, back.correlationId);
        assertEquals(InstanceStatus.COMPENSATION_FAILED, back.status, "the widest status name survives");
        assertEquals("undo refused", back.terminationReason);
        assertEquals("boom", back.error);
        assertEquals(i.context.json(), back.context.json());
        assertEquals(i.parentTokenId, back.parentTokenId);
        assertEquals(i.settleAt, back.settleAt);
        assertEquals(now, back.createdAt);
        assertEquals(now, back.updatedAt);
        assertEquals(7, back.revision);
        assertTrue(storage.inTx(tx -> tx.findInstance(id("wfi"))).isEmpty(), "an unknown id is absent");
    }

    @Test
    @DisplayName("a null settleAt and a null correlation id are stored as absent, not as text")
    void instanceNullablesStayNull() {
        Instance i = instance(id("wf"));
        store(i);
        Instance back = storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow();
        assertNull(back.settleAt, "only an observed run has a settle time");
        assertNull(back.correlationId);
        assertNull(back.parentTokenId);
        assertNull(back.terminationReason);
        assertNull(back.error);
    }

    @Test
    @DisplayName("insertInstanceIfAbsent leaves the winner's row standing")
    void insertIfAbsentIsIdempotent() {
        Instance first = instance(id("wf"));
        first.context = doc("reporter", "a");
        assertTrue(inTxBoolean(tx -> tx.insertInstanceIfAbsent(first)), "the first reporter creates the run");

        Instance second = instance(first.workflow);
        second.id = first.id;
        second.context = doc("reporter", "b");
        assertFalse(inTxBoolean(tx -> tx.insertInstanceIfAbsent(second)), "the second is told it lost");
        assertEquals(first.context.json(), storage.inTx(tx -> tx.findInstance(first.id)).orElseThrow().context.json(),
                "and the loser overwrote nothing -- it re-reads instead");
    }

    @Test
    @DisplayName("updateInstance writes the mutable fields and advances the revision")
    void updateInstanceWritesTheMutableFields() {
        Instance i = instance(id("wf"));
        store(i);

        Instance live = storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow();
        long before = live.revision;
        live.status = InstanceStatus.FAILED;
        live.terminationReason = "step failed";
        live.error = "handler threw";
        live.context = doc("stage", "failed");
        live.settleAt = 42L;
        live.updatedAt = now + 5;
        storage.inTxVoid(tx -> tx.updateInstance(live));

        assertEquals(before + 1, live.revision, "the caller's row is advanced too: the engine writes it back");
        Instance back = storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow();
        assertEquals(InstanceStatus.FAILED, back.status);
        assertEquals("step failed", back.terminationReason);
        assertEquals("handler threw", back.error);
        assertEquals(doc("stage", "failed").json(), back.context.json());
        assertEquals(42L, back.settleAt);
        assertEquals(now + 5, back.updatedAt);
        assertEquals(before + 1, back.revision);
    }

    @Test
    @DisplayName("updateInstance leaves the fields that settle an instance's identity alone")
    void updateInstanceDoesNotWriteTheIdentityFields() {
        Instance i = instance(id("wf"));
        i.correlationId = "order-" + i.id;
        i.parentTokenId = id("tok");
        store(i);

        Instance live = storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow();
        live.workflow = id("rewritten");
        live.version = 99;
        live.correlationId = "rewritten";
        live.parentTokenId = "rewritten";
        live.createdAt = now + 10_000;
        live.status = InstanceStatus.COMPLETED;      // one mutable field, so the write is no no-op
        live.updatedAt = now + 1;
        storage.inTxVoid(tx -> tx.updateInstance(live));

        Instance back = storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow();
        assertEquals(InstanceStatus.COMPLETED, back.status, "the mutable field was written");
        assertEquals(now + 1, back.updatedAt);
        // And none of the rest, however the caller had left them: an instance's workflow, its
        // version, its business key, its parent and when it started are settled at insert.
        assertEquals(i.workflow, back.workflow);
        assertEquals(1, back.version);
        assertEquals(i.correlationId, back.correlationId);
        assertEquals(i.parentTokenId, back.parentTokenId);
        assertEquals(now, back.createdAt);
    }

    @Test
    @DisplayName("the revision counts the writes the row has taken, not the writer's idea of them")
    void revisionIsAdvancedFromTheStoredRow() {
        Instance i = instance(id("wf"));
        store(i);
        for (int k = 0; k < 2; k++) {
            Instance live = storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow();
            live.updatedAt = now + k;
            storage.inTxVoid(tx -> tx.updateInstance(live));
        }
        assertEquals(2, storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow().revision);

        // A caller that never re-read, so its copy still says revision 0. The next revision is the
        // stored one plus one: a stale writer cannot walk it backwards.
        i.revision = 0;
        i.updatedAt = now + 5;
        storage.inTxVoid(tx -> tx.updateInstance(i));
        assertEquals(3, storage.inTx(tx -> tx.findInstance(i.id)).orElseThrow().revision);
        assertEquals(1, i.revision, "and the caller's own copy is advanced from where it was");
    }

    @Test
    @DisplayName("updateInstance on a row that is not there creates nothing")
    void updateInstanceOnAMissingRowIsANoOp() {
        Instance never = instance(id("wf"));
        storage.inTxVoid(tx -> tx.updateInstance(never));
        assertTrue(storage.inTx(tx -> tx.findInstance(never.id)).isEmpty(),
                "a write-back is not a way to insert");
    }

    @Test
    @DisplayName("updateInstances is updateInstance over a set")
    void updateInstancesIsTheBatchedUpdate() {
        String wf = id("wf");
        Instance a = instance(wf);
        Instance b = instance(wf);
        store(a);
        store(b);

        List<Instance> both = storage.inTx(tx -> tx.lockInstances(
                Stream.of(a.id, b.id).sorted().toList()));
        for (Instance i : both) {
            i.status = InstanceStatus.COMPLETED;
            i.updatedAt = now + 9;
        }
        storage.inTxVoid(tx -> tx.updateInstances(both));

        for (String id : List.of(a.id, b.id)) {
            Instance back = storage.inTx(tx -> tx.findInstance(id)).orElseThrow();
            assertEquals(InstanceStatus.COMPLETED, back.status);
            assertEquals(now + 9, back.updatedAt);
            assertEquals(1, back.revision, "one update, one revision");
        }
        storage.inTxVoid(tx -> tx.updateInstances(List.of()));
    }

    @Test
    @DisplayName("listInstances filters by workflow and status, newest first, bounded")
    void listInstancesFiltersAndOrders() {
        String wf = id("wf");
        Instance old = instance(wf);
        old.createdAt = ANCIENT;
        Instance mid = instance(wf);
        mid.createdAt = ANCIENT + 1;
        Instance newest = instance(wf);
        newest.createdAt = ANCIENT + 2;
        newest.status = InstanceStatus.COMPLETED;
        store(old);
        store(mid);
        store(newest);

        assertEquals(List.of(newest.id, mid.id, old.id),
                storage.inTx(tx -> tx.listInstances(wf, null, 10)).stream().map(i -> i.id).toList(),
                "newest first by creation");
        assertEquals(List.of(mid.id, old.id),
                storage.inTx(tx -> tx.listInstances(wf, InstanceStatus.RUNNING, 10)).stream().map(i -> i.id).toList(),
                "and narrowed by status");
        assertEquals(List.of(newest.id),
                storage.inTx(tx -> tx.listInstances(wf, null, 1)).stream().map(i -> i.id).toList(),
                "the limit takes the newest, not an arbitrary row");
    }

    @Test
    @DisplayName("findByCorrelation is the business key, newest first")
    void findByCorrelationIsNewestFirst() {
        String key = "corr-" + run + "-" + seq.incrementAndGet();
        Instance first = instance(id("wf"));
        first.correlationId = key;
        first.createdAt = ANCIENT;
        Instance second = instance(first.workflow);
        second.correlationId = key;
        second.createdAt = ANCIENT + 1;
        store(first);
        store(second);

        assertEquals(List.of(second.id, first.id),
                storage.inTx(tx -> tx.findByCorrelation(key, 10)).stream().map(i -> i.id).toList());
        assertEquals(List.of(second.id),
                storage.inTx(tx -> tx.findByCorrelation(key, 1)).stream().map(i -> i.id).toList());
        assertTrue(storage.inTx(tx -> tx.findByCorrelation("nothing-" + run, 10)).isEmpty());
    }

    @Test
    @DisplayName("countInstances counts by status")
    void countInstancesCountsByStatus() {
        int before = inTxInt(tx -> tx.countInstances(InstanceStatus.CANCELLED));
        String wf = id("wf");
        for (int k = 0; k < 2; k++) {
            Instance i = instance(wf);
            i.status = InstanceStatus.CANCELLED;
            store(i);
        }
        assertEquals(before + 2, inTxInt(tx -> tx.countInstances(InstanceStatus.CANCELLED)));
    }

    // -- tokens --

    @Test
    @DisplayName("a token round-trips every field, and its payload through the codec")
    void tokenRoundTrips() {
        Instance i = instance(id("wf"));
        Token t = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        t.nodeId = "n7";
        t.attempt = 2;
        t.leaseOwner = "worker-1";
        t.leaseExpiresAt = now + 30_000;
        t.joinStack = "fork-1,fork-2";
        t.lastError = "timed out";
        t.compSeq = 3L;
        t.startedAt = now + 1;
        t.finishedAt = now + 2;
        t.seq = 11L;
        t.stepInput = "{\"order\":1}";
        t.stepOutput = "{\"order\":1,\"paid\":true}";
        t.payload = TokenPayload.EMPTY
                .push(TokenPayload.FrameKind.ARM, 1, null, doc("arm", "left"))
                .push(TokenPayload.FrameKind.ITEM, 2, "k2", doc("item", "second"))
                .withLoopCount("gate", 3)
                .withStaged(Map.of("left", "done"));
        store(i, t);

        Token back = storage.inTx(tx -> tx.findToken(t.id)).orElseThrow();
        assertEquals(t.id, back.id);
        assertEquals(i.id, back.instanceId);
        assertEquals(i.workflow, back.workflow);
        assertEquals(1, back.version);
        assertEquals("n7", back.nodeId);
        assertEquals(NodeKind.TASK, back.kind);
        assertEquals(TokenStatus.RUNNING, back.status);
        assertEquals(t.activity, back.activity);
        assertEquals(t.queue, back.queue);
        assertEquals(2, back.attempt);
        assertEquals(now, back.availableAt);
        assertEquals("worker-1", back.leaseOwner);
        assertEquals(now + 30_000, back.leaseExpiresAt);
        assertEquals("fork-1,fork-2", back.joinStack);
        assertEquals("fork-2", back.currentJoinGroup());
        assertEquals("timed out", back.lastError);
        assertEquals(3L, back.compSeq);
        assertEquals(now + 1, back.startedAt);
        assertEquals(now + 2, back.finishedAt);
        assertEquals(11L, back.seq);
        assertEquals("{\"order\":1}", back.stepInput);
        assertEquals("{\"order\":1,\"paid\":true}", back.stepOutput);
        assertEquals(now, back.createdAt);
        assertEquals(now, back.updatedAt);
        // Every store encodes the payload on write, so what comes back is what the codec makes of
        // it -- a JSON number is a Long either way -- and not the object that was handed in.
        assertEquals(PayloadCodec.decode(PayloadCodec.encode(t.payload)), back.payload);
        assertTrue(storage.inTx(tx -> tx.findToken(id("tok"))).isEmpty(), "an unknown id is absent");
    }

    @Test
    @DisplayName("an update writes a token's recorded step input and output, and can clear them")
    void tokenStepIoUpdates() {
        Instance i = instance(id("wf"));
        Token t = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        store(i, t);
        assertNull(storage.inTx(tx -> tx.findToken(t.id)).orElseThrow().stepInput, "nothing recorded yet");

        Token settled = storage.inTx(tx -> tx.findToken(t.id)).orElseThrow();
        settled.stepInput = "{\"in\":1}";
        settled.stepOutput = "true";
        storage.inTx(tx -> { tx.updateToken(settled); return null; });
        Token back = storage.inTx(tx -> tx.findToken(t.id)).orElseThrow();
        assertEquals("{\"in\":1}", back.stepInput);
        assertEquals("true", back.stepOutput);

        back.stepOutput = null;
        storage.inTx(tx -> { tx.updateToken(back); return null; });
        assertNull(storage.inTx(tx -> tx.findToken(t.id)).orElseThrow().stepOutput);
    }

    @Test
    @DisplayName("a token outside every scope carries the empty payload, not null")
    void unscopedTokenCarriesTheEmptyPayload() {
        Instance i = instance(id("wf"));
        Token t = token(i, NodeKind.TASK, TokenStatus.READY, id("q"));
        store(i, t);
        Token back = storage.inTx(tx -> tx.findToken(t.id)).orElseThrow();
        assertEquals(TokenPayload.EMPTY, back.payload);
        assertEquals("", back.joinStack);
        assertNull(back.currentJoinGroup());
        assertNull(back.compSeq, "null comp seq is what tells forward work from an undo");
        assertNull(back.startedAt);
        assertNull(back.finishedAt);
        assertNull(back.seq);
        assertNull(back.leaseOwner);
    }

    @Test
    @DisplayName("the batched token forms are the single-row forms, and miss quietly")
    void batchedTokenFormsMatchTheSingleRowForms() {
        Instance i = instance(id("wf"));
        String q = id("q");
        Token a = token(i, NodeKind.TASK, TokenStatus.READY, q);
        Token b = token(i, NodeKind.TASK, TokenStatus.READY, q);
        storage.inTxVoid(tx -> {
            tx.insertInstance(i);
            tx.insertTokens(List.of(a, b));
        });

        List<String> asked = new ArrayList<>(List.of(a.id, b.id, id("tok")));
        List<Token> found = storage.inTx(tx -> tx.findTokens(asked));
        assertEquals(Set.of(a.id, b.id), Set.copyOf(idsOf(found)), "missing ids are absent, order is free");

        for (Token t : found) {
            t.status = TokenStatus.DONE;
            t.updatedAt = now + 3;
        }
        storage.inTxVoid(tx -> tx.updateTokens(found));
        for (String id : List.of(a.id, b.id)) {
            Token back = storage.inTx(tx -> tx.findToken(id)).orElseThrow();
            assertEquals(TokenStatus.DONE, back.status);
            assertEquals(now + 3, back.updatedAt);
        }

        storage.inTxVoid(tx -> tx.insertTokens(List.of()));
        assertTrue(storage.inTx(tx -> tx.findTokens(List.of())).isEmpty());
        storage.inTxVoid(tx -> tx.updateTokens(List.of()));
    }

    @Test
    @DisplayName("tokensOf is the instance's tokens and nobody else's")
    void tokensOfIsScopedToTheInstance() {
        String wf = id("wf");
        String q = id("q");
        Instance mine = instance(wf);
        Instance other = instance(wf);
        Token a = token(mine, NodeKind.TASK, TokenStatus.READY, q);
        Token b = token(mine, NodeKind.SLEEP, TokenStatus.WAITING, null);
        store(mine, a, b);
        store(other, token(other, NodeKind.TASK, TokenStatus.READY, q));

        assertEquals(Set.of(a.id, b.id), Set.copyOf(idsOf(storage.inTx(tx -> tx.tokensOf(mine.id)))));
        assertTrue(storage.inTx(tx -> tx.tokensOf(id("wfi"))).isEmpty());
    }

    @Test
    @DisplayName("hasActiveTokens follows the status table, per instance")
    void hasActiveTokensFollowsTheStatusTable() {
        Instance i = instance(id("wf"));
        Token active = token(i, NodeKind.SIGNAL, TokenStatus.AWAITING, null);
        Token done = token(i, NodeKind.TASK, TokenStatus.DONE, id("q"));
        store(i, active, done);
        assertTrue(inTxBoolean(tx -> tx.hasActiveTokens(i.id)), "one AWAITING token is enough");

        active.status = TokenStatus.CANCELLED;
        active.updatedAt = now + 1;
        storage.inTxVoid(tx -> tx.updateToken(active));
        assertFalse(inTxBoolean(tx -> tx.hasActiveTokens(i.id)), "DONE and CANCELLED are not active");
        assertFalse(inTxBoolean(tx -> tx.hasActiveTokens(id("wfi"))), "an unknown instance has none");
    }

    @Test
    @DisplayName("joinStacksAt counts the arrivals parked at one barrier")
    void joinStacksAtSeesOnlyTheParkedTokens() {
        Instance i = instance(id("wf"));
        Token arrivedA = token(i, NodeKind.JOIN, TokenStatus.JOINED, null);
        arrivedA.nodeId = "j1";
        arrivedA.joinStack = "fork-1";
        Token arrivedB = token(i, NodeKind.JOIN, TokenStatus.JOINED, null);
        arrivedB.nodeId = "j1";
        arrivedB.joinStack = "fork-1";
        Token elsewhere = token(i, NodeKind.JOIN, TokenStatus.JOINED, null);
        elsewhere.nodeId = "j2";
        elsewhere.joinStack = "fork-9";
        Token stillRunning = token(i, NodeKind.TASK, TokenStatus.READY, id("q"));
        stillRunning.nodeId = "j1";
        stillRunning.joinStack = "fork-1";
        store(i, arrivedA, arrivedB, elsewhere, stillRunning);

        assertEquals(List.of("fork-1", "fork-1"), storage.inTx(tx -> tx.joinStacksAt(i.id, "j1")),
                "both arrivals at this barrier, and neither the token still running nor the other node");
        assertEquals(List.of("fork-9"), storage.inTx(tx -> tx.joinStacksAt(i.id, "j2")));
        assertTrue(storage.inTx(tx -> tx.joinStacksAt(i.id, "j3")).isEmpty());
    }

    @Test
    @DisplayName("cancelActiveTokens takes the active ones, releases their leases, and leaves the rest")
    void cancelActiveTokensTakesOnlyTheActive() {
        Instance i = instance(id("wf"));
        Token leased = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        leased.leaseOwner = "worker-1";
        leased.leaseExpiresAt = now + 30_000;
        Token parked = token(i, NodeKind.JOIN, TokenStatus.JOINED, null);
        Token settled = token(i, NodeKind.TASK, TokenStatus.DONE, id("q"));
        store(i, leased, parked, settled);

        storage.inTxVoid(tx -> tx.cancelActiveTokens(i.id, now + 7));

        Token wasLeased = storage.inTx(tx -> tx.findToken(leased.id)).orElseThrow();
        assertEquals(TokenStatus.CANCELLED, wasLeased.status);
        assertNull(wasLeased.leaseOwner, "a cancelled token holds no lease");
        assertEquals(0, wasLeased.leaseExpiresAt);
        assertEquals(now + 7, wasLeased.updatedAt);
        assertEquals(TokenStatus.CANCELLED, storage.inTx(tx -> tx.findToken(parked.id)).orElseThrow().status);
        assertEquals(TokenStatus.DONE, storage.inTx(tx -> tx.findToken(settled.id)).orElseThrow().status,
                "a settled token is not re-stamped");
    }

    @Test
    @DisplayName("childInstanceIds are the sub-workflows started from this instance's tokens")
    void childInstanceIdsAreTheSubWorkflows() {
        String wf = id("wf");
        Instance parent = instance(wf);
        Token waiting = token(parent, NodeKind.SUB_WORKFLOW, TokenStatus.AWAITING, null);
        store(parent, waiting);

        Instance childA = instance(wf);
        childA.parentTokenId = waiting.id;
        Instance childB = instance(wf);
        childB.parentTokenId = waiting.id;
        Instance unrelated = instance(wf);
        store(childA);
        store(childB);
        store(unrelated);

        assertEquals(Stream.of(childA.id, childB.id).sorted().toList(),
                storage.inTx(tx -> tx.childInstanceIds(parent.id)), "by id, and nobody else's children");
        assertTrue(storage.inTx(tx -> tx.childInstanceIds(unrelated.id)).isEmpty());
    }

    // -- WGL-STOR-021/022: the claim, whichever statement the dialect uses --

    @Test
    @DisplayName("the claim serves the oldest instance's ready work first, and skips what is not due")
    void claimServesOldestInstanceFirst() {
        String q = id("q");
        Instance older = instance(id("wf"));
        older.createdAt = now - 10_000;
        Instance newer = instance(id("wf"));
        newer.createdAt = now - 5_000;
        Token newerReadyLongest = token(newer, NodeKind.TASK, TokenStatus.READY, q);
        newerReadyLongest.instCreatedAt = newer.createdAt;
        newerReadyLongest.availableAt = ANCIENT;
        Token olderReadyLater = token(older, NodeKind.TASK, TokenStatus.READY, q);
        olderReadyLater.instCreatedAt = older.createdAt;
        olderReadyLater.availableAt = ANCIENT + 100;
        Token olderNotDue = token(older, NodeKind.TASK, TokenStatus.READY, q);
        olderNotDue.instCreatedAt = older.createdAt;
        olderNotDue.availableAt = now + 60_000;
        store(older, olderReadyLater, olderNotDue);
        store(newer, newerReadyLongest);

        assertEquals(List.of(olderReadyLater.id),
                idsOf(storage.inTx(tx -> tx.claimTasks("w1", Set.of(q), null, 1, now, now + 1_000))),
                "the older instance first, though the newer one's task has been ready longer");
        assertEquals(List.of(newerReadyLongest.id),
                idsOf(storage.inTx(tx -> tx.claimTasks("w1", Set.of(q), null, 10, now, now + 1_000))),
                "a task not yet due is stepped over, not the end of the scan");
    }

    @Test
    @DisplayName("the claim leases a token, and never offers it twice")
    void claimLeasesAndIsNotOfferedTwice() {
        Instance i = instance(id("wf"));
        String q = id("q");
        Token t = token(i, NodeKind.TASK, TokenStatus.READY, q);
        t.availableAt = ANCIENT;
        store(i, t);

        List<Token> claimed = storage.inTx(tx -> tx.claimTasks("w1", Set.of(q), null, 10, now, now + 30_000));
        assertEquals(List.of(t.id), idsOf(claimed));
        Token got = claimed.getFirst();
        assertEquals(TokenStatus.RUNNING, got.status);
        assertEquals("w1", got.leaseOwner);
        assertEquals(now + 30_000, got.leaseExpiresAt);
        assertEquals(now, got.startedAt, "the step's clock starts when a worker takes it");
        assertNull(got.finishedAt);

        assertTrue(storage.inTx(tx -> tx.claimTasks("w2", Set.of(q), null, 10, now, now + 30_000)).isEmpty(),
                "a claimed token is not offered again");
        Token stored = storage.inTx(tx -> tx.findToken(t.id)).orElseThrow();
        assertEquals(TokenStatus.RUNNING, stored.status, "and the lease is durable, not just returned");
        assertEquals("w1", stored.leaseOwner);
    }

    @Test
    @DisplayName("the claim honours the queue and version filters, and an empty set is no filter")
    void claimHonoursTheFilters() {
        String q = id("q");
        String wanted = id("wf");
        String unwanted = id("wf");
        Instance a = instance(wanted);
        Instance b = instance(unwanted);
        Token onQueue = token(a, NodeKind.TASK, TokenStatus.READY, q);
        onQueue.availableAt = ANCIENT;
        Token otherVersion = token(b, NodeKind.TASK, TokenStatus.READY, q);
        otherVersion.availableAt = ANCIENT;
        Token otherQueue = token(a, NodeKind.TASK, TokenStatus.READY, id("q"));
        otherQueue.availableAt = ANCIENT;
        store(a, onQueue, otherQueue);
        store(b, otherVersion);

        Set<WorkflowVersion> only = Set.of(new WorkflowVersion(wanted, 1));
        assertEquals(List.of(onQueue.id),
                idsOf(storage.inTx(tx -> tx.claimTasks("w1", Set.of(q), only, 10, now, now + 1_000))),
                "the queue narrows it, and so does the worker's bound version");

        assertEquals(List.of(otherQueue.id),
                idsOf(storage.inTx(tx -> tx.claimTasks("w2", Set.of(), only, 10, now, now + 1_000))),
                "an empty queue set is no queue filter: the version-scoped worker still gets its own work");
        assertEquals(List.of(otherVersion.id),
                idsOf(storage.inTx(tx -> tx.claimTasks("w3", Set.of(q), null, 10, now, now + 1_000))),
                "and a null version set is no version filter");
    }

    @Test
    @DisplayName("the claim passes over work that is not due yet, and is bounded by max")
    void claimSkipsFutureWorkAndRespectsMax() {
        Instance i = instance(id("wf"));
        String q = id("q");
        List<Token> due = new ArrayList<>();
        for (int k = 0; k < 3; k++) {
            Token t = token(i, NodeKind.TASK, TokenStatus.READY, q);
            t.availableAt = ANCIENT + k;
            due.add(t);
        }
        Token later = token(i, NodeKind.TASK, TokenStatus.READY, q);
        later.availableAt = now + 3_600_000;
        storage.inTxVoid(tx -> {
            tx.insertInstance(i);
            tx.insertTokens(due);
            tx.insertToken(later);
        });

        List<Token> first = storage.inTx(tx -> tx.claimTasks("w1", Set.of(q), null, 2, now, now + 1_000));
        assertEquals(2, first.size(), "max bounds one claim");
        List<Token> rest = storage.inTx(tx -> tx.claimTasks("w1", Set.of(q), null, 10, now, now + 1_000));
        assertEquals(1, rest.size(), "the third is still there; the one not yet due is not");
        assertEquals(Set.copyOf(idsOf(due)),
                Set.copyOf(idsOf(Stream.concat(first.stream(), rest.stream()).toList())));
        assertEquals(TokenStatus.READY, storage.inTx(tx -> tx.findToken(later.id)).orElseThrow().status);
    }

    @Test
    @DisplayName("the claim dispatches only what a worker runs")
    void claimTakesOnlyWorkerDispatchedKinds() {
        Instance i = instance(id("wf"));
        String q = id("q");
        Token task = token(i, NodeKind.TASK, TokenStatus.READY, q);
        task.availableAt = ANCIENT;
        Token predicate = token(i, NodeKind.PREDICATE, TokenStatus.READY, q);
        predicate.availableAt = ANCIENT;
        List<Token> never = new ArrayList<>();
        for (NodeKind kind : List.of(NodeKind.SLEEP, NodeKind.JOIN, NodeKind.SIGNAL, NodeKind.END)) {
            Token t = token(i, kind, TokenStatus.READY, q);
            t.availableAt = ANCIENT;
            never.add(t);
        }
        storage.inTxVoid(tx -> {
            tx.insertInstance(i);
            tx.insertTokens(Stream.concat(
                    Stream.of(task, predicate), never.stream()).toList());
        });

        assertEquals(Set.of(task.id, predicate.id),
                Set.copyOf(idsOf(storage.inTx(tx -> tx.claimTasks("w1", Set.of(q), null, 10, now, now + 1_000)))),
                "TASK and PREDICATE are dispatchable; the server's own kinds never are");
        for (Token t : never) {
            assertEquals(TokenStatus.READY, storage.inTx(tx -> tx.findToken(t.id)).orElseThrow().status);
        }
    }

    @Test
    @DisplayName("concurrent claimers never receive the same token")
    void concurrentClaimsNeverOverlap() throws Exception {
        Instance i = instance(id("wf"));
        String q = id("q");
        List<Token> backlog = new ArrayList<>();
        for (int k = 0; k < 16; k++) {
            Token t = token(i, NodeKind.TASK, TokenStatus.READY, q);
            t.availableAt = ANCIENT + k;
            backlog.add(t);
        }
        storage.inTxVoid(tx -> {
            tx.insertInstance(i);
            tx.insertTokens(backlog);
        });

        List<Token> all = Collections.synchronizedList(new ArrayList<>());
        race(2, () -> all.addAll(storage.inTx(tx ->
                tx.claimTasks("race-" + Thread.currentThread().threadId(), Set.of(q), null, 16,
                        now, now + 30_000))));

        List<String> claimed = idsOf(List.copyOf(all));
        assertEquals(claimed.size(), Set.copyOf(claimed).size(), "no token was handed to both claimers");
        assertEquals(backlog.size(), claimed.size(), "and every token was claimed exactly once");
    }

    // -- the due-work sweeps --

    @Test
    @DisplayName("dueTimers are the waiting sleeps whose time has come")
    void dueTimersAreTheWaitingSleeps() {
        Instance i = instance(id("wf"));
        Token ripe = token(i, NodeKind.SLEEP, TokenStatus.WAITING, null);
        ripe.availableAt = ANCIENT;
        Token later = token(i, NodeKind.SLEEP, TokenStatus.WAITING, null);
        later.availableAt = now + 3_600_000;
        Token notATimer = token(i, NodeKind.SIGNAL, TokenStatus.WAITING, null);
        notATimer.availableAt = ANCIENT;
        Token alreadyWoken = token(i, NodeKind.SLEEP, TokenStatus.READY, null);
        alreadyWoken.availableAt = ANCIENT;
        store(i, ripe, later, notATimer, alreadyWoken);

        // The sweep reaches the whole store, so the assertion is about this test's own rows: the
        // contract fixes which of them are due, not their position among everyone else's.
        List<String> swept = idsOf(storage.inTx(tx -> tx.dueTimers(ANCIENT + 1, 10_000)));
        assertTrue(swept.contains(ripe.id), "a WAITING SLEEP whose fire time has passed");
        assertFalse(swept.contains(later.id), "not one still in the future");
        assertFalse(swept.contains(notATimer.id), "not a signal wait");
        assertFalse(swept.contains(alreadyWoken.id), "not one already promoted");
        assertTrue(storage.inTx(tx -> tx.dueTimers(ANCIENT + 1, 1)).size() <= 1, "max bounds the sweep");
    }

    @Test
    @DisplayName("pendingSignals are the signal waits, oldest first; dueSignals need a deadline")
    void signalWaitsAreListedAndDeadlined() {
        Instance i = instance(id("wf"));
        Token older = token(i, NodeKind.SIGNAL, TokenStatus.AWAITING, null);
        older.createdAt = ANCIENT;
        older.availableAt = 0;
        Token newer = token(i, NodeKind.SIGNAL, TokenStatus.AWAITING, null);
        newer.createdAt = ANCIENT + 1;
        newer.availableAt = ANCIENT;
        Token notWaiting = token(i, NodeKind.SIGNAL, TokenStatus.DONE, null);
        notWaiting.createdAt = ANCIENT + 2;
        store(i, older, newer, notWaiting);

        List<String> ids = List.of(older.id, newer.id, notWaiting.id);
        List<String> pending = storage.inTx(tx -> tx.pendingSignals(10_000)).stream()
                .map(t -> t.id).filter(ids::contains).toList();
        assertEquals(List.of(older.id, newer.id), pending, "oldest first, and only the ones still waiting");

        List<String> due = idsOf(storage.inTx(tx -> tx.dueSignals(ANCIENT + 1, 10_000)));
        assertTrue(due.contains(newer.id), "a deadline that has passed is due");
        assertFalse(due.contains(older.id), "a wait with no deadline never becomes due");
    }

    @Test
    @DisplayName("expiredLeases ignore a token that never took one")
    void expiredLeasesNeedALeaseToExpire() {
        Instance i = instance(id("wf"));
        Token expired = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        expired.leaseOwner = "gone";
        expired.leaseExpiresAt = ANCIENT;
        Token neverLeased = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        neverLeased.leaseExpiresAt = 0;
        Token stillGood = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        stillGood.leaseOwner = "alive";
        stillGood.leaseExpiresAt = now + 3_600_000;
        store(i, expired, neverLeased, stillGood);

        List<String> swept = idsOf(storage.inTx(tx -> tx.expiredLeases(ANCIENT + 1, 10_000)));
        assertTrue(swept.contains(expired.id));
        assertFalse(swept.contains(neverLeased.id), "a zero expiry is no lease, not one long overdue");
        assertFalse(swept.contains(stillGood.id));
    }

    @Test
    @DisplayName("dueSettle is the running observed runs whose settle time has passed, soonest first")
    void dueSettleIsSoonestFirst() {
        String wf = id("wf");
        Instance soon = instance(wf);
        soon.settleAt = ANCIENT;
        Instance later = instance(wf);
        later.settleAt = ANCIENT + 1;
        Instance notObserved = instance(wf);
        Instance finished = instance(wf);
        finished.status = InstanceStatus.COMPLETED;
        finished.settleAt = ANCIENT;
        for (Instance i : List.of(soon, later, notObserved, finished)) store(i);

        List<String> ids = List.of(soon.id, later.id, notObserved.id, finished.id);
        List<String> swept = storage.inTx(tx -> tx.dueSettle(ANCIENT + 2, 10_000)).stream()
                .map(i -> i.id).filter(ids::contains).toList();
        assertEquals(List.of(soon.id, later.id), swept,
                "soonest first; an instance with no settle time and a finished one are not judged");
    }

    @Test
    @DisplayName("the retention pass takes a terminal instance with its tokens and its comp log")
    void retentionTakesTheWholeInstance() {
        String wf = id("wf");
        Instance terminal = instance(wf);
        terminal.status = InstanceStatus.COMPLETED;
        terminal.updatedAt = ANCIENT;
        Token orphanIfLeft = token(terminal, NodeKind.TASK, TokenStatus.DONE, id("q"));
        Instance live = instance(wf);
        live.updatedAt = ANCIENT;
        store(terminal, orphanIfLeft);
        store(live);
        CompLog entry = compEntry(terminal, 1);
        storage.inTxVoid(tx -> tx.appendCompensation(entry));

        assertTrue(storage.inTx(tx -> tx.deleteTerminalInstancesBefore(ANCIENT + 1, 1_000)) >= 1);
        assertTrue(storage.inTx(tx -> tx.findInstance(terminal.id)).isEmpty());
        assertTrue(storage.inTx(tx -> tx.findToken(orphanIfLeft.id)).isEmpty(),
                "a token outliving its instance is a leak");
        assertTrue(storage.inTx(tx -> tx.compensationLog(terminal.id)).isEmpty());
        assertTrue(storage.inTx(tx -> tx.findInstance(live.id)).isPresent(),
                "a RUNNING instance is never terminal, however old");
        assertTrue(storage.inTx(tx -> tx.deleteTerminalInstancesBefore(ANCIENT + 1, 1)) <= 1,
                "the limit bounds one pass");
    }

    // -- schedules --

    @Test
    @DisplayName("a schedule round-trips, is found by workflow, and deletes")
    void scheduleRoundTrips() {
        Schedule s = new Schedule();
        s.id = id("sch");
        s.workflow = id("wf");
        s.intervalMillis = 0;
        s.cron = "0 */5 * * * *";
        s.context = doc("tenant", "acme");
        s.nextFireAt = ANCIENT;
        s.createdAt = now;
        storage.inTxVoid(tx -> tx.putSchedule(s));

        Schedule back = storage.inTx(tx -> tx.scheduleByWorkflow(s.workflow)).orElseThrow();
        assertEquals(s.id, back.id);
        assertEquals(0, back.intervalMillis);
        assertEquals("0 */5 * * * *", back.cron, "a cron cadence survives as text, not as an interval");
        assertEquals(s.context.json(), back.context.json());
        assertEquals(ANCIENT, back.nextFireAt);
        assertEquals(now, back.createdAt);
        assertTrue(storage.inTx(tx -> tx.schedules()).stream().anyMatch(x -> x.id.equals(s.id)));

        s.intervalMillis = 60_000;
        s.cron = null;
        storage.inTxVoid(tx -> tx.putSchedule(s));
        Schedule replaced = storage.inTx(tx -> tx.scheduleByWorkflow(s.workflow)).orElseThrow();
        assertEquals(60_000, replaced.intervalMillis, "putSchedule upserts by id");
        assertNull(replaced.cron);

        storage.inTxVoid(tx -> tx.deleteSchedule(s.id));
        assertTrue(storage.inTx(tx -> tx.scheduleByWorkflow(s.workflow)).isEmpty());
    }

    @Test
    @DisplayName("dueSchedules are soonest first, and claimSchedule is a compare-and-set")
    void claimScheduleIsACompareAndSet() {
        Schedule soon = schedule(ANCIENT);
        Schedule later = schedule(ANCIENT + 1);
        Schedule future = schedule(now + 3_600_000);
        for (Schedule s : List.of(soon, later, future)) storage.inTxVoid(tx -> tx.putSchedule(s));

        List<String> ids = List.of(soon.id, later.id, future.id);
        assertEquals(List.of(soon.id, later.id),
                storage.inTx(tx -> tx.dueSchedules(ANCIENT + 2, 10_000)).stream()
                        .map(s -> s.id).filter(ids::contains).toList(),
                "soonest first, and nothing not yet due");

        assertTrue(inTxBoolean(tx -> tx.claimSchedule(soon.id, ANCIENT, now + 60_000)),
                "the leader that reads the current fire time wins it");
        assertFalse(inTxBoolean(tx -> tx.claimSchedule(soon.id, ANCIENT, now + 60_000)),
                "and an overlapping leader holding the same stale time cannot fire it twice");
        assertEquals(now + 60_000, storage.inTx(tx -> tx.scheduleByWorkflow(soon.workflow)).orElseThrow().nextFireAt);
        assertFalse(inTxBoolean(tx -> tx.claimSchedule(id("sch"), ANCIENT, now)), "an unknown schedule is not claimed");
    }

    private Schedule schedule(long nextFireAt) {
        Schedule s = new Schedule();
        s.id = id("sch");
        s.workflow = id("wf");
        s.intervalMillis = 60_000;
        s.context = Doc.EMPTY;
        s.nextFireAt = nextFireAt;
        s.createdAt = now;
        return s;
    }

    // -- what lag monitoring and the console read --

    @Test
    @DisplayName("queueDepth counts the dispatchable work that is due")
    void queueDepthCountsDueDispatchableWork() {
        long asOf = ANCIENT + 1_000;
        Rows.QueueDepth before = storage.inTx(tx -> tx.queueDepth(asOf));

        Instance i = instance(id("wf"));
        String q = id("q");
        Token a = token(i, NodeKind.TASK, TokenStatus.READY, q);
        a.availableAt = ANCIENT;
        Token b = token(i, NodeKind.PREDICATE, TokenStatus.READY, q);
        b.availableAt = ANCIENT + 1;
        Token leased = token(i, NodeKind.TASK, TokenStatus.RUNNING, q);
        leased.availableAt = ANCIENT;
        Token future = token(i, NodeKind.TASK, TokenStatus.READY, q);
        future.availableAt = now + 3_600_000;
        store(i, a, b, leased, future);

        Rows.QueueDepth after = storage.inTx(tx -> tx.queueDepth(asOf));
        assertEquals(before.readyCount() + 2, after.readyCount(),
                "the two READY and due ones; not the leased one and not the one still in the future");
        assertTrue(after.oldestAvailableAt() > 0 && after.oldestAvailableAt() <= ANCIENT,
                "and the oldest of them, " + after.oldestAvailableAt());
    }

    @Test
    @DisplayName("backlogByVersion slices the backlog by workflow, version and queue")
    void backlogByVersionSlicesTheBacklog() {
        Instance i = instance(id("wf"));
        String q = id("q");
        for (int k = 0; k < 3; k++) {
            Token t = token(i, NodeKind.TASK, TokenStatus.READY, q);
            t.availableAt = ANCIENT + k;
            store(k == 0 ? i : instance(i.workflow), t);
        }

        List<Rows.BacklogSlice> mine = storage.inTx(tx -> tx.backlogByVersion(ANCIENT + 1_000, 10_000)).stream()
                .filter(s -> s.workflow().equals(i.workflow))
                .toList();
        assertEquals(1, mine.size(), "one slice: one workflow, one version, one queue");
        assertEquals(q, mine.getFirst().queue());
        assertEquals(1, mine.getFirst().version());
        assertEquals(3, mine.getFirst().readyCount());
        assertEquals(ANCIENT, mine.getFirst().oldestAvailableAt());
    }

    @Test
    @DisplayName("countProcessedSince counts the worker steps that settled after the mark")
    void countProcessedSinceCountsSettledWorkerSteps() {
        long since = FUTURE - 1;
        int before = inTxInt(tx -> tx.countProcessedSince(since));

        Instance i = instance(id("wf"));
        Token done = token(i, NodeKind.TASK, TokenStatus.DONE, id("q"));
        done.updatedAt = FUTURE;
        Token predicateDone = token(i, NodeKind.PREDICATE, TokenStatus.DONE, id("q"));
        predicateDone.updatedAt = FUTURE;
        Token stillRunning = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        stillRunning.updatedAt = FUTURE;
        Token serverSide = token(i, NodeKind.SLEEP, TokenStatus.DONE, null);
        serverSide.updatedAt = FUTURE;
        Token beforeTheMark = token(i, NodeKind.TASK, TokenStatus.DONE, id("q"));
        beforeTheMark.updatedAt = since;
        store(i, done, predicateDone, stillRunning, serverSide, beforeTheMark);

        assertEquals(before + 2, inTxInt(tx -> tx.countProcessedSince(since)),
                "worker-dispatched kinds only, DONE only, and strictly after the mark");
    }

    @Test
    @DisplayName("stepDurations are newest first, and a step that was never queued waited for nothing")
    void stepDurationsMeasureRunAndWait() {
        Instance i = instance(id("wf"));
        Token dispatched = token(i, NodeKind.TASK, TokenStatus.DONE, id("q"));
        dispatched.nodeId = "n-dispatched";
        dispatched.availableAt = 1_000;
        dispatched.startedAt = 1_200L;
        dispatched.finishedAt = 1_500L;
        Token observed = token(i, NodeKind.TASK, TokenStatus.DONE, id("q"));
        observed.nodeId = "n-observed";
        observed.availableAt = 1_000;
        observed.startedAt = 2_000L;
        observed.finishedAt = 2_050L;
        observed.seq = 1L;
        Token unfinished = token(i, NodeKind.TASK, TokenStatus.RUNNING, id("q"));
        unfinished.nodeId = "n-running";
        unfinished.startedAt = 3_000L;
        store(i, dispatched, observed, unfinished);

        List<Rows.StepDuration> steps = storage.inTx(tx -> tx.stepDurations(i.workflow, 1, 0, 10));
        assertEquals(List.of("n-observed", "n-dispatched"), steps.stream().map(Rows.StepDuration::nodeId).toList(),
                "newest first by finish time, and nothing still running");
        assertEquals(50, steps.getFirst().millis());
        assertEquals(0, steps.getFirst().waitMillis(), "a reported step was never dispatched from a queue");
        assertEquals(300, steps.getLast().millis());
        assertEquals(200, steps.getLast().waitMillis(), "ready to claimed");
        assertTrue(storage.inTx(tx -> tx.stepDurations(i.workflow, 1, 1_600, 10)).size() == 1,
                "the sample is bounded below by the mark");
        assertEquals(1, storage.inTx(tx -> tx.stepDurations(i.workflow, 1, 0, 1)).size(), "and above by max");
        assertTrue(storage.inTx(tx -> tx.stepDurations(i.workflow, 2, 0, 10)).isEmpty(), "one version at a time");
    }

    // -- shards --

    @Test
    @DisplayName("a database is claimed for one shard, and a later claim leaves the first standing")
    void aShardIdentityIsClaimedOnce() {
        storage.inTxVoid(tx -> tx.claimShardIdentity(7));
        int claimed = storage.inTx(tx -> tx.shardIdentity()).orElseThrow();   // 7, or an earlier run's
        storage.inTxVoid(tx -> tx.claimShardIdentity(claimed + 1));
        assertEquals(claimed, storage.inTx(tx -> tx.shardIdentity()).orElseThrow());
    }

    @Test
    @DisplayName("the shard registry keeps one row per shard, replaced in place")
    void theShardRegistryKeepsOneRowPerShard() {
        int shard = (int) (System.nanoTime() & 0x3fffffff);
        storage.inTxVoid(tx -> tx.putShardRecord(new Rows.ShardRecord(shard, ShardState.ACTIVE, ANCIENT, null)));
        storage.inTxVoid(tx -> tx.putShardRecord(new Rows.ShardRecord(shard, ShardState.RETIRED, ANCIENT, ANCIENT + 9)));
        List<Rows.ShardRecord> rows = storage.inTx(tx -> tx.shardRegistry()).stream()
                .filter(r -> r.shardId() == shard).toList();
        assertEquals(List.of(new Rows.ShardRecord(shard, ShardState.RETIRED, ANCIENT, ANCIENT + 9L)), rows);
        storage.inTxVoid(tx -> tx.putShardRecord(new Rows.ShardRecord(shard, ShardState.ACTIVE, ANCIENT, null)));
        assertEquals(List.of(new Rows.ShardRecord(shard, ShardState.ACTIVE, ANCIENT, null)),
                storage.inTx(tx -> tx.shardRegistry()).stream().filter(r -> r.shardId() == shard).toList(),
                "a null retiredAt reads back null, not zero");
    }

    // -- cluster membership --

    @Test
    @DisplayName("a node row carries the topology generation the node runs, updated with each heartbeat")
    void aNodeCarriesItsTopologyGeneration() {
        ServerNode n = serverNode(ANCIENT);
        n.topologyGeneration = 3;
        storage.inTxVoid(tx -> tx.upsertNode(n));
        assertEquals(3, node(n.id).orElseThrow().topologyGeneration);
        n.topologyGeneration = 4;
        storage.inTxVoid(tx -> tx.upsertNode(n));
        assertEquals(4, node(n.id).orElseThrow().topologyGeneration);
        storage.inTxVoid(tx -> tx.deleteNodesOlderThan(ANCIENT + 1));
    }

    @Test
    @DisplayName("a node keeps the heartbeat it first registered with, and the leader flag is set apart")
    void nodeMembershipKeepsTheFirstHeartbeat() {
        // The one that joined first is also the one that stops heartbeating, so the sweep at the
        // end has a cutoff that separates them.
        ServerNode first = serverNode(ANCIENT);
        ServerNode second = serverNode(ANCIENT + 1_000);
        storage.inTxVoid(tx -> tx.upsertNode(first));
        storage.inTxVoid(tx -> tx.upsertNode(second));

        ServerNode again = serverNode(ANCIENT + 5);
        again.id = first.id;
        again.name = "renamed";
        again.workers = 9;
        storage.inTxVoid(tx -> tx.upsertNode(again));

        ServerNode back = node(first.id).orElseThrow();
        assertEquals(ANCIENT, back.firstHeartbeat, "the first heartbeat is when this node joined, once");
        assertEquals(ANCIENT + 5, back.lastHeartbeat);
        assertEquals("renamed", back.name);
        assertEquals(9, back.workers);
        assertFalse(back.leader);

        storage.inTxVoid(tx -> tx.setLeader(first.id, true));
        assertTrue(node(first.id).orElseThrow().leader);
        assertFalse(node(second.id).orElseThrow().leader, "leadership is set on one node, not swapped");

        List<String> ids = List.of(first.id, second.id);
        assertEquals(ids, storage.inTx(tx -> tx.nodes()).stream()
                        .map(n -> n.id).filter(ids::contains).toList(),
                "listed by the order they joined");

        storage.inTxVoid(tx -> tx.deleteNodesOlderThan(ANCIENT + 6));
        assertTrue(node(first.id).isEmpty(), "a node that stopped heartbeating is dropped");
        assertTrue(node(second.id).isPresent(), "and one still heartbeating is not");
        storage.inTxVoid(tx -> tx.deleteNodesOlderThan(ANCIENT + 1_001));
    }

    private ServerNode serverNode(long heartbeat) {
        ServerNode n = new ServerNode();
        n.id = id("node");
        n.name = "contract-" + n.id;
        n.firstHeartbeat = heartbeat;
        n.lastHeartbeat = heartbeat;
        n.workers = 1;
        return n;
    }

    private Optional<ServerNode> node(String id) {
        return storage.inTx(tx -> tx.nodes()).stream().filter(n -> n.id.equals(id)).findFirst();
    }

    // -- the compensation log --

    @Test
    @DisplayName("the compensation log is seq-ordered, and one entry settles at a time")
    void compensationLogIsSeqOrdered() {
        Instance i = instance(id("wf"));
        store(i);
        CompLog one = compEntry(i, 1);
        CompLog two = compEntry(i, 2);
        storage.inTxVoid(tx -> {
            tx.appendCompensation(two);
            tx.appendCompensation(one);
        });

        List<CompLog> log = storage.inTx(tx -> tx.compensationLog(i.id));
        assertEquals(List.of(1L, 2L), log.stream().map(e -> e.seq).toList(),
                "seq order, whatever order they were appended in: the reverse pass walks it backwards");
        CompLog back = log.getFirst();
        assertEquals("n-1", back.nodeId);
        assertEquals(i.workflow + "#undo-1", back.activity);
        assertEquals("q-1", back.queue);
        assertEquals(doc("before", 1).json(), back.input.json());
        assertEquals(doc("after", 1).json(), back.result.json());
        assertFalse(back.compensated);

        storage.inTxVoid(tx -> tx.markCompensated(i.id, 2));
        List<CompLog> settled = storage.inTx(tx -> tx.compensationLog(i.id));
        assertFalse(settled.get(0).compensated, "only the entry named is settled");
        assertTrue(settled.get(1).compensated);
        assertTrue(storage.inTx(tx -> tx.compensationLog(id("wfi"))).isEmpty());
    }

    private static CompLog compEntry(Instance of, long seq) {
        CompLog e = new CompLog();
        e.instanceId = of.id;
        e.seq = seq;
        e.nodeId = "n-" + seq;
        e.activity = of.workflow + "#undo-" + seq;
        e.queue = "q-" + seq;
        e.input = doc("before", seq);
        e.result = doc("after", seq);
        return e;
    }

    // -- observed-run anomalies --

    @Test
    @DisplayName("anomalies are newest first, and narrowed by workflow or by instance")
    void anomaliesAreNewestFirstAndFilterable() {
        String wf = id("wf");
        Instance a = instance(wf);
        Instance b = instance(wf);
        store(a);
        store(b);
        Rows.Anomaly older = anomaly(a, "UNEXPECTED_STEP", ANCIENT);
        Rows.Anomaly newer = anomaly(b, "SKIPPED_STEP", ANCIENT + 1);
        storage.inTxVoid(tx -> {
            tx.insertAnomaly(older);
            tx.insertAnomaly(newer);
        });

        assertEquals(List.of(newer.id(), older.id()),
                storage.inTx(tx -> tx.anomalies(wf, null, 10)).stream().map(Rows.Anomaly::id).toList(),
                "newest first");
        assertEquals(List.of(older.id()),
                storage.inTx(tx -> tx.anomalies(null, a.id, 10)).stream().map(Rows.Anomaly::id).toList(),
                "one instance's departures");
        assertEquals(List.of(newer.id()),
                storage.inTx(tx -> tx.anomalies(wf, null, 1)).stream().map(Rows.Anomaly::id).toList(),
                "the limit keeps the newest");

        Rows.Anomaly back = storage.inTx(tx -> tx.anomalies(null, b.id, 10)).getFirst();
        assertEquals("SKIPPED_STEP", back.kind());
        assertEquals("n-expected", back.expectedNode());
        assertEquals("n-reported", back.reportedNode());
        assertEquals("reported out of order", back.detail());
        assertEquals(ANCIENT + 1, back.at());
        assertEquals(1, back.version());
    }

    private Rows.Anomaly anomaly(Instance of, String kind, long at) {
        return new Rows.Anomaly(id("anom"), of.id, of.workflow, of.version, kind,
                "n-expected", "n-reported", "reported out of order", at);
    }

    // -- WGL-STOR-005: the store's own identity --

    @Test
    @DisplayName("the fingerprint is a stable identity, or absent where there is none to give")
    void fingerprintIsStableOrAbsent() {
        String fp = storage.fingerprint();
        if (fp == null) {
            return;   // in-memory: no cross-node identity exists
        }
        assertFalse(fp.isBlank(), "a fingerprint is a value or it is null, never blank");
        assertEquals(fp, storage.fingerprint(), "and it does not change under a running node");
    }
}
