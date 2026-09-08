package com.wiggle.coordinator.ratis;

import com.wiggle.core.Json;
import com.wiggle.server.coord.EpochCodec;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The replicated state machine: it applies {@link CoordCommand}s to {@link RocksKv} deterministically
 * (every timestamp is a command arg, never a wall-clock read here) and serves reads via {@link #query}.
 *
 * <p><b>RocksDB is the snapshot.</b> The applied {@link TermIndex} is staged into the <em>same
 * WriteBatch</em> as each command's writes ({@link RocksKv#APPLIED}), so RocksDB always recovers to a
 * batch boundary where state and applied-position agree — even after a crash that loses an unsynced WAL
 * suffix (the Raft log simply redelivers those entries). On boot, {@link #getLatestSnapshot} reports that
 * persisted position, so Ratis resumes applying at the next index instead of replaying the whole log; and
 * {@link #takeSnapshot} is just a RocksDB flush (no checkpoint, no file copy) that advances the reported
 * position so the Raft log can compact. What remains unwired is <em>snapshot file shipping</em>
 * (InstallSnapshot to seed a brand-new member) — see docs/coordinator-ratis-rocksdb.md §5.
 */
public final class CoordStateMachine extends BaseStateMachine {

    private RocksKv kv;   // opened over the state-machine data dir in initialize(...)

    /**
     * The position {@link #getLatestSnapshot} reports: the persisted applied index at boot, advanced
     * afterwards only by {@link #takeSnapshot}. Deliberately NOT the live applied index — Ratis treats
     * the snapshot position as a log-compaction fence, so it must move only when the state below it is
     * durably flushed.
     */
    private volatile TermIndex snapshotPosition;

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        File smDir = raftStorage.getStorageDir().getStateMachineDir();
        this.kv = RocksKv.open(new File(smDir, "rocksdb"));
        restore();
    }

    /** The RELOAD path (snapshot install / reset): re-read the applied position from RocksDB. */
    @Override
    public void reinitialize() {
        restore();
    }

    private void restore() {
        TermIndex applied = kv.get(RocksKv.APPLIED).map(CoordStateMachine::decodeTermIndex).orElse(null);
        if (applied != null) setLastAppliedTermIndex(applied);
        this.snapshotPosition = applied;
    }

    @Override
    public void close() {
        if (kv != null) kv.close();
    }

    // ---- writes: go through the Raft log, applied in index order on every replica ----
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        LogEntryProto entry = trx.getLogEntry();
        CoordCommand cmd;
        try {
            cmd = CoordCommand.decode(trx.getStateMachineLogEntry().getLogData().toByteArray());
        } catch (RuntimeException e) {
            // Decoding is a pure function of the log bytes, so it fails identically on every replica:
            // record the entry as applied and reply ok=false instead of throwing — a throw here would
            // kill the StateMachineUpdater and stall the division on a poison entry forever.
            return complete(entry, kv.batch(), new CoordCommand.Result(false, "unreadable command: " + e.getMessage()));
        }
        // A failure below is an environment fault (RocksDB), not a command property: let it propagate.
        // Ratis stops applying on this node (fail-stop), which beats diverging from the other replicas.
        RocksKv.Batch batch = kv.batch();
        CoordCommand.Result result = switch (cmd.op()) {
            case CAS_POLICY            -> casPolicy(cmd.args(), batch);
            case UPSERT_NODE           -> upsertNode(cmd.args(), batch);
            case TOUCH_NODE            -> touchNode(cmd.args(), batch);
            case REMOVE_NODE           -> removeNode(cmd.args(), batch);
            case EXPIRE_NODES          -> expireNodes(cmd.args(), batch);
            case BIND_CELL             -> bindCell(cmd.args(), batch);
            case PRUNE_ORPHAN_BINDINGS -> pruneOrphanBindings(batch);
            case PUT_DEFINITION        -> putDefinition(cmd.args(), batch);
            case REMOVE_DEFINITION     -> removeDefinition(cmd.args(), batch);
            case PUT_NAMESPACE         -> putNamespace(cmd.args(), batch);
            case ACQUIRE_LEADERSHIP    -> acquireLeadership(cmd.args(), batch);
            case RELEASE_LEADERSHIP    -> releaseLeadership(cmd.args(), batch);
            default                    -> new CoordCommand.Result(false, "not a write op: " + cmd.op());
        };
        return complete(entry, batch, result);
    }

    /**
     * Stage the applied index into the SAME batch as the command's writes and commit once — the atomic
     * (state, index) pair that makes restart resume (and crash recovery) exact.
     */
    private CompletableFuture<Message> complete(LogEntryProto entry, RocksKv.Batch batch, CoordCommand.Result result) {
        batch.put(RocksKv.APPLIED, encodeTermIndex(entry.getTerm(), entry.getIndex())).commit();
        updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
        return CompletableFuture.completedFuture(reply(result));
    }

    // ---- reads: served against local applied state; linearizability is the server's read option ----
    @Override
    public CompletableFuture<Message> query(Message request) {
        CoordCommand cmd = CoordCommand.decode(request.getContent().toByteArray());
        Object value = switch (cmd.op()) {
            case GET_POLICY       -> kv.get(RocksKv.POLICY + arg(cmd, "ns")).orElse(null);
            case LIST_POLICIES    -> kv.values(RocksKv.POLICY);
            case GET_NODE         -> kv.get(RocksKv.NODE + arg(cmd, "id")).orElse(null);
            case LIST_NODES       -> nodesInNamespace(arg(cmd, "ns"));
            case GET_DEFINITION   -> kv.get(RocksKv.DEF + arg(cmd, "ns") + "/" + arg(cmd, "name")).orElse(null);
            case LIST_DEFINITIONS -> kv.values(RocksKv.DEF + arg(cmd, "ns") + "/");
            case GET_NAMESPACE    -> kv.get(RocksKv.NS + arg(cmd, "ns")).orElse(null);
            case LIST_NAMESPACES  -> kv.values(RocksKv.NS);
            default -> throw new IllegalStateException("not a read op: " + cmd.op());
        };
        return CompletableFuture.completedFuture(reply(new CoordCommand.Result(true, value)));
    }

    // ---- writes -------------------------------------------------------------------------------

    /** CAS on the policy revision -- the version check lives here, atomic because the leader serializes. */
    private CoordCommand.Result casPolicy(Map<String, Object> a, RocksKv.Batch batch) {
        String ns = str(a, "ns");
        long expected = num(a, "expectedRevision");
        String key = RocksKv.POLICY + ns;
        Optional<String> current = kv.get(key);
        boolean match = expected == 0 ? current.isEmpty()
                : current.map(CoordCodec::policyRevision).orElse(-1L) == expected;
        if (!match) return new CoordCommand.Result(true, -1L);          // CAS lost
        long next = expected == 0 ? 1 : expected + 1;
        String blob = CoordCodec.encodePolicy(ns, num(a, "currentEpoch"), next,
                EpochCodec.decode(str(a, "epochs")));
        batch.put(key, blob);
        return new CoordCommand.Result(true, next);                     // the new revision
    }

    private CoordCommand.Result upsertNode(Map<String, Object> a, RocksKv.Batch batch) {
        String json = str(a, "node");
        String id = Json.reqStr(Json.parseObject(json), "id");
        batch.put(RocksKv.NODE + id, json);
        return new CoordCommand.Result(true, null);
    }

    /** Update a node's heartbeat + observed generation; returns the updated node blob, or "" if unknown. */
    private CoordCommand.Result touchNode(Map<String, Object> a, RocksKv.Batch batch) {
        String id = str(a, "id");
        Optional<String> existing = kv.get(RocksKv.NODE + id);
        if (existing.isEmpty()) return new CoordCommand.Result(true, "");
        var n = CoordCodec.decodeNode(existing.get());
        var updated = new com.wiggle.server.coord.CoordNode(n.id(), n.namespace(), n.cellId(), n.endpoint(),
                n.region(), n.engineVersion(), n.cellFingerprint(), num(a, "configGeneration"), num(a, "lastHeartbeat"));
        String json = CoordCodec.encodeNode(updated);
        batch.put(RocksKv.NODE + id, json);
        return new CoordCommand.Result(true, json);
    }

    private CoordCommand.Result removeNode(Map<String, Object> a, RocksKv.Batch batch) {
        batch.delete(RocksKv.NODE + str(a, "id"));
        return new CoordCommand.Result(true, null);
    }

    /** Remove roster nodes whose lastHeartbeat < deadline (deadline is a command arg -> deterministic). */
    private CoordCommand.Result expireNodes(Map<String, Object> a, RocksKv.Batch batch) {
        long deadline = num(a, "deadlineMillis");
        int removed = 0;
        for (RocksKv.Kv e : kv.entries(RocksKv.NODE)) {
            if (Json.num(Json.parseObject(e.value()), "lastHeartbeat", 0) < deadline) {
                batch.delete(e.key());
                removed++;
            }
        }
        return new CoordCommand.Result(true, (long) removed);
    }

    /** Claim (ns, cell) -> fingerprint if absent; else succeed only if the same fingerprint holds it. */
    private CoordCommand.Result bindCell(Map<String, Object> a, RocksKv.Batch batch) {
        String key = RocksKv.CELL + str(a, "ns") + "/" + str(a, "cellId");
        String fp = str(a, "fingerprint");
        Optional<String> held = kv.get(key);
        if (held.isEmpty()) { batch.put(key, fp); return new CoordCommand.Result(true, true); }
        return new CoordCommand.Result(true, fp.equals(held.get()));    // replica -> true; other cell -> false
    }

    /** Delete cell bindings that no live node references any more. Returns the count pruned. */
    private CoordCommand.Result pruneOrphanBindings(RocksKv.Batch batch) {
        Set<String> liveCells = new HashSet<>();
        for (String nodeJson : kv.values(RocksKv.NODE)) {
            var n = CoordCodec.decodeNode(nodeJson);
            liveCells.add(n.namespace() + "/" + n.cellId());
        }
        int pruned = 0;
        for (String cellKey : kv.scan(RocksKv.CELL)) {
            if (!liveCells.contains(cellKey.substring(RocksKv.CELL.length()))) { batch.delete(cellKey); pruned++; }
        }
        return new CoordCommand.Result(true, (long) pruned);
    }

    private CoordCommand.Result putDefinition(Map<String, Object> a, RocksKv.Batch batch) {
        String json = str(a, "def");
        var m = Json.parseObject(json);
        batch.put(RocksKv.DEF + Json.reqStr(m, "namespace") + "/" + Json.reqStr(m, "name"), json);
        return new CoordCommand.Result(true, null);
    }

    private CoordCommand.Result removeDefinition(Map<String, Object> a, RocksKv.Batch batch) {
        String key = RocksKv.DEF + str(a, "ns") + "/" + str(a, "name");
        boolean existed = kv.get(key).isPresent();
        if (existed) batch.delete(key);
        return new CoordCommand.Result(true, existed);
    }

    private CoordCommand.Result putNamespace(Map<String, Object> a, RocksKv.Batch batch) {
        String json = str(a, "ns");
        batch.put(RocksKv.NS + Json.reqStr(Json.parseObject(json), "namespace"), json);
        return new CoordCommand.Result(true, null);
    }

    /** Lease-based leadership: nowMillis is a command arg, so apply stays deterministic. */
    private CoordCommand.Result acquireLeadership(Map<String, Object> a, RocksKv.Batch batch) {
        String nodeId = str(a, "nodeId");
        long now = num(a, "nowMillis");
        long lease = num(a, "leaseMillis");
        Optional<String> held = kv.get(RocksKv.LEADER);
        String holder = held.map(v -> Json.str(Json.parseObject(v), "holder", null)).orElse(null);
        long expiresAt = held.map(v -> Json.num(Json.parseObject(v), "expiresAt", 0)).orElse(0L);
        boolean canTake = holder == null || nodeId.equals(holder) || expiresAt <= now;
        if (!canTake) return new CoordCommand.Result(true, false);
        batch.put(RocksKv.LEADER, Json.write(Map.of("holder", nodeId, "expiresAt", now + lease)));
        return new CoordCommand.Result(true, true);
    }

    private CoordCommand.Result releaseLeadership(Map<String, Object> a, RocksKv.Batch batch) {
        String nodeId = str(a, "nodeId");
        Optional<String> held = kv.get(RocksKv.LEADER);
        String holder = held.map(v -> Json.str(Json.parseObject(v), "holder", null)).orElse(null);
        if (nodeId.equals(holder)) batch.delete(RocksKv.LEADER);
        return new CoordCommand.Result(true, null);
    }

    // ---- read helpers ----

    private List<String> nodesInNamespace(String ns) {
        List<String> out = new ArrayList<>();
        for (String nodeJson : kv.values(RocksKv.NODE)) {
            if (CoordCodec.decodeNode(nodeJson).namespace().equals(ns)) out.add(nodeJson);
        }
        return out;
    }

    // ---- snapshot: RocksDB is the snapshot -----------------------------------------------------

    /**
     * Flush RocksDB so everything applied so far is durable in the SST files (no WAL dependence), then
     * advance the reported snapshot position — the Raft log up to it becomes purgeable.
     */
    @Override
    public long takeSnapshot() {
        TermIndex last = getLastAppliedTermIndex();
        if (last == null || last.getIndex() < 0) return RaftLog.INVALID_LOG_INDEX;
        kv.flush();
        this.snapshotPosition = last;
        return last.getIndex();
    }

    /**
     * The persisted applied position doubles as the snapshot info: on boot Ratis resumes applying right
     * after it instead of replaying the whole log. No files are listed — shipping state to a brand-new
     * member is the documented remaining gap (seed it by copying the rocksdb dir; see the doc §5).
     */
    @Override
    public SnapshotInfo getLatestSnapshot() {
        TermIndex ti = snapshotPosition;
        if (ti == null || ti.getIndex() < 0) return null;
        return new SnapshotInfo() {
            @Override public TermIndex getTermIndex() { return ti; }
            @Override public List<FileInfo> getFiles() { return List.of(); }
        };
    }

    // ---- small arg + reply helpers ----
    private static String encodeTermIndex(long term, long index) {
        return Json.write(Map.of("term", term, "index", index));
    }
    private static TermIndex decodeTermIndex(String json) {
        Map<String, Object> m = Json.parseObject(json);
        return TermIndex.valueOf(Json.num(m, "term", 0), Json.num(m, "index", -1));
    }
    private static Message reply(CoordCommand.Result r) {
        return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(r.encode()));
    }
    private static String arg(CoordCommand c, String k) { return (String) c.args().get(k); }
    private static String str(Map<String, Object> a, String k) { return (String) a.get(k); }
    private static long num(Map<String, Object> a, String k) { return ((Number) a.get(k)).longValue(); }
}
