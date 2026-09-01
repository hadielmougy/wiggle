package com.wiggle.coordinator.ratis;

import com.wiggle.core.Json;
import com.wiggle.server.coord.EpochCodec;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;

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
 * RocksDB is opened under the Ratis state-machine directory in {@link #initialize}. Snapshots are RocksDB
 * checkpoints (§5); snapshot <em>install/restore</em> is not yet wired, so a durable restart replays the
 * log from the beginning — fine for a single-member dev group, revisited when multi-node restore lands.
 * See docs/coordinator-ratis-rocksdb.md §2/§5.
 */
public final class CoordStateMachine extends BaseStateMachine {

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private RocksKv kv;   // opened over the state-machine data dir in initialize(...)

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        File smDir = raftStorage.getStorageDir().getStateMachineDir();
        this.kv = RocksKv.open(new File(smDir, "rocksdb"));
    }

    @Override
    public void close() {
        if (kv != null) kv.close();
    }

    // ---- writes: go through the Raft log, applied in index order on every replica ----
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        CoordCommand cmd = CoordCommand.decode(trx.getStateMachineLogEntry().getLogData().toByteArray());
        CoordCommand.Result result = switch (cmd.op()) {
            case CAS_POLICY            -> casPolicy(cmd.args());
            case UPSERT_NODE           -> upsertNode(cmd.args());
            case TOUCH_NODE            -> touchNode(cmd.args());
            case REMOVE_NODE           -> removeNode(cmd.args());
            case EXPIRE_NODES          -> expireNodes(cmd.args());
            case BIND_CELL             -> bindCell(cmd.args());
            case PRUNE_ORPHAN_BINDINGS -> pruneOrphanBindings();
            case PUT_DEFINITION        -> putDefinition(cmd.args());
            case REMOVE_DEFINITION     -> removeDefinition(cmd.args());
            case PUT_NAMESPACE         -> putNamespace(cmd.args());
            case ACQUIRE_LEADERSHIP    -> acquireLeadership(cmd.args());
            case RELEASE_LEADERSHIP    -> releaseLeadership(cmd.args());
            default -> throw new IllegalStateException("not a write op: " + cmd.op());
        };
        return CompletableFuture.completedFuture(reply(result));
    }

    // ---- reads: linearizable query (Ratis routes read-index to the leader) ----
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
    private CoordCommand.Result casPolicy(Map<String, Object> a) {
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
        kv.batch().put(key, blob).commit();
        return new CoordCommand.Result(true, next);                     // the new revision
    }

    private CoordCommand.Result upsertNode(Map<String, Object> a) {
        String json = str(a, "node");
        String id = Json.reqStr(Json.parseObject(json), "id");
        kv.batch().put(RocksKv.NODE + id, json).commit();
        return new CoordCommand.Result(true, null);
    }

    /** Update a node's heartbeat + observed generation; returns the updated node blob, or "" if unknown. */
    private CoordCommand.Result touchNode(Map<String, Object> a) {
        String id = str(a, "id");
        Optional<String> existing = kv.get(RocksKv.NODE + id);
        if (existing.isEmpty()) return new CoordCommand.Result(true, "");
        var n = CoordCodec.decodeNode(existing.get());
        var updated = new com.wiggle.server.coord.CoordNode(n.id(), n.namespace(), n.cellId(), n.endpoint(),
                n.region(), n.engineVersion(), n.cellFingerprint(), num(a, "configGeneration"), num(a, "lastHeartbeat"));
        String json = CoordCodec.encodeNode(updated);
        kv.batch().put(RocksKv.NODE + id, json).commit();
        return new CoordCommand.Result(true, json);
    }

    private CoordCommand.Result removeNode(Map<String, Object> a) {
        kv.batch().delete(RocksKv.NODE + str(a, "id")).commit();
        return new CoordCommand.Result(true, null);
    }

    /** Remove roster nodes whose lastHeartbeat < deadline (deadline is a command arg -> deterministic). */
    private CoordCommand.Result expireNodes(Map<String, Object> a) {
        long deadline = num(a, "deadlineMillis");
        int removed = 0;
        RocksKv.Batch batch = kv.batch();
        for (RocksKv.Kv e : kv.entries(RocksKv.NODE)) {
            if (Json.num(Json.parseObject(e.value()), "lastHeartbeat", 0) < deadline) {
                batch.delete(e.key());
                removed++;
            }
        }
        batch.commit();
        return new CoordCommand.Result(true, (long) removed);
    }

    /** Claim (ns, cell) -> fingerprint if absent; else succeed only if the same fingerprint holds it. */
    private CoordCommand.Result bindCell(Map<String, Object> a) {
        String key = RocksKv.CELL + str(a, "ns") + "/" + str(a, "cellId");
        String fp = str(a, "fingerprint");
        Optional<String> held = kv.get(key);
        if (held.isEmpty()) { kv.batch().put(key, fp).commit(); return new CoordCommand.Result(true, true); }
        return new CoordCommand.Result(true, fp.equals(held.get()));    // replica -> true; other cell -> false
    }

    /** Delete cell bindings that no live node references any more. Returns the count pruned. */
    private CoordCommand.Result pruneOrphanBindings() {
        Set<String> liveCells = new HashSet<>();
        for (String nodeJson : kv.values(RocksKv.NODE)) {
            var n = CoordCodec.decodeNode(nodeJson);
            liveCells.add(n.namespace() + "/" + n.cellId());
        }
        int pruned = 0;
        RocksKv.Batch batch = kv.batch();
        for (String cellKey : kv.scan(RocksKv.CELL)) {
            if (!liveCells.contains(cellKey.substring(RocksKv.CELL.length()))) { batch.delete(cellKey); pruned++; }
        }
        batch.commit();
        return new CoordCommand.Result(true, (long) pruned);
    }

    private CoordCommand.Result putDefinition(Map<String, Object> a) {
        String json = str(a, "def");
        var m = Json.parseObject(json);
        kv.batch().put(RocksKv.DEF + Json.reqStr(m, "namespace") + "/" + Json.reqStr(m, "name"), json).commit();
        return new CoordCommand.Result(true, null);
    }

    private CoordCommand.Result removeDefinition(Map<String, Object> a) {
        String key = RocksKv.DEF + str(a, "ns") + "/" + str(a, "name");
        boolean existed = kv.get(key).isPresent();
        if (existed) kv.batch().delete(key).commit();
        return new CoordCommand.Result(true, existed);
    }

    private CoordCommand.Result putNamespace(Map<String, Object> a) {
        String json = str(a, "ns");
        kv.batch().put(RocksKv.NS + Json.reqStr(Json.parseObject(json), "namespace"), json).commit();
        return new CoordCommand.Result(true, null);
    }

    /** Lease-based leadership: nowMillis is a command arg, so apply stays deterministic. */
    private CoordCommand.Result acquireLeadership(Map<String, Object> a) {
        String nodeId = str(a, "nodeId");
        long now = num(a, "nowMillis");
        long lease = num(a, "leaseMillis");
        Optional<String> held = kv.get(RocksKv.LEADER);
        String holder = held.map(v -> Json.str(Json.parseObject(v), "holder", null)).orElse(null);
        long expiresAt = held.map(v -> Json.num(Json.parseObject(v), "expiresAt", 0)).orElse(0L);
        boolean canTake = holder == null || nodeId.equals(holder) || expiresAt <= now;
        if (!canTake) return new CoordCommand.Result(true, false);
        kv.batch().put(RocksKv.LEADER, Json.write(Map.of("holder", nodeId, "expiresAt", now + lease))).commit();
        return new CoordCommand.Result(true, true);
    }

    private CoordCommand.Result releaseLeadership(Map<String, Object> a) {
        String nodeId = str(a, "nodeId");
        Optional<String> held = kv.get(RocksKv.LEADER);
        String holder = held.map(v -> Json.str(Json.parseObject(v), "holder", null)).orElse(null);
        if (nodeId.equals(holder)) kv.batch().delete(RocksKv.LEADER).commit();
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

    // ---- snapshot: RocksDB checkpoint at the last applied index (§5) ----
    @Override
    public long takeSnapshot() {
        var last = getLastAppliedTermIndex();
        kv.checkpointTo(storage.getSnapshotFile(last.getTerm(), last.getIndex()).getAbsolutePath());
        return last.getIndex();
    }

    // ---- small arg + reply helpers ----
    private static Message reply(CoordCommand.Result r) {
        return Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(r.encode()));
    }
    private static String arg(CoordCommand c, String k) { return (String) c.args().get(k); }
    private static String str(Map<String, Object> a, String k) { return (String) a.get(k); }
    private static long num(Map<String, Object> a, String k) { return ((Number) a.get(k)).longValue(); }
}