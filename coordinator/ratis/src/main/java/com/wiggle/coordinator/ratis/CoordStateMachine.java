package com.wiggle.coordinator.ratis;

import com.wiggle.core.Json;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * DESIGN SKETCH — not wired into the build; Ratis API calls are indicative. The replicated state machine:
 * it applies {@link CoordCommand}s to {@link RocksKv} deterministically (every timestamp is a command
 * arg, never a wall-clock read here), and serves reads via {@link #query}. Snapshots are RocksDB
 * checkpoints. See docs/coordinator-ratis-rocksdb.md §2/§5.
 */
public final class CoordStateMachine extends BaseStateMachine {

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    private RocksKv kv;   // opened over the state-machine data dir in initialize(...); omitted here

    // ---- writes: go through the Raft log, applied in index order on every replica ----
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        CoordCommand cmd = CoordCommand.decode(trx.getStateMachineLogEntry().getLogData().toByteArray());
        CoordCommand.Result result = switch (cmd.op()) {
            case CAS_POLICY            -> casPolicy(cmd.args());
            case BIND_CELL             -> bindCell(cmd.args());
            case EXPIRE_NODES          -> expireNodes(cmd.args());
            case ACQUIRE_LEADERSHIP    -> acquireLeadership(cmd.args());
            case UPSERT_NODE, TOUCH_NODE, REMOVE_NODE, PRUNE_ORPHAN_BINDINGS,
                 PUT_DEFINITION, REMOVE_DEFINITION, PUT_NAMESPACE, RELEASE_LEADERSHIP
                                       -> applyOther(cmd);   // straightforward puts/deletes -- omitted
            default -> throw new IllegalStateException("not a write op: " + cmd.op());
        };
        return CompletableFuture.completedFuture(Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(result.encode())));
    }

    // ---- reads: linearizable query (Ratis routes read-index to the leader) ----
    @Override
    public CompletableFuture<Message> query(Message request) {
        CoordCommand cmd = CoordCommand.decode(request.getContent().toByteArray());
        Object value = switch (cmd.op()) {
            case GET_POLICY       -> kv.get(RocksKv.POLICY + arg(cmd, "ns")).orElse(null);
            case LIST_NODES       -> kv.values(RocksKv.NODE_NS + arg(cmd, "ns") + "/");   // via the index
            case GET_DEFINITION   -> kv.get(RocksKv.DEF + arg(cmd, "ns") + "/" + arg(cmd, "name")).orElse(null);
            // GET_NODE / LIST_POLICIES / LIST_DEFINITIONS / GET_NAMESPACE / LIST_NAMESPACES -- omitted
            default -> throw new IllegalStateException("not a read op: " + cmd.op());
        };
        return CompletableFuture.completedFuture(Message.valueOf(
                org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(
                        new CoordCommand.Result(true, value).encode())));
    }

    // ---- representative deterministic applies -------------------------------------------------

    /** CAS on the policy revision -- the version check lives here, atomic because the leader serializes. */
    private CoordCommand.Result casPolicy(Map<String, Object> a) {
        String ns = (String) a.get("ns");
        long expected = ((Number) a.get("expectedRevision")).longValue();
        String key = RocksKv.POLICY + ns;
        long current = kv.get(key).map(v -> (long) Json.num(Json.parseObject(v), "revision", 0)).orElse(0L);
        boolean create = expected == 0;
        boolean match = create ? kv.get(key).isEmpty() : current == expected;
        if (!match) return new CoordCommand.Result(false, -1L);            // CAS lost
        long next = expected + 1;
        kv.batch().put(key, encodePolicy(a, next)).commit();
        return new CoordCommand.Result(true, next);                        // the new revision
    }

    /** Claim (ns, cell) -> fingerprint if absent; else succeed only if the same fingerprint holds it. */
    private CoordCommand.Result bindCell(Map<String, Object> a) {
        String fp = (String) a.get("fingerprint");
        if (fp == null) return new CoordCommand.Result(true, true);        // guard skipped (no storage identity)
        String key = RocksKv.CELL + a.get("ns") + "/" + a.get("cellId");
        Optional<String> held = kv.get(key);
        if (held.isEmpty()) { kv.batch().put(key, fp).commit(); return new CoordCommand.Result(true, true); }
        return new CoordCommand.Result(true, fp.equals(held.get()));       // replica -> true; other cell -> false
    }

    /** Remove roster nodes whose lastHeartbeat < deadline (deadline is a command arg -> deterministic). */
    private CoordCommand.Result expireNodes(Map<String, Object> a) {
        long deadline = ((Number) a.get("deadlineMillis")).longValue();
        int removed = 0;
        RocksKv.Batch batch = kv.batch();
        for (String id : kv.scan(RocksKv.NODE)) {
            Map<String, Object> node = Json.parseObject(kv.get(id).orElseThrow());
            if ((long) Json.num(node, "lastHeartbeat", 0) < deadline) {
                batch.delete(id).delete(RocksKv.NODE_NS + node.get("namespace") + "/" + node.get("id"));
                removed++;
            }
        }
        batch.commit();
        return new CoordCommand.Result(true, removed);
    }

    /** Lease-based leadership (Option A): nowMillis is a command arg, so apply stays deterministic.
     *  Option B would delete this and key duties off Raft's own leader (§4). */
    private CoordCommand.Result acquireLeadership(Map<String, Object> a) {
        String nodeId = (String) a.get("nodeId");
        long now = ((Number) a.get("nowMillis")).longValue();
        long lease = ((Number) a.get("leaseMillis")).longValue();
        Optional<String> held = kv.get(RocksKv.LEADER);
        String holder = held.map(v -> Json.str(Json.parseObject(v), "holder", null)).orElse(null);
        long expiresAt = held.map(v -> (long) Json.num(Json.parseObject(v), "expiresAt", 0)).orElse(0L);
        boolean canTake = holder == null || nodeId.equals(holder) || expiresAt <= now;
        if (!canTake) return new CoordCommand.Result(true, false);
        kv.batch().put(RocksKv.LEADER, Json.write(Map.of("holder", nodeId, "expiresAt", now + lease))).commit();
        return new CoordCommand.Result(true, true);
    }

    // ---- snapshot: RocksDB checkpoint at the last applied index (§5) ----
    @Override
    public long takeSnapshot() {
        long index = getLastAppliedTermIndex().getIndex();
        kv.checkpointTo(storage.getSnapshotFile(getLastAppliedTermIndex().getTerm(), index).getAbsolutePath());
        return index;
    }

    private CoordCommand.Result applyOther(CoordCommand cmd) { throw new UnsupportedOperationException("sketch: " + cmd.op()); }
    private static String encodePolicy(Map<String, Object> a, long rev) { return "TODO: EpochCodec + {currentEpoch, rev}"; }
    private static String arg(CoordCommand c, String k) { return (String) c.args().get(k); }
}
