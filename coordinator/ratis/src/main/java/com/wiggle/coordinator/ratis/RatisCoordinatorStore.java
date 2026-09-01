package com.wiggle.coordinator.ratis;

import com.wiggle.core.Json;
import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.CoordinatorStore;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * DESIGN SKETCH — not wired into the build; Ratis API calls are indicative. {@link CoordinatorStore} over
 * an embedded Ratis group + RocksDB: writes are submitted to the Raft log, reads are linearizable queries.
 * Depends only on {@code coordinator:spi} (never the engine {@code Storage}) — the server ⊥ coordinator
 * decoupling holds. See docs/coordinator-ratis-rocksdb.md.
 */
public final class RatisCoordinatorStore implements CoordinatorStore {

    private final RaftClient client;   // constructed from the group's peers; single-member for dev

    public RatisCoordinatorStore(RaftClient client) { this.client = client; }

    // ---- helpers: submit a write to the log, or a linearizable read-query ----
    private CoordCommand.Result write(CoordCommand.Op op, Map<String, Object> args) {
        try {
            var reply = client.io().send(Message.valueOf(bs(new CoordCommand(op, args).encode())));
            return CoordCommand.Result.decode(reply.getMessage().getContent().toByteArray());
        } catch (Exception e) { throw new IllegalStateException("ratis write " + op + " failed", e); }
    }

    private CoordCommand.Result read(CoordCommand.Op op, Map<String, Object> args) {
        try {
            var reply = client.io().sendReadOnly(Message.valueOf(bs(new CoordCommand(op, args).encode())));
            return CoordCommand.Result.decode(reply.getMessage().getContent().toByteArray());
        } catch (Exception e) { throw new IllegalStateException("ratis read " + op + " failed", e); }
    }

    // ---- policy (CAS-guarded) ----
    @Override public long casPolicy(String namespace, long expectedRevision, CoordPolicy desired) {
        CoordCommand.Result r = write(CoordCommand.Op.CAS_POLICY, Map.of(
                "ns", namespace,
                "expectedRevision", expectedRevision,
                "currentEpoch", desired.currentEpoch(),
                "epochs", com.wiggle.server.coord.EpochCodec.encode(desired.epochs())));
        return ((Number) r.value()).longValue();   // new revision, or -1 if the CAS lost
    }

    @Override public Optional<CoordPolicy> getPolicy(String namespace) {
        Object v = read(CoordCommand.Op.GET_POLICY, Map.of("ns", namespace)).value();
        return v == null || "".equals(v) ? Optional.empty() : Optional.of(decodePolicy(namespace, (String) v));
    }

    // ---- roster ----
    @Override public void upsertNode(CoordNode node) { write(CoordCommand.Op.UPSERT_NODE, nodeArgs(node)); }

    @Override public Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration) {
        CoordCommand.Result r = write(CoordCommand.Op.TOUCH_NODE, Map.of(
                "id", id, "lastHeartbeat", lastHeartbeat, "configGeneration", configGeneration));
        return r.ok() && r.value() != null && !"".equals(r.value())
                ? Optional.of(decodeNode((String) r.value())) : Optional.empty();
    }

    @Override public int expireNodes(long deadlineMillis) {
        return ((Number) write(CoordCommand.Op.EXPIRE_NODES, Map.of("deadlineMillis", deadlineMillis)).value()).intValue();
    }

    @Override public List<CoordNode> nodes(String namespace) {
        @SuppressWarnings("unchecked")
        List<String> rows = (List<String>) read(CoordCommand.Op.LIST_NODES, Map.of("ns", namespace)).value();
        return rows.stream().map(this::decodeNode).toList();
    }

    // ---- cell binding ----
    @Override public boolean bindCell(String namespace, String cellId, String fingerprint) {
        Map<String, Object> args = new java.util.HashMap<>();
        args.put("ns", namespace); args.put("cellId", cellId); args.put("fingerprint", fingerprint);
        return Boolean.TRUE.equals(write(CoordCommand.Op.BIND_CELL, args).value());
    }

    // ---- leadership (Option A: lease command; Option B would use Raft's own leader — §4) ----
    @Override public boolean acquireLeadership(String nodeId, long nowMillis, long leaseMillis) {
        return Boolean.TRUE.equals(write(CoordCommand.Op.ACQUIRE_LEADERSHIP, Map.of(
                "nodeId", nodeId, "nowMillis", nowMillis, "leaseMillis", leaseMillis)).value());
    }

    // ---- remaining CoordinatorStore methods follow the same write()/read() pattern (omitted in sketch):
    //   listPolicies, node, removeNode, pruneOrphanCellBindings,
    //   getDefinition/putDefinition/removeDefinition/definitions,
    //   getNamespace/putNamespace/namespaces, releaseLeadership ----

    @Override public void close() { try { client.close(); } catch (Exception ignored) { } }

    // ---- (de)serialization helpers ----
    private static Map<String, Object> nodeArgs(CoordNode n) {
        return Map.of("id", n.id(), "namespace", n.namespace(), "cellId", n.cellId(),
                "endpoint", n.endpoint(), "region", n.region() == null ? "" : n.region(),
                "lastHeartbeat", n.lastHeartbeat());   // + engineVersion/fingerprint/configGeneration
    }
    private CoordNode decodeNode(String json) { throw new UnsupportedOperationException("sketch"); }
    private CoordPolicy decodePolicy(String ns, String json) { throw new UnsupportedOperationException("sketch"); }
    private static org.apache.ratis.thirdparty.com.google.protobuf.ByteString bs(byte[] b) {
        return org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(b);
    }

    // Unimplemented in the sketch:
    @Override public List<CoordPolicy> listPolicies() { throw new UnsupportedOperationException("sketch"); }
    @Override public Optional<CoordNode> node(String id) { throw new UnsupportedOperationException("sketch"); }
    @Override public void removeNode(String id) { write(CoordCommand.Op.REMOVE_NODE, Map.of("id", id)); }
    @Override public int pruneOrphanCellBindings() {
        return ((Number) write(CoordCommand.Op.PRUNE_ORPHAN_BINDINGS, Map.of()).value()).intValue();
    }
    @Override public Optional<CoordDefinition> getDefinition(String ns, String name) { throw new UnsupportedOperationException("sketch"); }
    @Override public void putDefinition(CoordDefinition def) { throw new UnsupportedOperationException("sketch"); }
    @Override public boolean removeDefinition(String ns, String name) { throw new UnsupportedOperationException("sketch"); }
    @Override public List<CoordDefinition> definitions(String ns) { throw new UnsupportedOperationException("sketch"); }
    @Override public Optional<CoordNamespace> getNamespace(String ns) { throw new UnsupportedOperationException("sketch"); }
    @Override public List<CoordNamespace> namespaces() { throw new UnsupportedOperationException("sketch"); }
    @Override public void putNamespace(CoordNamespace ns) { throw new UnsupportedOperationException("sketch"); }
}
