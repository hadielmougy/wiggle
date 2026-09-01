package com.wiggle.coordinator.ratis;

import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.EpochCodec;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.server.RaftServer;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link CoordinatorStore} over an embedded Ratis group + RocksDB: writes are submitted to the Raft log
 * ({@code client.io().send}) and reads are linearizable queries ({@code sendReadOnly}); the authoritative
 * state and all mutation logic live in {@link CoordStateMachine}, so this client just marshals commands
 * and decodes replies. Depends only on {@code coordinator:spi} (never the engine {@code Storage}) — the
 * server ⊥ coordinator decoupling holds. See docs/coordinator-ratis-rocksdb.md.
 */
public final class RatisCoordinatorStore implements CoordinatorStore {

    private final RaftClient client;
    private final RaftServer server;   // this node's group member, if any (null for a pure client); closed with us

    RatisCoordinatorStore(RaftClient client, RaftServer server) {
        this.client = client;
        this.server = server;
    }

    // ---- submit a write to the log, or a linearizable read-query ----
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
                "epochs", EpochCodec.encode(desired.epochs())));
        return ((Number) r.value()).longValue();   // new revision, or -1 if the CAS lost
    }

    @Override public Optional<CoordPolicy> getPolicy(String namespace) {
        return single(read(CoordCommand.Op.GET_POLICY, Map.of("ns", namespace))).map(CoordCodec::decodePolicy);
    }

    @Override public List<CoordPolicy> listPolicies() {
        return list(read(CoordCommand.Op.LIST_POLICIES, Map.of())).stream().map(CoordCodec::decodePolicy).toList();
    }

    // ---- roster ----
    @Override public void upsertNode(CoordNode node) {
        write(CoordCommand.Op.UPSERT_NODE, Map.of("node", CoordCodec.encodeNode(node)));
    }

    @Override public Optional<CoordNode> node(String id) {
        return single(read(CoordCommand.Op.GET_NODE, Map.of("id", id))).map(CoordCodec::decodeNode);
    }

    @Override public List<CoordNode> nodes(String namespace) {
        return list(read(CoordCommand.Op.LIST_NODES, Map.of("ns", namespace))).stream().map(CoordCodec::decodeNode).toList();
    }

    @Override public Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration) {
        CoordCommand.Result r = write(CoordCommand.Op.TOUCH_NODE, Map.of(
                "id", id, "lastHeartbeat", lastHeartbeat, "configGeneration", configGeneration));
        return single(r).map(CoordCodec::decodeNode);
    }

    @Override public void removeNode(String id) {
        write(CoordCommand.Op.REMOVE_NODE, Map.of("id", id));
    }

    @Override public int expireNodes(long deadlineMillis) {
        return ((Number) write(CoordCommand.Op.EXPIRE_NODES, Map.of("deadlineMillis", deadlineMillis)).value()).intValue();
    }

    // ---- cell binding ----
    @Override public boolean bindCell(String namespace, String cellId, String fingerprint) {
        if (fingerprint == null) return true;   // guard skipped for a node with no storage identity
        return Boolean.TRUE.equals(write(CoordCommand.Op.BIND_CELL, Map.of(
                "ns", namespace, "cellId", cellId, "fingerprint", fingerprint)).value());
    }

    @Override public int pruneOrphanCellBindings() {
        return ((Number) write(CoordCommand.Op.PRUNE_ORPHAN_BINDINGS, Map.of()).value()).intValue();
    }

    // ---- definition registry ----
    @Override public Optional<CoordDefinition> getDefinition(String ns, String name) {
        return single(read(CoordCommand.Op.GET_DEFINITION, Map.of("ns", ns, "name", name))).map(CoordCodec::decodeDef);
    }

    @Override public void putDefinition(CoordDefinition def) {
        write(CoordCommand.Op.PUT_DEFINITION, Map.of("def", CoordCodec.encodeDef(def)));
    }

    @Override public boolean removeDefinition(String ns, String name) {
        return Boolean.TRUE.equals(write(CoordCommand.Op.REMOVE_DEFINITION, Map.of("ns", ns, "name", name)).value());
    }

    @Override public List<CoordDefinition> definitions(String ns) {
        return list(read(CoordCommand.Op.LIST_DEFINITIONS, Map.of("ns", ns))).stream().map(CoordCodec::decodeDef).toList();
    }

    // ---- namespace registry ----
    @Override public Optional<CoordNamespace> getNamespace(String ns) {
        return single(read(CoordCommand.Op.GET_NAMESPACE, Map.of("ns", ns))).map(CoordCodec::decodeNamespace);
    }

    @Override public List<CoordNamespace> namespaces() {
        return list(read(CoordCommand.Op.LIST_NAMESPACES, Map.of())).stream().map(CoordCodec::decodeNamespace).toList();
    }

    @Override public void putNamespace(CoordNamespace ns) {
        write(CoordCommand.Op.PUT_NAMESPACE, Map.of("ns", CoordCodec.encodeNamespace(ns)));
    }

    // ---- leadership (durable lease over the replicated state) ----
    @Override public boolean acquireLeadership(String nodeId, long nowMillis, long leaseMillis) {
        return Boolean.TRUE.equals(write(CoordCommand.Op.ACQUIRE_LEADERSHIP, Map.of(
                "nodeId", nodeId, "nowMillis", nowMillis, "leaseMillis", leaseMillis)).value());
    }

    @Override public void releaseLeadership(String nodeId) {
        write(CoordCommand.Op.RELEASE_LEADERSHIP, Map.of("nodeId", nodeId));
    }

    @Override public void close() {
        try { client.close(); } catch (Exception ignored) { }
        try { if (server != null) server.close(); } catch (Exception ignored) { }
    }

    // ---- reply shape helpers: the state machine returns a single JSON string, "" for absent, or a list ----
    private static Optional<String> single(CoordCommand.Result r) {
        Object v = r.value();
        return (v == null || "".equals(v)) ? Optional.empty() : Optional.of((String) v);
    }

    @SuppressWarnings("unchecked")
    private static List<String> list(CoordCommand.Result r) {
        Object v = r.value();
        return v == null ? List.of() : (List<String>) v;
    }

    private static org.apache.ratis.thirdparty.com.google.protobuf.ByteString bs(byte[] b) {
        return org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(b);
    }
}