package com.wiggle.coordinator.etcd;

import com.wiggle.core.Json;
import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.EpochCodec;
import com.wiggle.server.coord.ProvisionState;
import com.wiggle.server.coord.StorageConfig;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link CoordinatorStore} over etcd — a consensus-backed control-plane store that needs no engine
 * database. Every value is a JSON blob under {@code /wiggle/coord/…}; each mutation that must be atomic
 * (the policy compare-and-set and the leader lease) is an etcd transaction fenced on the key's
 * {@code version} (etcd's per-key write counter), which lines up with the coordinator's own revision
 * semantics. Because coordinator state is bounded, the by-namespace and expiry scans are prefix reads
 * filtered in memory.
 *
 * <p>The leader lease mirrors the JDBC/Cassandra stores (a {@code {holder, expiresAt}} value taken over
 * only when absent, expired, or self, via a version-fenced Txn) rather than an etcd native lease — one
 * consistent contract across backends. A native-lease variant (auto-expiry, no clock dependency) is a
 * possible refinement.
 */
public final class EtcdCoordinatorStore implements CoordinatorStore {

    private static final String PREFIX = "/wiggle/coord/";
    private static final String POLICY = PREFIX + "policy/";
    private static final String NODE = PREFIX + "node/";
    private static final String DEF = PREFIX + "def/";
    private static final String NS = PREFIX + "ns/";
    private static final String LEADER = PREFIX + "leader";

    private final Client client;
    private final boolean ownsClient;
    private final KV kv;

    /** Wraps an existing client (does not close it). */
    public EtcdCoordinatorStore(Client client) {
        this(client, false);
    }

    private EtcdCoordinatorStore(Client client, boolean ownsClient) {
        this.client = client;
        this.ownsClient = ownsClient;
        this.kv = client.getKVClient();
    }

    /** Connects to a comma-separated list of etcd endpoints (e.g. {@code http://127.0.0.1:2379}). */
    public static EtcdCoordinatorStore connect(String endpoints) {
        Client c = Client.builder().endpoints(endpoints.split(",")).build();
        return new EtcdCoordinatorStore(c, true);
    }

    // ---- policy (version-fenced CAS) ----

    @Override public Optional<CoordPolicy> getPolicy(String namespace) {
        return get(POLICY + namespace).map(v -> decodePolicy(namespace, v));
    }

    @Override public List<CoordPolicy> listPolicies() {
        List<CoordPolicy> out = new ArrayList<>();
        for (KeyValue e : prefix(POLICY)) {
            out.add(decodePolicy(suffix(e, POLICY), e.getValue().toString(StandardCharsets.UTF_8)));
        }
        return out;
    }

    @Override public long casPolicy(String namespace, long expectedRevision, CoordPolicy desired) {
        long next = expectedRevision == 0 ? 1 : expectedRevision + 1;
        String value = encodePolicy(namespace, desired.currentEpoch(), next, desired.epochs());
        // create -> the key must be absent (version 0); update -> its version must equal expectedRevision.
        boolean applied = putIfVersion(POLICY + namespace, value, expectedRevision);
        return applied ? next : -1;
    }

    // ---- node roster ----

    @Override public void upsertNode(CoordNode n) {
        put(NODE + n.id(), encodeNode(n));
    }

    @Override public Optional<CoordNode> node(String id) {
        return get(NODE + id).map(EtcdCoordinatorStore::decodeNode);
    }

    @Override public List<CoordNode> nodes(String namespace) {
        List<CoordNode> out = new ArrayList<>();
        for (KeyValue e : prefix(NODE)) {
            CoordNode n = decodeNode(e.getValue().toString(StandardCharsets.UTF_8));
            if (n.namespace().equals(namespace)) out.add(n);
        }
        return out;
    }

    @Override public Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration) {
        Optional<CoordNode> existing = node(id);
        if (existing.isEmpty()) return Optional.empty();
        CoordNode n = existing.get();
        CoordNode updated = new CoordNode(n.id(), n.namespace(), n.cellId(), n.endpoint(), n.region(),
                n.engineVersion(), n.cellFingerprint(), configGeneration, lastHeartbeat);
        put(NODE + id, encodeNode(updated));
        return Optional.of(updated);
    }

    @Override public void removeNode(String id) {
        await(kv.delete(bs(NODE + id)));
    }

    @Override public int expireNodes(long deadlineMillis) {
        int removed = 0;
        for (KeyValue e : prefix(NODE)) {
            CoordNode n = decodeNode(e.getValue().toString(StandardCharsets.UTF_8));
            if (n.lastHeartbeat() < deadlineMillis) { await(kv.delete(e.getKey())); removed++; }
        }
        return removed;
    }

    // ---- definition registry ----

    @Override public Optional<CoordDefinition> getDefinition(String namespace, String name) {
        return get(DEF + namespace + "/" + name).map(v -> decodeDef(namespace, name, v));
    }

    @Override public void putDefinition(CoordDefinition d) {
        put(DEF + d.namespace() + "/" + d.name(), encodeDef(d));
    }

    @Override public boolean removeDefinition(String namespace, String name) {
        long n = await(kv.delete(bs(DEF + namespace + "/" + name))).getDeleted();
        return n > 0;
    }

    @Override public List<CoordDefinition> definitions(String namespace) {
        String p = DEF + namespace + "/";
        List<CoordDefinition> out = new ArrayList<>();
        for (KeyValue e : prefix(p)) out.add(decodeDef(namespace, suffix(e, p), e.getValue().toString(StandardCharsets.UTF_8)));
        return out;
    }

    // ---- namespace registry ----

    @Override public Optional<CoordNamespace> getNamespace(String namespace) {
        return get(NS + namespace).map(v -> decodeNamespace(namespace, v));
    }

    @Override public List<CoordNamespace> namespaces() {
        List<CoordNamespace> out = new ArrayList<>();
        for (KeyValue e : prefix(NS)) out.add(decodeNamespace(suffix(e, NS), e.getValue().toString(StandardCharsets.UTF_8)));
        return out;
    }

    @Override public void putNamespace(CoordNamespace ns) {
        put(NS + ns.namespace(), encodeNamespace(ns));
    }

    // ---- leader election (version-fenced lease) ----

    @Override public boolean acquireLeadership(String nodeId, long nowMillis, long leaseMillis) {
        String value = Json.write(Map.of("holder", nodeId, "expiresAt", nowMillis + leaseMillis));
        GetResponse r = await(kv.get(bs(LEADER)));
        if (r.getKvs().isEmpty()) {
            return putIfVersion(LEADER, value, 0);   // claim when absent
        }
        KeyValue cur = r.getKvs().get(0);
        Map<String, Object> held = Json.parseObject(cur.getValue().toString(StandardCharsets.UTF_8));
        String holder = Json.str(held, "holder", null);
        long expiresAt = (long) Json.num(held, "expiresAt", 0);
        if (nodeId.equals(holder) || expiresAt <= nowMillis) {
            return putIfVersion(LEADER, value, cur.getVersion());   // renew/take-over, fenced on version
        }
        return false;
    }

    @Override public void releaseLeadership(String nodeId) {
        GetResponse r = await(kv.get(bs(LEADER)));
        if (r.getKvs().isEmpty()) return;
        KeyValue cur = r.getKvs().get(0);
        String holder = Json.str(Json.parseObject(cur.getValue().toString(StandardCharsets.UTF_8)), "holder", null);
        if (nodeId.equals(holder)) {
            kv.txn().If(new Cmp(bs(LEADER), Cmp.Op.EQUAL, CmpTarget.version(cur.getVersion())))
                    .Then(Op.delete(bs(LEADER), io.etcd.jetcd.options.DeleteOption.DEFAULT))
                    .commit();   // best-effort
        }
    }

    @Override public void close() {
        if (ownsClient) client.close();
    }

    // ---- etcd helpers ----

    private static ByteSequence bs(String s) {
        return ByteSequence.from(s, StandardCharsets.UTF_8);
    }

    private Optional<String> get(String key) {
        List<KeyValue> kvs = await(kv.get(bs(key))).getKvs();
        return kvs.isEmpty() ? Optional.empty() : Optional.of(kvs.get(0).getValue().toString(StandardCharsets.UTF_8));
    }

    private void put(String key, String value) {
        await(kv.put(bs(key), bs(value)));
    }

    private List<KeyValue> prefix(String prefix) {
        return await(kv.get(bs(prefix), GetOption.newBuilder().isPrefix(true).build())).getKvs();
    }

    /** Atomic put fenced on the key's current version ({@code 0} means "must be absent"). */
    private boolean putIfVersion(String key, String value, long expectedVersion) {
        return await(kv.txn()
                .If(new Cmp(bs(key), Cmp.Op.EQUAL, CmpTarget.version(expectedVersion)))
                .Then(Op.put(bs(key), bs(value), PutOption.DEFAULT))
                .commit()).isSucceeded();
    }

    private static String suffix(KeyValue e, String prefix) {
        return e.getKey().toString(StandardCharsets.UTF_8).substring(prefix.length());
    }

    private static <T> T await(CompletableFuture<T> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("etcd call interrupted", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new RuntimeException("etcd call failed", e.getCause());
        }
    }

    // ---- JSON codecs ----

    private static String encodePolicy(String ns, long currentEpoch, long revision, Map<Long, CoordPolicy.EpochRing> epochs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("currentEpoch", currentEpoch);
        m.put("revision", revision);
        m.put("epochs", EpochCodec.encode(epochs));
        return Json.write(m);
    }

    private static CoordPolicy decodePolicy(String ns, String json) {
        Map<String, Object> m = Json.parseObject(json);
        return new CoordPolicy(ns, (long) Json.num(m, "currentEpoch", 0), (long) Json.num(m, "revision", 0),
                EpochCodec.decode(Json.reqStr(m, "epochs")));
    }

    private static String encodeNode(CoordNode n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", n.id());
        m.put("namespace", n.namespace());
        m.put("cellId", n.cellId());
        m.put("endpoint", n.endpoint());
        m.put("region", n.region());
        m.put("engineVersion", n.engineVersion());
        m.put("cellFingerprint", n.cellFingerprint());
        m.put("configGeneration", n.configGeneration());
        m.put("lastHeartbeat", n.lastHeartbeat());
        return Json.write(m);
    }

    private static CoordNode decodeNode(String json) {
        Map<String, Object> m = Json.parseObject(json);
        return new CoordNode(Json.reqStr(m, "id"), Json.reqStr(m, "namespace"), Json.reqStr(m, "cellId"),
                Json.reqStr(m, "endpoint"), Json.str(m, "region", null), Json.str(m, "engineVersion", null),
                Json.str(m, "cellFingerprint", null),
                (long) Json.num(m, "configGeneration", 0), (long) Json.num(m, "lastHeartbeat", 0));
    }

    private static String encodeDef(CoordDefinition d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("namespace", d.namespace());
        m.put("name", d.name());
        m.put("version", d.version());
        m.put("hash", d.hash());
        m.put("registeredAt", d.registeredAt());
        return Json.write(m);
    }

    private static CoordDefinition decodeDef(String ns, String name, String json) {
        Map<String, Object> m = Json.parseObject(json);
        return new CoordDefinition(ns, name, (int) Json.num(m, "version", 0), Json.reqStr(m, "hash"),
                (long) Json.num(m, "registeredAt", 0));
    }

    private static String encodeNamespace(CoordNamespace n) {
        StorageConfig sc = n.storage();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("namespace", n.namespace());
        m.put("state", n.state().name());
        m.put("scheme", sc.scheme());
        m.put("jdbcUrl", sc.jdbcUrl());
        m.put("user", sc.user());
        m.put("secretRef", sc.secretRef());
        m.put("poolSize", sc.poolSize());
        m.put("replicas", n.replicas());
        m.put("region", n.region());
        m.put("endpoint", n.endpoint());
        m.put("error", n.error());
        m.put("updatedAt", n.updatedAt());
        return Json.write(m);
    }

    private static CoordNamespace decodeNamespace(String ns, String json) {
        Map<String, Object> m = Json.parseObject(json);
        StorageConfig sc = new StorageConfig(Json.str(m, "scheme", null), Json.str(m, "jdbcUrl", null),
                Json.str(m, "user", null), Json.str(m, "secretRef", null), (int) Json.num(m, "poolSize", 0));
        return new CoordNamespace(ns, ProvisionState.valueOf(Json.reqStr(m, "state")), sc,
                (int) Json.num(m, "replicas", 0), Json.str(m, "region", null), Json.str(m, "endpoint", null),
                Json.str(m, "error", null), (long) Json.num(m, "updatedAt", 0));
    }
}
