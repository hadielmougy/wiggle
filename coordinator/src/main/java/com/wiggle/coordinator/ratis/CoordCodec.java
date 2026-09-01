package com.wiggle.coordinator.ratis;

import com.wiggle.core.Json;
import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.EpochCodec;
import com.wiggle.server.coord.ProvisionState;
import com.wiggle.server.coord.StorageConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The JSON on-disk form for the Ratis+RocksDB coordinator store, shared by the client
 * ({@link RatisCoordinatorStore}, which decodes query replies) and the replicated state machine
 * ({@link CoordStateMachine}, which encodes what it stores). It mirrors the field names the JDBC /
 * Cassandra coordinator stores persist, with one deliberate difference: every record is
 * <em>self-contained</em> — the policy blob carries its own {@code namespace} — so a value read back from
 * RocksDB decodes without also carrying its key. Bounded control-plane state; never per-instance.
 */
final class CoordCodec {

    private CoordCodec() {}

    // ---- node ----

    static String encodeNode(CoordNode n) {
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

    static CoordNode decodeNode(String json) {
        Map<String, Object> m = Json.parseObject(json);
        return new CoordNode(Json.reqStr(m, "id"), Json.reqStr(m, "namespace"), Json.reqStr(m, "cellId"),
                Json.reqStr(m, "endpoint"), Json.str(m, "region", null), Json.str(m, "engineVersion", null),
                Json.str(m, "cellFingerprint", null),
                Json.num(m, "configGeneration", 0), Json.num(m, "lastHeartbeat", 0));
    }

    // ---- policy (self-contained: carries its own namespace) ----

    static String encodePolicy(String namespace, long currentEpoch, long revision, Map<Long, CoordPolicy.EpochRing> epochs) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("namespace", namespace);
        m.put("currentEpoch", currentEpoch);
        m.put("revision", revision);
        m.put("epochs", EpochCodec.encode(epochs));
        return Json.write(m);
    }

    static CoordPolicy decodePolicy(String json) {
        Map<String, Object> m = Json.parseObject(json);
        return new CoordPolicy(Json.reqStr(m, "namespace"), Json.num(m, "currentEpoch", 0),
                Json.num(m, "revision", 0), EpochCodec.decode(Json.reqStr(m, "epochs")));
    }

    /** The stored revision of a policy blob, or {@code 0} when absent — the CAS fence. */
    static long policyRevision(String json) {
        return Json.num(Json.parseObject(json), "revision", 0);
    }

    // ---- definition (self-contained: carries namespace + name) ----

    static String encodeDef(CoordDefinition d) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("namespace", d.namespace());
        m.put("name", d.name());
        m.put("version", d.version());
        m.put("hash", d.hash());
        m.put("registeredAt", d.registeredAt());
        return Json.write(m);
    }

    static CoordDefinition decodeDef(String json) {
        Map<String, Object> m = Json.parseObject(json);
        return new CoordDefinition(Json.reqStr(m, "namespace"), Json.reqStr(m, "name"),
                (int) Json.num(m, "version", 0), Json.reqStr(m, "hash"), Json.num(m, "registeredAt", 0));
    }

    // ---- namespace (self-contained) ----

    static String encodeNamespace(CoordNamespace n) {
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

    static CoordNamespace decodeNamespace(String json) {
        Map<String, Object> m = Json.parseObject(json);
        StorageConfig sc = new StorageConfig(Json.str(m, "scheme", null), Json.str(m, "jdbcUrl", null),
                Json.str(m, "user", null), Json.str(m, "secretRef", null), (int) Json.num(m, "poolSize", 0));
        return new CoordNamespace(Json.reqStr(m, "namespace"), ProvisionState.valueOf(Json.reqStr(m, "state")), sc,
                (int) Json.num(m, "replicas", 0), Json.str(m, "region", null), Json.str(m, "endpoint", null),
                Json.str(m, "error", null), Json.num(m, "updatedAt", 0));
    }
}