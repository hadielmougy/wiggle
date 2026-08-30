package com.wiggle.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.EpochCodec;
import com.wiggle.server.coord.ProvisionState;
import com.wiggle.server.coord.StorageConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A CQL {@link CoordinatorStore} over the {@code coord_*} tables in the coordinator keyspace. The
 * policy compare-and-set is a Cassandra <em>lightweight transaction</em> (LWT): create is
 * {@code INSERT ... IF NOT EXISTS}, update is {@code UPDATE ... IF revision=?} — the same fence the
 * JDBC store gets from {@code WHERE revision=?}, which is what makes the tolerated brief-overlap leader
 * election safe (a stale ex-leader's write is not applied).
 *
 * <p>This shares the {@link CassandraStorage} session and never closes it. Coordinator state is bounded
 * (never per-instance), so the by-namespace and expiry scans use {@code ALLOW FILTERING} on the small
 * roster without concern.
 */
public final class CassandraCoordinatorStore implements CoordinatorStore {

    private final CqlSession session;
    private final Map<String, PreparedStatement> prepared = new ConcurrentHashMap<>();

    CassandraCoordinatorStore(CqlSession session) {
        this.session = session;
    }

    private PreparedStatement ps(String cql) {
        return prepared.computeIfAbsent(cql, session::prepare);
    }

    // ---- policy (LWT compare-and-set) ----

    @Override public Optional<CoordPolicy> getPolicy(String namespace) {
        Row r = session.execute(ps("SELECT current_epoch, epochs, revision FROM coord_policy WHERE namespace=?")
                .bind(namespace)).one();
        if (r == null) return Optional.empty();
        return Optional.of(new CoordPolicy(namespace, r.getLong("current_epoch"), r.getLong("revision"),
                EpochCodec.decode(r.getString("epochs"))));
    }

    @Override public List<CoordPolicy> listPolicies() {
        List<CoordPolicy> out = new ArrayList<>();
        for (Row r : session.execute(ps("SELECT namespace, current_epoch, epochs, revision FROM coord_policy").bind())) {
            out.add(new CoordPolicy(r.getString("namespace"), r.getLong("current_epoch"), r.getLong("revision"),
                    EpochCodec.decode(r.getString("epochs"))));
        }
        return out;
    }

    @Override public long casPolicy(String namespace, long expectedRevision, CoordPolicy desired) {
        String epochs = EpochCodec.encode(desired.epochs());
        if (expectedRevision == 0) {
            boolean applied = session.execute(ps(
                    "INSERT INTO coord_policy (namespace, current_epoch, epochs, revision) VALUES (?,?,?,1) IF NOT EXISTS")
                    .bind(namespace, desired.currentEpoch(), epochs)).wasApplied();
            return applied ? 1 : -1;
        }
        long next = expectedRevision + 1;
        boolean applied = session.execute(ps(
                "UPDATE coord_policy SET current_epoch=?, epochs=?, revision=? WHERE namespace=? IF revision=?")
                .bind(desired.currentEpoch(), epochs, next, namespace, expectedRevision)).wasApplied();
        return applied ? next : -1;
    }

    // ---- node roster ----

    @Override public void upsertNode(CoordNode n) {
        session.execute(ps("INSERT INTO coord_node (id, namespace, cell_id, endpoint, region, engine_version, " +
                "cell_fingerprint, config_generation, last_heartbeat) VALUES (?,?,?,?,?,?,?,?,?)")
                .bind(n.id(), n.namespace(), n.cellId(), n.endpoint(), n.region(), n.engineVersion(),
                        n.cellFingerprint(), n.configGeneration(), n.lastHeartbeat()));
    }

    @Override public Optional<CoordNode> node(String id) {
        Row r = session.execute(ps("SELECT * FROM coord_node WHERE id=?").bind(id)).one();
        return r == null ? Optional.empty() : Optional.of(readNode(r));
    }

    @Override public List<CoordNode> nodes(String namespace) {
        List<CoordNode> out = new ArrayList<>();
        for (Row r : session.execute(ps("SELECT * FROM coord_node WHERE namespace=? ALLOW FILTERING").bind(namespace))) {
            out.add(readNode(r));
        }
        return out;
    }

    @Override public Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration) {
        // IF EXISTS guards the removed-concurrently case: a blind UPDATE would resurrect a partial row.
        boolean applied = session.execute(ps(
                "UPDATE coord_node SET last_heartbeat=?, config_generation=? WHERE id=? IF EXISTS")
                .bind(lastHeartbeat, configGeneration, id)).wasApplied();
        return applied ? node(id) : Optional.empty();
    }

    @Override public void removeNode(String id) {
        session.execute(ps("DELETE FROM coord_node WHERE id=?").bind(id));
    }

    @Override public int expireNodes(long deadlineMillis) {
        List<String> dead = new ArrayList<>();
        for (Row r : session.execute(ps("SELECT id FROM coord_node WHERE last_heartbeat < ? ALLOW FILTERING")
                .bind(deadlineMillis))) {
            dead.add(r.getString("id"));
        }
        for (String id : dead) removeNode(id);
        return dead.size();
    }

    @Override public boolean bindCell(String namespace, String cellId, String fingerprint) {
        if (fingerprint == null) return true;
        com.datastax.oss.driver.api.core.cql.ResultSet rs = session.execute(ps(
                "INSERT INTO coord_cell (namespace, cell_id, fingerprint) VALUES (?,?,?) IF NOT EXISTS")
                .bind(namespace, cellId, fingerprint));
        if (rs.wasApplied()) return true;   // claimed -- single-partition LWT made this the atomic winner
        Row existing = rs.one();            // not applied: LWT returns the current row; compare its fingerprint
        return existing != null && fingerprint.equals(existing.getString("fingerprint"));
    }

    @Override public int pruneOrphanCellBindings() {
        int pruned = 0;
        for (Row r : session.execute("SELECT namespace, cell_id FROM coord_cell")) {
            String ns = r.getString("namespace"), cell = r.getString("cell_id");
            boolean hasNode = session.execute(ps(
                    "SELECT id FROM coord_node WHERE namespace=? AND cell_id=? LIMIT 1 ALLOW FILTERING")
                    .bind(ns, cell)).one() != null;
            if (!hasNode) {
                session.execute(ps("DELETE FROM coord_cell WHERE namespace=? AND cell_id=?").bind(ns, cell));
                pruned++;
            }
        }
        return pruned;
    }

    // ---- definition registry ----

    @Override public Optional<CoordDefinition> getDefinition(String namespace, String name) {
        Row r = session.execute(ps("SELECT version, hash, registered_at FROM coord_definition WHERE namespace=? AND name=?")
                .bind(namespace, name)).one();
        if (r == null) return Optional.empty();
        return Optional.of(new CoordDefinition(namespace, name, r.getInt("version"), r.getString("hash"),
                r.getLong("registered_at")));
    }

    @Override public void putDefinition(CoordDefinition d) {
        session.execute(ps("INSERT INTO coord_definition (namespace, name, version, hash, registered_at) VALUES (?,?,?,?,?)")
                .bind(d.namespace(), d.name(), d.version(), d.hash(), d.registeredAt()));
    }

    @Override public boolean removeDefinition(String namespace, String name) {
        return session.execute(ps("DELETE FROM coord_definition WHERE namespace=? AND name=? IF EXISTS")
                .bind(namespace, name)).wasApplied();
    }

    @Override public List<CoordDefinition> definitions(String namespace) {
        List<CoordDefinition> out = new ArrayList<>();
        for (Row r : session.execute(ps("SELECT name, version, hash, registered_at FROM coord_definition WHERE namespace=?")
                .bind(namespace))) {
            out.add(new CoordDefinition(namespace, r.getString("name"), r.getInt("version"), r.getString("hash"),
                    r.getLong("registered_at")));
        }
        return out;
    }

    // ---- namespace registry ----

    @Override public Optional<CoordNamespace> getNamespace(String namespace) {
        Row r = session.execute(ps("SELECT * FROM coord_namespace WHERE namespace=?").bind(namespace)).one();
        return r == null ? Optional.empty() : Optional.of(readNamespace(r));
    }

    @Override public List<CoordNamespace> namespaces() {
        List<CoordNamespace> out = new ArrayList<>();
        for (Row r : session.execute(ps("SELECT * FROM coord_namespace").bind())) out.add(readNamespace(r));
        return out;
    }

    @Override public void putNamespace(CoordNamespace ns) {
        StorageConfig sc = ns.storage();
        session.execute(ps("INSERT INTO coord_namespace (namespace, state, scheme, jdbc_url, db_user, secret_ref, " +
                "pool_size, replicas, region, endpoint, error, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
                .bind(ns.namespace(), ns.state().name(), sc.scheme(), sc.jdbcUrl(), sc.user(), sc.secretRef(),
                        sc.poolSize(), ns.replicas(), ns.region(), ns.endpoint(), ns.error(), ns.updatedAt()));
    }

    // ---- leader election (single-row lease via LWT) ----

    @Override public boolean acquireLeadership(String nodeId, long nowMillis, long leaseMillis) {
        long expiry = nowMillis + leaseMillis;
        Row r = session.execute(ps("SELECT holder, expires_at FROM coord_leader WHERE id='coordinator'").bind()).one();
        if (r == null) {
            return session.execute(ps(
                    "INSERT INTO coord_leader (id, holder, expires_at) VALUES ('coordinator', ?, ?) IF NOT EXISTS")
                    .bind(nodeId, expiry)).wasApplied();
        }
        String holder = r.getString("holder");
        long curExpiry = r.getLong("expires_at");
        if (nodeId.equals(holder) || curExpiry <= nowMillis) {
            return session.execute(ps(
                    "UPDATE coord_leader SET holder=?, expires_at=? WHERE id='coordinator' IF holder=? AND expires_at=?")
                    .bind(nodeId, expiry, holder, curExpiry)).wasApplied();   // CAS on the read state
        }
        return false;   // a valid other holder
    }

    @Override public void releaseLeadership(String nodeId) {
        session.execute(ps("DELETE FROM coord_leader WHERE id='coordinator' IF holder=?").bind(nodeId));
    }

    private static CoordNode readNode(Row r) {
        return new CoordNode(r.getString("id"), r.getString("namespace"), r.getString("cell_id"),
                r.getString("endpoint"), r.getString("region"), r.getString("engine_version"),
                r.getString("cell_fingerprint"), r.getLong("config_generation"), r.getLong("last_heartbeat"));
    }

    private static CoordNamespace readNamespace(Row r) {
        StorageConfig sc = new StorageConfig(r.getString("scheme"), r.getString("jdbc_url"),
                r.getString("db_user"), r.getString("secret_ref"), r.getInt("pool_size"));
        return new CoordNamespace(r.getString("namespace"), ProvisionState.valueOf(r.getString("state")), sc,
                r.getInt("replicas"), r.getString("region"), r.getString("endpoint"),
                r.getString("error"), r.getLong("updated_at"));
    }
}
