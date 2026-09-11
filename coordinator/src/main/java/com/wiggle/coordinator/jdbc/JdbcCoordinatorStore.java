package com.wiggle.coordinator.jdbc;

import com.wiggle.core.Json;
import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.EpochCodec;
import com.wiggle.server.coord.ProvisionState;
import com.wiggle.server.coord.StorageConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link CoordinatorStore} backed by a relational database — the "stateless coordinator + your
 * existing database" backend, an alternative to the embedded Ratis+RocksDB one. The coordinator's
 * state is small, read-mostly and rarely written (placement policy, node roster, definition and
 * namespace registries — never per-instance), so a database is a natural, operationally-boring fit
 * for teams that already run a managed HA database: they inherit its backup/DR/monitoring, and the
 * coordinator process becomes stateless and horizontally scalable, with single-writer coordination
 * provided by a durable leader lease ({@link #acquireLeadership}).
 *
 * <p>All consistency is provided by the database. Every mutation is either a conditional {@code
 * UPDATE} (compare-and-set on a revision / lease, atomic on any engine) or an {@code INSERT} whose
 * primary key makes the claim atomic — no explicit locking, and portable across PostgreSQL, H2,
 * MySQL, and friends. The only per-database detail is the large-text column type, detected from the
 * JDBC metadata at migrate time. This module deliberately does NOT depend on the engine's {@code
 * jdbc} storage module (which would pull in {@code :server}); it talks to the database directly.
 */
public final class JdbcCoordinatorStore implements CoordinatorStore {

    private final DataSource ds;

    public JdbcCoordinatorStore(DataSource ds) {
        this.ds = ds;
        migrate();
    }

    // ---------------------------------------------------------------- schema

    private void migrate() {
        try (Connection c = ds.getConnection()) {
            String text = textType(c);
            createIfAbsent(c, "coord_policy",
                    "CREATE TABLE coord_policy (" +
                            "namespace VARCHAR(200) PRIMARY KEY, current_epoch BIGINT NOT NULL, " +
                            "revision BIGINT NOT NULL, epochs " + text + " NOT NULL)");
            createIfAbsent(c, "coord_node",
                    "CREATE TABLE coord_node (" +
                            "id VARCHAR(200) PRIMARY KEY, namespace VARCHAR(200) NOT NULL, " +
                            "cell_id VARCHAR(200), endpoint VARCHAR(400), region VARCHAR(100), " +
                            "engine_version VARCHAR(100), cell_fingerprint VARCHAR(200), " +
                            "config_generation BIGINT NOT NULL, last_heartbeat BIGINT NOT NULL)");
            createIfAbsent(c, "coord_definition",
                    "CREATE TABLE coord_definition (" +
                            "namespace VARCHAR(200) NOT NULL, name VARCHAR(300) NOT NULL, " +
                            "version INT NOT NULL, hash VARCHAR(128), registered_at BIGINT NOT NULL, " +
                            "PRIMARY KEY (namespace, name))");
            createIfAbsent(c, "coord_namespace",
                    "CREATE TABLE coord_namespace (" +
                            "namespace VARCHAR(200) PRIMARY KEY, state VARCHAR(40) NOT NULL, " +
                            "storage " + text + ", replicas INT NOT NULL, region VARCHAR(100), " +
                            "endpoint VARCHAR(400), err " + text + ", updated_at BIGINT NOT NULL)");
            createIfAbsent(c, "coord_cell_binding",
                    "CREATE TABLE coord_cell_binding (" +
                            "namespace VARCHAR(200) NOT NULL, cell_id VARCHAR(200) NOT NULL, " +
                            "fingerprint VARCHAR(200) NOT NULL, PRIMARY KEY (namespace, cell_id))");
            createIfAbsent(c, "coord_leader",
                    "CREATE TABLE coord_leader (" +
                            "id INT PRIMARY KEY, holder VARCHAR(200), expiry BIGINT NOT NULL)");
        } catch (SQLException e) {
            throw new RuntimeException("coordinator store migration failed", e);
        }
    }

    /** The portable large-text column type for JSON blobs, per database. */
    private static String textType(Connection c) throws SQLException {
        String db = c.getMetaData().getDatabaseProductName().toLowerCase();
        if (db.contains("postgres")) return "text";
        if (db.contains("mysql") || db.contains("maria")) return "longtext";
        if (db.contains("sql server") || db.contains("microsoft")) return "nvarchar(max)";
        return "clob"; // H2, Oracle, and a safe default — getString/setString work everywhere
    }

    private static void createIfAbsent(Connection c, String table, String ddl) throws SQLException {
        if (tableExists(c, table)) return;
        try (Statement s = c.createStatement()) {
            s.execute(ddl);
        } catch (SQLException e) {
            if (!tableExists(c, table)) throw e; // ignore a lost create race; rethrow real failures
        }
    }

    private static boolean tableExists(Connection c, String table) throws SQLException {
        DatabaseMetaData md = c.getMetaData();
        for (String t : new String[]{table, table.toUpperCase(), table.toLowerCase()}) {
            try (ResultSet rs = md.getTables(null, null, t, new String[]{"TABLE"})) {
                if (rs.next()) return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------- policy (CAS-guarded)

    @Override public Optional<CoordPolicy> getPolicy(String namespace) {
        return query("SELECT * FROM coord_policy WHERE namespace = ?",
                ps -> ps.setString(1, namespace),
                rs -> rs.next() ? Optional.of(readPolicy(rs)) : Optional.<CoordPolicy>empty());
    }

    @Override public List<CoordPolicy> listPolicies() {
        return query("SELECT * FROM coord_policy", ps -> { }, rs -> {
            List<CoordPolicy> out = new ArrayList<>();
            while (rs.next()) out.add(readPolicy(rs));
            return out;
        });
    }

    @Override public long casPolicy(String namespace, long expectedRevision, CoordPolicy desired) {
        String epochs = EpochCodec.encode(desired.epochs());
        try (Connection c = ds.getConnection()) {
            if (expectedRevision == 0) {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO coord_policy (namespace, current_epoch, revision, epochs) VALUES (?,?,?,?)")) {
                    ps.setString(1, namespace);
                    ps.setLong(2, desired.currentEpoch());
                    ps.setLong(3, 1);
                    ps.setString(4, epochs);
                    ps.executeUpdate();
                    return 1;
                } catch (SQLException raced) {
                    if (isConstraint(raced)) return -1; // a policy already exists
                    throw raced;
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE coord_policy SET current_epoch = ?, revision = ?, epochs = ? " +
                            "WHERE namespace = ? AND revision = ?")) {
                ps.setLong(1, desired.currentEpoch());
                ps.setLong(2, expectedRevision + 1);
                ps.setString(3, epochs);
                ps.setString(4, namespace);
                ps.setLong(5, expectedRevision);
                return ps.executeUpdate() == 1 ? expectedRevision + 1 : -1;
            }
        } catch (SQLException e) {
            throw new RuntimeException("casPolicy failed", e);
        }
    }

    private static CoordPolicy readPolicy(ResultSet rs) throws SQLException {
        return new CoordPolicy(rs.getString("namespace"), rs.getLong("current_epoch"),
                rs.getLong("revision"), EpochCodec.decode(rs.getString("epochs")));
    }

    // ---------------------------------------------------------------- node roster

    private static final String NODE_UPDATE =
            "UPDATE coord_node SET namespace=?, cell_id=?, endpoint=?, region=?, engine_version=?, " +
                    "cell_fingerprint=?, config_generation=?, last_heartbeat=? WHERE id=?";

    @Override public void upsertNode(CoordNode n) {
        // update-else-insert (portable, no ON CONFLICT): update; if the row was absent, insert;
        // on a lost insert race, fall back to update.
        if (rowCount(NODE_UPDATE, ps -> bindNodeUpdate(ps, n)) == 0) {
            try {
                rowCount("INSERT INTO coord_node (namespace, cell_id, endpoint, region, engine_version, " +
                                "cell_fingerprint, config_generation, last_heartbeat, id) VALUES (?,?,?,?,?,?,?,?,?)",
                        ps -> bindNodeUpdate(ps, n));
            } catch (RuntimeException e) {
                if (!rootIsConstraint(e)) throw e;
                rowCount(NODE_UPDATE, ps -> bindNodeUpdate(ps, n));
            }
        }
    }

    private static void bindNodeUpdate(PreparedStatement ps, CoordNode n) throws SQLException {
        ps.setString(1, n.namespace());
        ps.setString(2, n.cellId());
        ps.setString(3, n.endpoint());
        ps.setString(4, n.region());
        ps.setString(5, n.engineVersion());
        ps.setString(6, n.cellFingerprint());
        ps.setLong(7, n.configGeneration());
        ps.setLong(8, n.lastHeartbeat());
        ps.setString(9, n.id());
    }

    @Override public Optional<CoordNode> node(String id) {
        return query("SELECT * FROM coord_node WHERE id = ?", ps -> ps.setString(1, id),
                rs -> rs.next() ? Optional.of(readNode(rs)) : Optional.<CoordNode>empty());
    }

    @Override public List<CoordNode> nodes(String namespace) {
        return query("SELECT * FROM coord_node WHERE namespace = ?", ps -> ps.setString(1, namespace), rs -> {
            List<CoordNode> out = new ArrayList<>();
            while (rs.next()) out.add(readNode(rs));
            return out;
        });
    }

    @Override public Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration) {
        int n = rowCount("UPDATE coord_node SET last_heartbeat = ?, config_generation = ? WHERE id = ?",
                ps -> { ps.setLong(1, lastHeartbeat); ps.setLong(2, configGeneration); ps.setString(3, id); });
        return n == 1 ? node(id) : Optional.empty();
    }

    @Override public void removeNode(String id) {
        rowCount("DELETE FROM coord_node WHERE id = ?", ps -> ps.setString(1, id));
    }

    @Override public int expireNodes(long deadlineMillis) {
        return rowCount("DELETE FROM coord_node WHERE last_heartbeat < ?", ps -> ps.setLong(1, deadlineMillis));
    }

    private static CoordNode readNode(ResultSet rs) throws SQLException {
        return new CoordNode(rs.getString("id"), rs.getString("namespace"), rs.getString("cell_id"),
                rs.getString("endpoint"), rs.getString("region"), rs.getString("engine_version"),
                rs.getString("cell_fingerprint"), rs.getLong("config_generation"), rs.getLong("last_heartbeat"));
    }

    // ---------------------------------------------------------------- cell-identity binding

    @Override public boolean bindCell(String namespace, String cellId, String fingerprint) {
        if (fingerprint == null) return true;
        try {
            rowCount("INSERT INTO coord_cell_binding (namespace, cell_id, fingerprint) VALUES (?,?,?)",
                    ps -> { ps.setString(1, namespace); ps.setString(2, cellId); ps.setString(3, fingerprint); });
            return true; // claimed
        } catch (RuntimeException e) {
            if (!rootIsConstraint(e)) throw e;
            // already bound — the claim holds only if the existing fingerprint equals ours
            String held = query("SELECT fingerprint FROM coord_cell_binding WHERE namespace = ? AND cell_id = ?",
                    ps -> { ps.setString(1, namespace); ps.setString(2, cellId); },
                    rs -> rs.next() ? rs.getString(1) : null);
            return fingerprint.equals(held);
        }
    }

    @Override public int pruneOrphanCellBindings() {
        return rowCount("DELETE FROM coord_cell_binding WHERE NOT EXISTS (" +
                "SELECT 1 FROM coord_node n WHERE n.namespace = coord_cell_binding.namespace " +
                "AND n.cell_id = coord_cell_binding.cell_id)", ps -> { });
    }

    // ---------------------------------------------------------------- definition registry

    @Override public Optional<CoordDefinition> getDefinition(String namespace, String name) {
        return query("SELECT * FROM coord_definition WHERE namespace = ? AND name = ?",
                ps -> { ps.setString(1, namespace); ps.setString(2, name); },
                rs -> rs.next() ? Optional.of(readDefinition(rs)) : Optional.<CoordDefinition>empty());
    }

    @Override public void putDefinition(CoordDefinition d) {
        if (rowCount("UPDATE coord_definition SET version=?, hash=?, registered_at=? WHERE namespace=? AND name=?",
                ps -> bindDefUpdate(ps, d)) == 0) {
            try {
                rowCount("INSERT INTO coord_definition (version, hash, registered_at, namespace, name) " +
                        "VALUES (?,?,?,?,?)", ps -> bindDefUpdate(ps, d));
            } catch (RuntimeException e) {
                if (!rootIsConstraint(e)) throw e;
                rowCount("UPDATE coord_definition SET version=?, hash=?, registered_at=? WHERE namespace=? AND name=?",
                        ps -> bindDefUpdate(ps, d));
            }
        }
    }

    private static void bindDefUpdate(PreparedStatement ps, CoordDefinition d) throws SQLException {
        ps.setInt(1, d.version());
        ps.setString(2, d.hash());
        ps.setLong(3, d.registeredAt());
        ps.setString(4, d.namespace());
        ps.setString(5, d.name());
    }

    @Override public boolean removeDefinition(String namespace, String name) {
        return rowCount("DELETE FROM coord_definition WHERE namespace = ? AND name = ?",
                ps -> { ps.setString(1, namespace); ps.setString(2, name); }) > 0;
    }

    @Override public List<CoordDefinition> definitions(String namespace) {
        return query("SELECT * FROM coord_definition WHERE namespace = ?", ps -> ps.setString(1, namespace), rs -> {
            List<CoordDefinition> out = new ArrayList<>();
            while (rs.next()) out.add(readDefinition(rs));
            return out;
        });
    }

    private static CoordDefinition readDefinition(ResultSet rs) throws SQLException {
        return new CoordDefinition(rs.getString("namespace"), rs.getString("name"),
                rs.getInt("version"), rs.getString("hash"), rs.getLong("registered_at"));
    }

    // ---------------------------------------------------------------- namespace registry

    @Override public Optional<CoordNamespace> getNamespace(String namespace) {
        return query("SELECT * FROM coord_namespace WHERE namespace = ?", ps -> ps.setString(1, namespace),
                rs -> rs.next() ? Optional.of(readNamespace(rs)) : Optional.<CoordNamespace>empty());
    }

    @Override public List<CoordNamespace> namespaces() {
        return query("SELECT * FROM coord_namespace", ps -> { }, rs -> {
            List<CoordNamespace> out = new ArrayList<>();
            while (rs.next()) out.add(readNamespace(rs));
            return out;
        });
    }

    @Override public void putNamespace(CoordNamespace ns) {
        if (rowCount("UPDATE coord_namespace SET state=?, storage=?, replicas=?, region=?, endpoint=?, " +
                "err=?, updated_at=? WHERE namespace=?", ps -> bindNamespaceUpdate(ps, ns)) == 0) {
            try {
                rowCount("INSERT INTO coord_namespace (state, storage, replicas, region, endpoint, err, " +
                        "updated_at, namespace) VALUES (?,?,?,?,?,?,?,?)", ps -> bindNamespaceUpdate(ps, ns));
            } catch (RuntimeException e) {
                if (!rootIsConstraint(e)) throw e;
                rowCount("UPDATE coord_namespace SET state=?, storage=?, replicas=?, region=?, endpoint=?, " +
                        "err=?, updated_at=? WHERE namespace=?", ps -> bindNamespaceUpdate(ps, ns));
            }
        }
    }

    private static void bindNamespaceUpdate(PreparedStatement ps, CoordNamespace ns) throws SQLException {
        ps.setString(1, ns.state().name());
        ps.setString(2, encodeStorage(ns.storage()));
        ps.setInt(3, ns.replicas());
        ps.setString(4, ns.region());
        ps.setString(5, ns.endpoint());
        ps.setString(6, ns.error());
        ps.setLong(7, ns.updatedAt());
        ps.setString(8, ns.namespace());
    }

    private static CoordNamespace readNamespace(ResultSet rs) throws SQLException {
        return new CoordNamespace(rs.getString("namespace"), ProvisionState.valueOf(rs.getString("state")),
                decodeStorage(rs.getString("storage")), rs.getInt("replicas"), rs.getString("region"),
                rs.getString("endpoint"), rs.getString("err"), rs.getLong("updated_at"));
    }

    /** StorageConfig as a small JSON blob (field names match the Ratis backend's, for one on-disk form). */
    private static String encodeStorage(StorageConfig sc) {
        if (sc == null) return null;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("scheme", sc.scheme());
        m.put("jdbcUrl", sc.jdbcUrl());
        m.put("user", sc.user());
        m.put("secretRef", sc.secretRef());
        m.put("poolSize", sc.poolSize());
        return Json.write(m);
    }

    private static StorageConfig decodeStorage(String json) {
        if (json == null || json.isBlank()) return null;
        Map<String, Object> m = Json.parseObject(json);
        return new StorageConfig(Json.str(m, "scheme", null), Json.str(m, "jdbcUrl", null),
                Json.str(m, "user", null), Json.str(m, "secretRef", null), (int) Json.num(m, "poolSize", 0));
    }

    // ---------------------------------------------------------------- leader election (durable lease)

    @Override public boolean acquireLeadership(String nodeId, long nowMillis, long leaseMillis) {
        long expiry = nowMillis + leaseMillis;
        // Take or renew the single lease row: succeeds if unheld, expired, or already ours.
        int updated = rowCount("UPDATE coord_leader SET holder = ?, expiry = ? " +
                "WHERE id = 1 AND (holder IS NULL OR expiry <= ? OR holder = ?)",
                ps -> { ps.setString(1, nodeId); ps.setLong(2, expiry); ps.setLong(3, nowMillis); ps.setString(4, nodeId); });
        if (updated == 1) return true;
        // Either the row is absent (first ever acquisition) or a valid holder owns it. Try to create it:
        // an INSERT wins iff it was absent; a PK conflict means a valid holder got there first.
        try {
            rowCount("INSERT INTO coord_leader (id, holder, expiry) VALUES (1, ?, ?)",
                    ps -> { ps.setString(1, nodeId); ps.setLong(2, expiry); });
            return true;
        } catch (RuntimeException e) {
            if (rootIsConstraint(e)) return false;
            throw e;
        }
    }

    @Override public void releaseLeadership(String nodeId) {
        rowCount("UPDATE coord_leader SET holder = NULL, expiry = 0 WHERE id = 1 AND holder = ?",
                ps -> ps.setString(1, nodeId));
    }

    /** Closes the backing connection pool if it owns one (a HikariDataSource is AutoCloseable). */
    @Override public void close() {
        if (ds instanceof AutoCloseable ac) {
            try { ac.close(); } catch (Exception ignored) { }
        }
    }

    // ---------------------------------------------------------------- tiny JDBC helpers

    @FunctionalInterface private interface Binder { void bind(PreparedStatement ps) throws SQLException; }
    @FunctionalInterface private interface Reader<T> { T read(ResultSet rs) throws SQLException; }

    private <T> T query(String sql, Binder binder, Reader<T> reader) {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return reader.read(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException("coordinator query failed: " + sql, e);
        }
    }

    private int rowCount(String sql, Binder binder) {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcException(sql, e);
        }
    }

    private static boolean rootIsConstraint(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException se && isConstraint(se)) return true;
        }
        return false;
    }

    /**
     * A duplicate-key / integrity violation, detected portably by SQLState. The SQL-standard class
     * {@code 23} is "integrity constraint violation" — PostgreSQL uses {@code 23505}, MySQL
     * {@code 23000}, H2 {@code 23505} — which is far more reliable across drivers than the exception
     * type (PostgreSQL throws a plain {@code SQLException}, not {@code SQLIntegrityConstraintViolationException}).
     */
    private static boolean isConstraint(SQLException e) {
        if (e instanceof SQLIntegrityConstraintViolationException) return true;
        String state = e.getSQLState();
        return state != null && state.startsWith("23");
    }

    /** A RuntimeException carrying the SQLException, so callers can inspect the cause for a constraint hit. */
    static final class JdbcException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        JdbcException(String sql, SQLException cause) { super("coordinator write failed: " + sql, cause); }
    }
}
