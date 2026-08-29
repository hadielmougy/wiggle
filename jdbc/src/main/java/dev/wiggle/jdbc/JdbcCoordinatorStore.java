package dev.wiggle.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.wiggle.core.Json;
import dev.wiggle.server.coord.CoordDefinition;
import dev.wiggle.server.coord.CoordNode;
import dev.wiggle.server.coord.CoordPolicy;
import dev.wiggle.server.coord.CoordPolicy.EpochRing;
import dev.wiggle.server.coord.CoordPolicy.EpochStatus;
import dev.wiggle.server.coord.CoordPolicy.RingSlot;
import dev.wiggle.server.coord.CoordinatorStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC {@link CoordinatorStore} over the {@code coord_*} tables (created by
 * {@link JdbcStorage#COORDINATOR_MIGRATIONS}). Policy writes are a compare-and-set on the
 * {@code revision} column, which makes the tolerated brief-overlap leader election safe: a stale
 * ex-leader's update matches zero rows.
 *
 * <p>The coordinator runs on its own database, so this owns its own connection pool (autocommit --
 * every op is a single atomic statement). The epoch ring history is stored as a JSON blob in
 * {@code coord_policy.epochs}.
 */
public final class JdbcCoordinatorStore implements CoordinatorStore {

    private final DataSource ds;
    private final boolean ownsPool;

    /** Standalone: owns a dedicated pool (used by tests). */
    public JdbcCoordinatorStore(String url, String user, String password, int poolSize, Dialect dialect) {
        Objects.requireNonNull(dialect, "dialect");
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        if (user != null) cfg.setUsername(user);
        if (password != null) cfg.setPassword(password);
        cfg.setMaximumPoolSize(Math.max(1, poolSize));
        cfg.setAutoCommit(true);   // each coordinator op is one atomic statement (CAS via WHERE revision=?)
        cfg.setPoolName("wiggle-coord-" + dialect.id());
        this.ds = new HikariDataSource(cfg);
        this.ownsPool = true;
    }

    /** Shares an existing pool (e.g. the coordinator node's {@link JdbcStorage}); does not close it. */
    public JdbcCoordinatorStore(DataSource sharedPool) {
        this.ds = Objects.requireNonNull(sharedPool, "sharedPool");
        this.ownsPool = false;
    }

    private Connection borrow() {
        try {
            Connection c = ds.getConnection();
            // Each coordinator op is a single atomic statement; force autocommit even when borrowing
            // from a shared pool configured for manual commit (Hikari restores the default on return).
            c.setAutoCommit(true);
            return c;
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("cannot obtain connection", e);
        }
    }

    // ---- policy ----

    @Override public Optional<CoordPolicy> getPolicy(String namespace) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement(
                     "SELECT current_epoch, epochs, revision FROM coord_policy WHERE namespace=?")) {
            p.setString(1, namespace);
            try (ResultSet rs = p.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new CoordPolicy(namespace, rs.getLong(1), rs.getLong(3),
                        decodeEpochs(rs.getString(2))));
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("getPolicy failed", e);
        }
    }

    @Override public List<CoordPolicy> listPolicies() {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement("SELECT namespace, current_epoch, epochs, revision FROM coord_policy");
             ResultSet rs = p.executeQuery()) {
            List<CoordPolicy> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new CoordPolicy(rs.getString(1), rs.getLong(2), rs.getLong(4), decodeEpochs(rs.getString(3))));
            }
            return out;
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("listPolicies failed", e);
        }
    }

    @Override public long casPolicy(String namespace, long expectedRevision, CoordPolicy desired) {
        String epochs = encodeEpochs(desired.epochs());
        try (Connection c = borrow()) {
            if (expectedRevision == 0) {
                // Create only if absent. Coordinator writes are leader-gated (single writer), so a
                // check-then-insert is safe here; the update path below is the true CAS fence.
                if (getPolicy(namespace).isPresent()) return -1;
                try (PreparedStatement p = c.prepareStatement(
                        "INSERT INTO coord_policy (namespace, current_epoch, epochs, revision) VALUES (?,?,?,1)")) {
                    p.setString(1, namespace);
                    p.setLong(2, desired.currentEpoch());
                    p.setString(3, epochs);
                    p.executeUpdate();
                }
                return 1;
            }
            long next = expectedRevision + 1;
            try (PreparedStatement p = c.prepareStatement(
                    "UPDATE coord_policy SET current_epoch=?, epochs=?, revision=? WHERE namespace=? AND revision=?")) {
                p.setLong(1, desired.currentEpoch());
                p.setString(2, epochs);
                p.setLong(3, next);
                p.setString(4, namespace);
                p.setLong(5, expectedRevision);
                return p.executeUpdate() == 1 ? next : -1;
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("casPolicy failed", e);
        }
    }

    // ---- node roster ----

    @Override public void upsertNode(CoordNode n) {
        try (Connection c = borrow()) {
            try (PreparedStatement up = c.prepareStatement(
                    "UPDATE coord_node SET namespace=?, endpoint=?, region=?, engine_version=?, " +
                    "config_generation=?, last_heartbeat=? WHERE id=?")) {
                up.setString(1, n.namespace()); up.setString(2, n.endpoint()); up.setString(3, n.region());
                up.setString(4, n.engineVersion()); up.setLong(5, n.configGeneration());
                up.setLong(6, n.lastHeartbeat()); up.setString(7, n.id());
                if (up.executeUpdate() == 1) return;
            }
            try (PreparedStatement ins = c.prepareStatement(
                    "INSERT INTO coord_node (id, namespace, endpoint, region, engine_version, " +
                    "config_generation, last_heartbeat) VALUES (?,?,?,?,?,?,?)")) {
                ins.setString(1, n.id()); ins.setString(2, n.namespace()); ins.setString(3, n.endpoint());
                ins.setString(4, n.region()); ins.setString(5, n.engineVersion());
                ins.setLong(6, n.configGeneration()); ins.setLong(7, n.lastHeartbeat());
                ins.executeUpdate();
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("upsertNode failed", e);
        }
    }

    @Override public Optional<CoordNode> node(String id) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement(
                     "SELECT namespace, endpoint, region, engine_version, config_generation, last_heartbeat " +
                     "FROM coord_node WHERE id=?")) {
            p.setString(1, id);
            try (ResultSet rs = p.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new CoordNode(id, rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getString(4), rs.getLong(5), rs.getLong(6)));
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("node failed", e);
        }
    }

    @Override public Optional<CoordNode> touchNode(String id, long lastHeartbeat, long configGeneration) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement(
                     "UPDATE coord_node SET last_heartbeat=?, config_generation=? WHERE id=?")) {
            p.setLong(1, lastHeartbeat);
            p.setLong(2, configGeneration);
            p.setString(3, id);
            if (p.executeUpdate() != 1) return Optional.empty();
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("touchNode failed", e);
        }
        return node(id);
    }

    @Override public void removeNode(String id) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement("DELETE FROM coord_node WHERE id=?")) {
            p.setString(1, id);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("removeNode failed", e);
        }
    }

    @Override public List<CoordNode> nodes(String namespace) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement(
                     "SELECT id, endpoint, region, engine_version, config_generation, last_heartbeat " +
                     "FROM coord_node WHERE namespace=?")) {
            p.setString(1, namespace);
            try (ResultSet rs = p.executeQuery()) {
                List<CoordNode> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new CoordNode(rs.getString(1), namespace, rs.getString(2), rs.getString(3),
                            rs.getString(4), rs.getLong(5), rs.getLong(6)));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("nodes failed", e);
        }
    }

    @Override public int expireNodes(long deadlineMillis) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement("DELETE FROM coord_node WHERE last_heartbeat<?")) {
            p.setLong(1, deadlineMillis);
            return p.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("expireNodes failed", e);
        }
    }

    // ---- definitions ----

    @Override public Optional<CoordDefinition> getDefinition(String namespace, String name) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement(
                     "SELECT version, hash, registered_at FROM coord_definition WHERE namespace=? AND name=?")) {
            p.setString(1, namespace); p.setString(2, name);
            try (ResultSet rs = p.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new CoordDefinition(namespace, name, rs.getInt(1), rs.getString(2), rs.getLong(3)));
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("getDefinition failed", e);
        }
    }

    @Override public void putDefinition(CoordDefinition d) {
        try (Connection c = borrow()) {
            try (PreparedStatement up = c.prepareStatement(
                    "UPDATE coord_definition SET version=?, hash=?, registered_at=? WHERE namespace=? AND name=?")) {
                up.setInt(1, d.version()); up.setString(2, d.hash()); up.setLong(3, d.registeredAt());
                up.setString(4, d.namespace()); up.setString(5, d.name());
                if (up.executeUpdate() == 1) return;
            }
            try (PreparedStatement ins = c.prepareStatement(
                    "INSERT INTO coord_definition (namespace, name, version, hash, registered_at) VALUES (?,?,?,?,?)")) {
                ins.setString(1, d.namespace()); ins.setString(2, d.name()); ins.setInt(3, d.version());
                ins.setString(4, d.hash()); ins.setLong(5, d.registeredAt());
                ins.executeUpdate();
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("putDefinition failed", e);
        }
    }

    @Override public List<CoordDefinition> definitions(String namespace) {
        try (Connection c = borrow();
             PreparedStatement p = c.prepareStatement(
                     "SELECT name, version, hash, registered_at FROM coord_definition WHERE namespace=?")) {
            p.setString(1, namespace);
            try (ResultSet rs = p.executeQuery()) {
                List<CoordDefinition> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new CoordDefinition(namespace, rs.getString(1), rs.getInt(2), rs.getString(3), rs.getLong(4)));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new JdbcStorage.StorageException("definitions failed", e);
        }
    }

    @Override public void close() {
        if (ownsPool && ds instanceof HikariDataSource h) h.close();
    }

    // ---- epoch JSON codec ----

    private static String encodeEpochs(Map<Long, EpochRing> epochs) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<Long, EpochRing> e : epochs.entrySet()) {
            EpochRing er = e.getValue();
            List<Object> ring = new ArrayList<>();
            for (RingSlot s : er.ring()) {
                Map<String, Object> slot = new LinkedHashMap<>();
                slot.put("shard", s.shard());
                slot.put("cellId", s.cellId());
                slot.put("region", s.region());
                ring.add(slot);
            }
            Map<String, Object> ringObj = new LinkedHashMap<>();
            ringObj.put("status", er.status().name());
            ringObj.put("ring", ring);
            out.put(Long.toString(e.getKey()), ringObj);
        }
        return Json.write(out);
    }

    private static Map<Long, EpochRing> decodeEpochs(String json) {
        Map<Long, EpochRing> out = new LinkedHashMap<>();
        if (json == null || json.isBlank()) return out;
        Map<String, Object> obj = Json.parseObject(json);
        for (Map.Entry<String, Object> e : obj.entrySet()) {
            Map<String, Object> er = Json.asObject(e.getValue());
            EpochStatus status = EpochStatus.valueOf(Json.reqStr(er, "status"));
            List<RingSlot> ring = new ArrayList<>();
            for (Object o : Json.asArray(er.get("ring"))) {
                Map<String, Object> sm = Json.asObject(o);
                ring.add(new RingSlot((int) Json.num(sm, "shard", 0), Json.reqStr(sm, "cellId"),
                        Json.str(sm, "region", null)));
            }
            out.put(Long.parseLong(e.getKey()), new EpochRing(ring, status));
        }
        return out;
    }
}
