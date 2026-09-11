package com.wiggle.coordinator.jdbc;

import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.CoordinatorStoreProvider;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * The {@link CoordinatorStoreProvider} seam for the JDBC backend — the "point the coordinator at your
 * existing database" option, resolved by {@code dist} the same way the Ratis backend is. It builds a
 * pooled {@link HikariDataSource} from a plain JDBC URL and hands back a {@link JdbcCoordinatorStore}
 * (which migrates its {@code coord_*} schema on construction and owns the pool). The JDBC driver is
 * supplied at runtime by the distribution (which bundles every backend's driver).
 *
 * <p>Wire it with {@code WIGGLE_COORD_STORE=jdbc:postgresql://host:5432/wiggle_coord} plus
 * {@code WIGGLE_COORD_JDBC_USER} / {@code WIGGLE_COORD_JDBC_PASSWORD}. Give the coordinator its own
 * small database (or schema) — never a tenant cell's — so the control plane's blast radius stays
 * separate. Several coordinator processes may point at the same database; they stay single-writer via
 * the store's durable leader lease.
 */
public final class JdbcCoordinatorStoreProvider implements CoordinatorStoreProvider {

    private final JdbcCoordinatorStore store;

    public JdbcCoordinatorStoreProvider(String jdbcUrl, String user, String password, int poolSize) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(jdbcUrl);
        if (user != null && !user.isBlank()) cfg.setUsername(user);
        if (password != null && !password.isBlank()) cfg.setPassword(password);
        cfg.setMaximumPoolSize(poolSize > 0 ? poolSize : 4);
        cfg.setPoolName("wiggle-coord");
        this.store = new JdbcCoordinatorStore(new HikariDataSource(cfg));
    }

    @Override public CoordinatorStore coordinatorStore() {
        return store;
    }
}
