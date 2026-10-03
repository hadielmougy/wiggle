package com.wiggle.postgres;

import com.wiggle.jdbc.Dialect;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageFactory;
import com.wiggle.server.topology.Topology;

import java.util.ArrayList;
import java.util.List;

/**
 * Storage selection by URL scheme: an explicit switch the compiler checks, not a {@code
 * ServiceLoader} lookup. {@code jdbc:postgresql:} gets the PostgreSQL dialect, {@code jdbc:h2:} the
 * H2 one, and no URL at all keeps state in memory. A storage topology gets one such store per shard.
 *
 * <p>This lives here rather than in the runnable distribution because an application that
 * <em>embeds</em> the engine needs it too, and {@code dist} is not published. Everything it touches
 * already arrives with this module: {@code postgres} exposes {@code jdbc} as {@code api}, which
 * exposes {@code server}, so a single {@code sh.wiggle:wiggle-postgres} dependency is enough to
 * write {@code new WiggleServer(config, new PostgresStorageFactory())}.
 *
 * <p>PostgreSQL is the supported deployment backend. H2 runs in PostgreSQL-compatibility mode and is
 * here for tests and local runs: it exercises the JDBC store, its migrations and the generic claim
 * path with nothing to install, but it cannot run the {@code FOR UPDATE SKIP LOCKED} claim a real
 * cluster depends on, so it is not a deployment target.
 *
 * <p>Nothing stops an application supplying its own {@link StorageFactory} instead -- it is a
 * functional interface, and this is only the mapping the project itself ships.
 */
public class PostgresStorageFactory implements StorageFactory {

    @Override public Storage create(ServerConfig config) {
        if (config.topology() != null) return sharded(config.topology());
        String url = config.jdbcUrl();
        if (url == null || url.isBlank()) return new InMemoryStorage();
        return new JdbcStorage(url, config.jdbcUser(), config.jdbcPassword(), config.jdbcPoolSize(),
                dialect(url));
    }

    /**
     * One store per shard that holds instances or the cluster-global rows, behind a
     * {@link ShardedStorage}. Shards that only carry auth or search hold nothing the engine reads yet,
     * so they are not opened.
     */
    public static ShardedStorage sharded(Topology topology) {
        List<ShardedStorage.Member> members = new ArrayList<>();
        try {
            for (Topology.Shard s : topology.shards()) {
                boolean instances = s.has(Topology.Role.INSTANCES);
                if (!instances && !s.has(Topology.Role.HOME)) continue;
                Topology.Connection c = s.primary();
                members.add(new ShardedStorage.Member(s.id(), s.state(), instances,
                        new JdbcStorage(c.url(), c.user(), c.password(), c.pool(), dialect(c.url()))));
            }
            return new ShardedStorage(members, topology.home());
        } catch (RuntimeException e) {
            members.forEach(m -> {
                try { m.storage().close(); } catch (RuntimeException suppressed) { e.addSuppressed(suppressed); }
            });
            throw e;
        }
    }

    /** The dialect for a JDBC URL, or a clear failure naming what is supported. */
    public static Dialect dialect(String url) {
        if (url.startsWith("jdbc:postgresql:")) return new PostgresDialect();
        if (url.startsWith("jdbc:h2:")) return new H2Dialect();       // tests and local runs only
        throw new IllegalArgumentException("no storage backend for URL '" + url
                + "' -- expected jdbc:postgresql: (or jdbc:h2: for tests)");
    }
}
