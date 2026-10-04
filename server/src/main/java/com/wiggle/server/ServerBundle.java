package com.wiggle.server;

import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.search.Search;
import com.wiggle.server.search.SearchIndex;
import com.wiggle.server.search.SearchIndexer;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.topology.Topology;
import com.wiggle.server.grpc.Authorizer;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.cluster.Housekeeper;
import com.wiggle.server.cluster.QueueLagMonitor;
import com.wiggle.server.cluster.ReplicaMonitor;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.InstanceIds;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.grpc.GrpcApi;
import com.wiggle.server.http.HealthServer;
import com.wiggle.server.store.Storage;
import com.wiggle.server.topology.Placement;

import java.io.IOException;
import java.util.List;

/**
 * The subsystems of a {@link WiggleServer}: everything a node runs <em>beyond</em> the shared
 * storage + {@link com.wiggle.server.cluster.ClusterManager} -- the engine, its housekeeping and the
 * control plane.
 */
final class ServerBundle {

    private static final System.Logger LOG = System.getLogger(ServerBundle.class.getName());

    private final WorkflowEngine engine;
    private final Housekeeper housekeeper;
    private final ReplicaMonitor replicaMonitor;
    private final QueueLagMonitor queueLagMonitor;
    private final GrpcApi api;
    private final Search search;
    private final SearchIndexer indexer;
    /** A {@code /healthz} probe endpoint for Kubernetes, on the configured port; null if none. */
    private final HealthServer health;

    ServerBundle(ServerConfig config, Storage storage, ClusterManager cluster, AuthCache authCache) throws IOException {
        super();
        this.engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), config.defaultLease().toMillis(),
                config.topology() == null ? InstanceIds.across(storage.instanceShards())
                        : new Placement(config.topology(), System::currentTimeMillis));
        this.replicaMonitor = new ReplicaMonitor(storage, cluster, 1_000);
        this.housekeeper = new Housekeeper(engine, cluster, config.pollInterval(),
                config.retention(), config.housekeepingBatch(), Housekeeper.adaptiveByDefault(),
                config.defaultLease());
        this.queueLagMonitor = new QueueLagMonitor(engine, cluster,
                config.queueLagCheckInterval(), config.queueLagWarnThreshold());
        if (config.searchEnabled()) {
            SearchIndex index = new SearchIndex(storage, searchTargets(config, storage));
            this.search = new Search(index, engine);
            this.indexer = new SearchIndexer(engine, storage, index, cluster, config.search().retention().toMillis(),
                    config.search().workflows(), System::currentTimeMillis);
        } else {
            this.search = null;
            this.indexer = null;
        }
        this.api = new GrpcApi(engine, cluster, config.port(), config.maxLongPoll().toMillis(),
                config.tls(), config.memory(), new Authorizer(config.auth().grpc(), authCache), search);
        // The dashboard moved to the console; the former dashboard port now serves only /healthz.
        this.health = config.dashboardPort() <= 0 ? null : new HealthServer(config.dashboardPort());
    }

    /** The search shards: the topology's, or the one database's when search is enabled on it. */
    private static List<SearchIndex.Target> searchTargets(ServerConfig config, Storage storage) {
        Topology t = config.topology();
        if (t == null) {
            LOG.log(System.Logger.Level.WARNING, "search runs on the instance database: indexing shares its write "
                    + "capacity; give search its own shard in a storage topology to keep them apart");
            return List.of(new SearchIndex.Target(storage.home(), ShardState.ACTIVE));
        }
        for (Topology.Shard s : t.searchShards()) {
            if (s.has(Topology.Role.INSTANCES)) {
                LOG.log(System.Logger.Level.WARNING, () -> "shard " + s.id() + " carries both instances and search: "
                        + "indexing shares its write capacity");
            }
        }
        return t.searchShards().stream().map(s -> new SearchIndex.Target(s.id(), s.state())).toList();
    }

    public void start() {
        if (indexer != null) indexer.start(250);
        housekeeper.start();
        replicaMonitor.start();
        queueLagMonitor.start();
        api.start();
        if (health != null) health.start();
    }

    public void close() {
        if (health != null) health.close();
        api.close();
        if (indexer != null) indexer.close();
        queueLagMonitor.close();
        housekeeper.close();
        replicaMonitor.close();
    }

    public int port() { return api.port(); }


    public WorkflowEngine engine() { return engine; }

    /** Full-text search, or null when search is not enabled. */
    public Search search() { return search; }
}
