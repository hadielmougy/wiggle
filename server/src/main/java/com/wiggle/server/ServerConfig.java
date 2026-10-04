package com.wiggle.server;

import com.wiggle.core.Tls;
import com.wiggle.server.topology.Topology;
import com.wiggle.server.topology.TopologyParser;

import java.time.Duration;

/**
 * Configuration with sane defaults; every field is overridable from env or system properties.
 *
 * <p>{@code dashboardUser}/{@code dashboardPassword} secure the HTTP dashboard and its JSON API
 * with HTTP Basic auth. Auth is enforced only when a password is set (the {@code /healthz}
 * endpoint is always open); with no password the dashboard is unauthenticated.
 */
public record ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                           int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                           int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                           Duration retention, int housekeepingBatch, int dashboardPort,
                           Duration queueLagCheckInterval, Duration queueLagWarnThreshold,
                           String dashboardUser, String dashboardPassword, Tls.Options tls, Memory memory,
                           Topology topology, Auth auth, Search search) {

    /**
     * Full-text search over instances. On a topology, search runs when a shard carries the search role;
     * on one database (or in memory), when {@code enabled}, on that database. {@code workflows} limits
     * which workflows are indexed (empty: all); {@code retention} is how long a document outlives its
     * instance's last change.
     */
    public record Search(boolean enabled, Duration retention, java.util.Set<String> workflows) {

        public static final Search DISABLED = new Search(false, Duration.ofDays(30), java.util.Set.of());

        public Search {
            if (retention == null || retention.isNegative() || retention.isZero()) retention = Duration.ofDays(30);
            workflows = workflows == null ? java.util.Set.of() : java.util.Set.copyOf(workflows);
        }

        public static Search fromEnvironment() {
            java.util.Set<String> workflows = new java.util.LinkedHashSet<>();
            for (String w : strProp("wiggle.search.workflows", "WIGGLE_SEARCH_WORKFLOWS", "").split(",")) {
                if (!w.isBlank()) workflows.add(w.trim());
            }
            return new Search(boolProp("wiggle.search.enabled", "WIGGLE_SEARCH_ENABLED", false),
                    Duration.ofMillis(Long.parseLong(strProp("wiggle.search.retentionMillis",
                            "WIGGLE_SEARCH_RETENTION_MILLIS", String.valueOf(Duration.ofDays(30).toMillis())).trim())),
                    workflows);
        }
    }

    /** How the gRPC API treats a call's credential ({@code WIGGLE_GRPC_AUTH}). */
    public enum GrpcAuth {
        /** No credential is read; any peer that can connect may call any RPC. */
        OFF,
        /** Credentials are checked and every call that enforcement would refuse is logged, but served. */
        LOG,
        /** A call without a known credential is UNAUTHENTICATED; one its role does not allow is PERMISSION_DENIED. */
        ENFORCE
    }

    /**
     * Who may call what. {@code grpc} is how the gRPC API checks calls; {@code cache} is how long a
     * node serves a cached account, session or credential before reading it again.
     */
    public record Auth(GrpcAuth grpc, Duration cache) {

        public static final Auth DISABLED = new Auth(GrpcAuth.OFF, Duration.ofSeconds(30));

        public Auth {
            if (grpc == null) grpc = GrpcAuth.OFF;
            if (cache == null || cache.isNegative()) cache = Duration.ofSeconds(30);
        }

        public static Auth fromEnvironment() {
            String mode = strProp("wiggle.grpc.auth", "WIGGLE_GRPC_AUTH", "off").trim().toUpperCase(java.util.Locale.ROOT);
            GrpcAuth grpc;
            try {
                grpc = GrpcAuth.valueOf(mode);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("WIGGLE_GRPC_AUTH='" + mode.toLowerCase(java.util.Locale.ROOT)
                        + "' is not one of off, log, enforce");
            }
            return new Auth(grpc, Duration.ofMillis(intProp("wiggle.auth.cacheMillis", "WIGGLE_AUTH_CACHE_MILLIS", 30_000)));
        }
    }

    /**
     * Memory-pressure admission control for worker polls. When GC-accurate heap utilization crosses
     * {@code threshold} the server is under pressure and starts <em>probabilistically</em> rejecting
     * new polls: it rejects a {@code rejectRatio} fraction of them (default 0.10 -- accept 90%,
     * reject 10%) rather than shedding all at once, easing load gently. A rejected poll returns
     * immediately empty with a jittered hold-off ({@code retryInterval} + up to {@code retryJitter})
     * telling the worker to wait before trying again. Disabled by default.
     */
    public record Memory(boolean enabled, double threshold, double rejectRatio,
                         Duration retryInterval, Duration retryJitter) {

        public static final Memory DISABLED =
                new Memory(false, 0.90, 0.10, Duration.ofSeconds(2), Duration.ofSeconds(1));

        public Memory {
            if (threshold <= 0 || threshold > 1) threshold = 0.90;
            if (rejectRatio < 0 || rejectRatio > 1) rejectRatio = 0.10;
            if (retryInterval == null) retryInterval = Duration.ofSeconds(2);
            if (retryJitter == null) retryJitter = Duration.ZERO;
        }

        public static Memory fromEnvironment() {
            return new Memory(
                    boolProp("wiggle.memory.shedding.enabled", "WIGGLE_MEMORY_SHEDDING_ENABLED", false),
                    doubleProp("wiggle.memory.threshold", "WIGGLE_MEMORY_THRESHOLD", 0.90),
                    doubleProp("wiggle.memory.rejectRatio", "WIGGLE_MEMORY_REJECT_RATIO", 0.10),
                    Duration.ofMillis(intProp("wiggle.memory.retryMillis",
                            "WIGGLE_MEMORY_RETRY_MILLIS", 2_000)),
                    Duration.ofMillis(intProp("wiggle.memory.retryJitterMillis",
                            "WIGGLE_MEMORY_RETRY_JITTER_MILLIS", 1_000)));
        }
    }

    /** Back-compat constructor: no dashboard auth (password unset), admin as the default user. */
    public ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                        int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                        int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                        Duration retention, int housekeepingBatch, int dashboardPort,
                        Duration queueLagCheckInterval, Duration queueLagWarnThreshold) {
        this(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval, heartbeatInterval,
                missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention, housekeepingBatch, dashboardPort,
                queueLagCheckInterval, queueLagWarnThreshold, "admin", null);
    }

    /** Constructor with dashboard auth but no TLS (plaintext). */
    public ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                        int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                        int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                        Duration retention, int housekeepingBatch, int dashboardPort,
                        Duration queueLagCheckInterval, Duration queueLagWarnThreshold,
                        String dashboardUser, String dashboardPassword) {
        this(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval, heartbeatInterval,
                missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention, housekeepingBatch, dashboardPort,
                queueLagCheckInterval, queueLagWarnThreshold, dashboardUser, dashboardPassword, Tls.Options.DISABLED);
    }

    /** Constructor for one database (or none): no storage topology. */
    public ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                        int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                        int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                        Duration retention, int housekeepingBatch, int dashboardPort,
                        Duration queueLagCheckInterval, Duration queueLagWarnThreshold,
                        String dashboardUser, String dashboardPassword, Tls.Options tls, Memory memory) {
        this(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval, heartbeatInterval,
                missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention, housekeepingBatch, dashboardPort,
                queueLagCheckInterval, queueLagWarnThreshold, dashboardUser, dashboardPassword, tls, memory, null, null, null);
    }

    /** Back-compat constructor: TLS but default (disabled) memory shedding. */
    public ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                        int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                        int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                        Duration retention, int housekeepingBatch, int dashboardPort,
                        Duration queueLagCheckInterval, Duration queueLagWarnThreshold,
                        String dashboardUser, String dashboardPassword, Tls.Options tls) {
        this(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval, heartbeatInterval,
                missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention, housekeepingBatch, dashboardPort,
                queueLagCheckInterval, queueLagWarnThreshold, dashboardUser, dashboardPassword, tls, Memory.DISABLED);
    }

    public ServerConfig {
        if (tls == null) tls = Tls.Options.DISABLED;
        if (memory == null) memory = Memory.DISABLED;
        if (auth == null) auth = Auth.DISABLED;
        if (search == null) search = Search.DISABLED;
    }

    /** A copy of this config searching as {@code search} says. */
    public ServerConfig withSearch(Search search) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, topology, auth, search);
    }

    /** Whether search runs: a search shard in the topology, or search enabled on the one database. */
    public boolean searchEnabled() {
        return topology != null ? !topology.searchShards().isEmpty() : search.enabled();
    }

    /** A copy of this config checking calls as {@code auth} says. */
    public ServerConfig withAuth(Auth auth) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, topology, auth, search);
    }

    /** A copy of this config on the given storage topology (null ⇒ the JDBC url, or in-memory). */
    public ServerConfig withTopology(Topology topology) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, topology, auth, search);
    }

    /** A copy of this config on the given storage (null/blank url ⇒ in-memory). */
    public ServerConfig withStorage(String jdbcUrl, String jdbcUser, String jdbcPassword, int jdbcPoolSize) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, topology, auth, search);
    }

    /** A copy of this config bound to a different gRPC port. */
    public ServerConfig withPort(int port) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, topology, auth, search);
    }

    public static ServerConfig fromEnvironment() {
        return new ServerConfig(
                intProp("wiggle.port", "WIGGLE_PORT", 8080),
                strProp("wiggle.node.name", "WIGGLE_NODE_NAME", defaultNodeName()),
                strProp("wiggle.jdbc.url", "WIGGLE_JDBC_URL", null),
                strProp("wiggle.jdbc.user", "WIGGLE_JDBC_USER", null),
                strProp("wiggle.jdbc.password", "WIGGLE_JDBC_PASSWORD", null),
                intProp("wiggle.jdbc.poolSize", "WIGGLE_JDBC_POOL_SIZE", 10),
                Duration.ofMillis(intProp("wiggle.poll.intervalMillis", "WIGGLE_POLL_INTERVAL_MILLIS", 1000)),
                Duration.ofMillis(intProp("wiggle.heartbeat.intervalMillis", "WIGGLE_HEARTBEAT_INTERVAL_MILLIS", 5000)),
                intProp("wiggle.heartbeat.missedBeforeDead", "WIGGLE_MISSED_HEARTBEATS", 3),
                Duration.ofMillis(intProp("wiggle.lease.millis", "WIGGLE_LEASE_MILLIS", 30_000)),
                Duration.ofMillis(intProp("wiggle.longpoll.maxMillis", "WIGGLE_LONGPOLL_MAX_MILLIS", 20_000)),
                Duration.ofMillis(intProp("wiggle.retention.millis", "WIGGLE_RETENTION_MILLIS", 86_400_000)),
                intProp("wiggle.housekeeping.batch", "WIGGLE_HOUSEKEEPING_BATCH", 100),
                // The read-only web dashboard. 0 = off; set a port to enable it.
                intProp("wiggle.dashboard.port", "WIGGLE_DASHBOARD_PORT", 0),
                Duration.ofMillis(intProp("wiggle.queueLag.checkIntervalMillis",
                        "WIGGLE_QUEUE_LAG_CHECK_INTERVAL_MILLIS", 5_000)),
                Duration.ofMillis(intProp("wiggle.queueLag.warnThresholdMillis",
                        "WIGGLE_QUEUE_LAG_WARN_MILLIS", 10_000)),
                // HTTP Basic credentials for the dashboard/API; no password => unauthenticated.
                strProp("wiggle.dashboard.user", "WIGGLE_DASHBOARD_USER", "admin"),
                strProp("wiggle.dashboard.password", "WIGGLE_DASHBOARD_PASSWORD", null),
                // TLS for gRPC + HTTP; no keystore => plaintext, no truststore => no client-cert (mTLS).
                Tls.Options.fromEnvironment(),
                // Memory-pressure load shedding; disabled unless WIGGLE_MEMORY_SHEDDING_ENABLED=true.
                Memory.fromEnvironment(),
                // Several databases (WIGGLE_STORAGE_TOPOLOGY); unset for one database or none.
                TopologyParser.fromEnvironment(System.getenv()).orElse(null),
                // Per-RPC authorization; off unless WIGGLE_GRPC_AUTH is log or enforce.
                Auth.fromEnvironment(),
                // Full-text search; on one database, off unless WIGGLE_SEARCH_ENABLED=true.
                Search.fromEnvironment());
    }

    public boolean isInMemory() {
        return topology == null && (jdbcUrl == null || jdbcUrl.isBlank());
    }

    /**
     * Whether this node honours a forced re-registration -- replacing the graph of an already
     * published {@code (name, version)} instead of rejecting it. A development affordance:
     * instances already running on that version have their graph swapped underneath them, one node
     * at a time, so it is off unless {@code WIGGLE_ALLOW_GRAPH_REPLACE=true}.
     */
    public static boolean allowGraphReplace() {
        return boolProp("wiggle.allowGraphReplace", "WIGGLE_ALLOW_GRAPH_REPLACE", false);
    }

    private static String defaultNodeName() {
        try {
            return java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "wiggle-node";
        }
    }

    private static String strProp(String sysProp, String env, String def) {
        String v = System.getProperty(sysProp);
        if (v == null) v = System.getenv(env);
        return v == null || v.isBlank() ? def : v;
    }

    private static int intProp(String sysProp, String env, int def) {
        String v = strProp(sysProp, env, null);
        return v == null ? def : Integer.parseInt(v.trim());
    }

    private static boolean boolProp(String sysProp, String env, boolean def) {
        String v = strProp(sysProp, env, null);
        return v == null ? def : Boolean.parseBoolean(v.trim());
    }

    private static double doubleProp(String sysProp, String env, double def) {
        String v = strProp(sysProp, env, null);
        return v == null ? def : Double.parseDouble(v.trim());
    }
}
