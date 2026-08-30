package com.wiggle.server;

import com.wiggle.core.Tls;

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
                           String namespace) {

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

    /** Back-compat: the pre-namespace canonical signature; defaults {@code namespace} to none. */
    public ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                        int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                        int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                        Duration retention, int housekeepingBatch, int dashboardPort,
                        Duration queueLagCheckInterval, Duration queueLagWarnThreshold,
                        String dashboardUser, String dashboardPassword, Tls.Options tls, Memory memory) {
        this(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval, heartbeatInterval,
                missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention, housekeepingBatch, dashboardPort,
                queueLagCheckInterval, queueLagWarnThreshold, dashboardUser, dashboardPassword, tls, memory, null);
    }

    public ServerConfig {
        if (tls == null) tls = Tls.Options.DISABLED;
        if (memory == null) memory = Memory.DISABLED;
    }

    /** A copy of this config with the given placement namespace (used to mint epoch-aware ids). */
    public ServerConfig withNamespace(String namespace) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, namespace);
    }

    /** A copy of this config on the given storage (null/blank url ⇒ in-memory). */
    public ServerConfig withStorage(String jdbcUrl, String jdbcUser, String jdbcPassword, int jdbcPoolSize) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, namespace);
    }

    /** A copy of this config bound to a different gRPC port. */
    public ServerConfig withPort(int port) {
        return new ServerConfig(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval,
                heartbeatInterval, missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention,
                housekeepingBatch, dashboardPort, queueLagCheckInterval, queueLagWarnThreshold,
                dashboardUser, dashboardPassword, tls, memory, namespace);
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
                // placement namespace (WIGGLE_NAMESPACE); when set, the cell mints epoch-aware ids.
                strProp("wiggle.namespace", "WIGGLE_NAMESPACE", null));
    }

    public boolean isInMemory() {
        return jdbcUrl == null || jdbcUrl.isBlank();
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
