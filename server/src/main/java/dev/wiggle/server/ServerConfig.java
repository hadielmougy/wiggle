package dev.wiggle.server;

import dev.wiggle.core.Tls;

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
                           String dashboardUser, String dashboardPassword, Tls.Options tls, Stability stability) {

    /**
     * Load-shedding backpressure driven by the gRPC handler <em>thread-pool queue</em> -- a purely
     * server-local signal, so it can never depend on (or be defeated by) storage state. When enabled
     * the RPC handlers run on a bounded pool of {@code threads}; requests waiting for a thread sit in
     * its queue. When that queue is high <em>and growing</em> the server is saturated, so it stops
     * long-polling worker requests: it returns immediately and hands each worker a jittered
     * {@code holdOff} to wait before retrying, and already-parked long-polls exit early -- draining
     * the queue instead of letting parked requests pile up and block clients / exhaust memory.
     * Shedding clears once the queue stops growing or falls back below {@code lowWatermark}. Disabled
     * by default (then the handler pool is the unbounded virtual-thread executor, as before).
     */
    public record Stability(boolean enabled, int threads, int highWatermark, int lowWatermark,
                            Duration checkInterval, Duration holdOff, Duration holdOffJitter) {

        public static final Stability DISABLED =
                new Stability(false, 0, 0, 0, Duration.ofMillis(500), Duration.ofSeconds(2), Duration.ZERO);

        public Stability {
            if (checkInterval == null) checkInterval = Duration.ofMillis(500);
            if (holdOff == null) holdOff = Duration.ofSeconds(2);
            if (holdOffJitter == null) holdOffJitter = Duration.ZERO;
        }

        public static Stability fromEnvironment() {
            return new Stability(
                    boolProp("wiggle.stability.enabled", "WIGGLE_STABILITY_ENABLED", false),
                    intProp("wiggle.stability.threads", "WIGGLE_STABILITY_THREADS", 200),
                    intProp("wiggle.stability.highWatermark", "WIGGLE_STABILITY_HIGH_WATERMARK", 100),
                    intProp("wiggle.stability.lowWatermark", "WIGGLE_STABILITY_LOW_WATERMARK", 20),
                    Duration.ofMillis(intProp("wiggle.stability.checkIntervalMillis",
                            "WIGGLE_STABILITY_CHECK_INTERVAL_MILLIS", 500)),
                    Duration.ofMillis(intProp("wiggle.stability.holdOffMillis",
                            "WIGGLE_STABILITY_HOLD_OFF_MILLIS", 2_000)),
                    Duration.ofMillis(intProp("wiggle.stability.holdOffJitterMillis",
                            "WIGGLE_STABILITY_HOLD_OFF_JITTER_MILLIS", 1_000)));
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

    /** Back-compat constructor: TLS but default (disabled) stability. */
    public ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                        int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                        int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                        Duration retention, int housekeepingBatch, int dashboardPort,
                        Duration queueLagCheckInterval, Duration queueLagWarnThreshold,
                        String dashboardUser, String dashboardPassword, Tls.Options tls) {
        this(port, nodeName, jdbcUrl, jdbcUser, jdbcPassword, jdbcPoolSize, pollInterval, heartbeatInterval,
                missedHeartbeatsBeforeDead, defaultLease, maxLongPoll, retention, housekeepingBatch, dashboardPort,
                queueLagCheckInterval, queueLagWarnThreshold, dashboardUser, dashboardPassword, tls, Stability.DISABLED);
    }

    public ServerConfig {
        if (tls == null) tls = Tls.Options.DISABLED;
        if (stability == null) stability = Stability.DISABLED;
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
                // Load-shedding backpressure for worker polls; disabled unless WIGGLE_STABILITY_ENABLED=true.
                Stability.fromEnvironment());
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
}
