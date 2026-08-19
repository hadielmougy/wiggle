package dev.wiggle.server;

import java.time.Duration;

/** Configuration with sane defaults; every field is overridable from env or system properties. */
public record ServerConfig(int port, String nodeName, String jdbcUrl, String jdbcUser, String jdbcPassword,
                           int jdbcPoolSize, Duration pollInterval, Duration heartbeatInterval,
                           int missedHeartbeatsBeforeDead, Duration defaultLease, Duration maxLongPoll,
                           Duration retention, int housekeepingBatch) {

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
                intProp("wiggle.housekeeping.batch", "WIGGLE_HOUSEKEEPING_BATCH", 100));
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
}
