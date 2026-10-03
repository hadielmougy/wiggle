package com.wiggle.server.topology;

import com.wiggle.core.Json;
import com.wiggle.server.store.ReplicatedStorage;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.topology.Topology.Connection;
import com.wiggle.server.topology.Topology.Generation;
import com.wiggle.server.topology.Topology.Role;
import com.wiggle.server.topology.Topology.Shard;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads and validates the {@code WIGGLE_STORAGE_TOPOLOGY} document. Anything it cannot make sense of
 * fails with an {@link IllegalArgumentException} naming what is wrong, so a node refuses to start
 * rather than run on a topology it misread.
 */
public final class TopologyParser {

    /** Names the document: a file path, or the document itself when it starts with {@code '{'}. */
    public static final String ENV = "WIGGLE_STORAGE_TOPOLOGY";

    /** Read replicas of a deployment on one database, named by {@code WIGGLE_JDBC_REPLICA_URLS}. */
    private static final List<String> REPLICA_ENV = List.of("WIGGLE_JDBC_REPLICA_URLS",
            "WIGGLE_JDBC_REPLICA_POOL_SIZE", "WIGGLE_JDBC_MAX_REPLICA_LAG_MILLIS", "WIGGLE_JDBC_REPLICA_FALLBACK");

    private static final Pattern VAR = Pattern.compile("\\$\\{([A-Za-z_][A-Za-z0-9_]*)}");
    private static final int DEFAULT_POOL = 10;
    private static final int DEFAULT_REPLICA_POOL = 16;
    private static final long DEFAULT_MAX_REPLICA_LAG_MILLIS = 5_000;

    private TopologyParser() {}

    /**
     * The topology {@code env} names, or empty when it names none -- a deployment on one database
     * configured with {@code WIGGLE_JDBC_URL}, as before sharding.
     */
    public static Optional<Topology> fromEnvironment(Map<String, String> env) {
        String value = env.get(ENV);
        boolean replicaEnv = REPLICA_ENV.stream().anyMatch(n -> set(env.get(n)));
        if (!set(value)) return replicaEnv ? Optional.of(oneDatabase(env)) : Optional.empty();
        if (replicaEnv) {
            throw new IllegalArgumentException("WIGGLE_JDBC_REPLICA_* is set alongside " + ENV
                    + "; a sharded deployment lists each shard's replicas in the topology");
        }
        if (set(env.get("WIGGLE_JDBC_URL"))) {
            throw new IllegalArgumentException("both " + ENV + " and WIGGLE_JDBC_URL are set; a sharded "
                    + "deployment names every database in the topology, so unset WIGGLE_JDBC_URL");
        }
        String document = value.stripLeading().startsWith("{") ? value : read(Path.of(value.trim()));
        return Optional.of(parse(document, env));
    }

    /**
     * The one-shard topology of a deployment on one database with read replicas: {@code WIGGLE_JDBC_*}
     * for the primary, {@code WIGGLE_JDBC_REPLICA_URLS} (comma-separated) for the replicas.
     */
    private static Topology oneDatabase(Map<String, String> env) {
        String url = env.get("WIGGLE_JDBC_URL");
        if (!set(url)) throw bad("WIGGLE_JDBC_REPLICA_* is set without WIGGLE_JDBC_URL, the primary they replicate");
        String urls = env.get("WIGGLE_JDBC_REPLICA_URLS");
        if (!set(urls)) throw bad("a WIGGLE_JDBC_REPLICA_* setting is set without WIGGLE_JDBC_REPLICA_URLS");
        String user = env.get("WIGGLE_JDBC_USER"), password = env.get("WIGGLE_JDBC_PASSWORD");
        int replicaPool = poolOf(number(env, "WIGGLE_JDBC_REPLICA_POOL_SIZE"), "a replica",
                DEFAULT_REPLICA_POOL);
        List<Connection> replicas = new ArrayList<>();
        for (String r : urls.split(",")) {
            if (!r.isBlank()) replicas.add(new Connection(r.trim(), user, password, replicaPool));
        }
        Shard shard = new Shard(0, ShardState.ACTIVE, EnumSet.of(Role.INSTANCES, Role.HOME, Role.AUTH),
                new Connection(url, user, password, poolOf(number(env, "WIGGLE_JDBC_POOL_SIZE"), "the primary", 10)),
                replicas, lagOf(number(env, "WIGGLE_JDBC_MAX_REPLICA_LAG_MILLIS"), "the replicas"),
                fallbackOf(env.get("WIGGLE_JDBC_REPLICA_FALLBACK"), "WIGGLE_JDBC_REPLICA_FALLBACK"));
        Topology t = new Topology(List.of(shard), List.of(new Generation(1, 0, Map.of(0, 1))));
        validate(t);
        return t;
    }

    private static Object number(Map<String, String> env, String name) {
        String v = env.get(name);
        if (!set(v)) return null;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw bad(name + " is not a whole number: '" + v + "'");
        }
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException e) {
            throw new UncheckedIOException(ENV + " names " + path + ", which could not be read", e);
        }
    }

    /** Parses and validates a topology document, resolving {@code ${NAME}} from {@code env}. */
    public static Topology parse(String document, Map<String, String> env) {
        Map<String, Object> root;
        try {
            root = Json.parseObject(document);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("the storage topology is not valid JSON: " + e.getMessage(), e);
        }
        Map<String, Object> defaults = object(root, "defaults");
        List<Shard> shards = new ArrayList<>();
        for (Object o : array(root, "shards")) shards.add(shard(Json.asObject(o), defaults, env));
        List<Generation> generations = new ArrayList<>();
        for (Object o : array(root, "generations")) generations.add(generation(Json.asObject(o)));
        Topology topology = new Topology(withAuth(shards), generations);
        validate(topology);
        return topology;
    }

    private static Shard shard(Map<String, Object> s, Map<String, Object> defaults, Map<String, String> env) {
        int id = intOf(s, "id", "a shard");
        String what = "shard " + id;
        ShardState state = enumOf(ShardState.class, string(s, "state", what), what + " state");
        Set<Role> roles = EnumSet.noneOf(Role.class);
        for (Object r : array(s, "roles")) roles.add(enumOf(Role.class, String.valueOf(r), what + " role"));
        if (roles.isEmpty()) throw bad(what + " has no roles");
        Map<String, Object> primary = object(s, "primary");
        Connection conn = null;
        if (!primary.isEmpty() || roles.contains(Role.INSTANCES) || roles.contains(Role.HOME)) {
            conn = connection(primary, s, defaults, env, what + " primary", "pool", DEFAULT_POOL);
        }
        List<Connection> replicas = new ArrayList<>();
        List<Object> listed = array(s, "replicas");
        for (int i = 0; i < listed.size(); i++) {
            replicas.add(connection(Json.asObject(listed.get(i)), s, defaults, env, what + " replica " + (i + 1),
                    "replicaPool", DEFAULT_REPLICA_POOL));
        }
        if (!replicas.isEmpty() && conn == null) throw bad(what + " lists replicas but has no primary");
        Object lag = s.containsKey("maxReplicaLagMillis") ? s.get("maxReplicaLagMillis") : defaults.get("maxReplicaLagMillis");
        Object fallback = s.containsKey("replicaFallback") ? s.get("replicaFallback") : defaults.get("replicaFallback");
        return new Shard(id, state, roles, conn, replicas, lagOf(lag, what),
                fallbackOf(fallback == null ? null : String.valueOf(fallback), what + " replicaFallback"));
    }

    /**
     * A connection: its url from {@code own}, and user, password and pool from {@code own}, else the
     * shard, else the defaults. A replica's own {@code pool} overrides the inherited {@code replicaPool}.
     */
    private static Connection connection(Map<String, Object> own, Map<String, Object> shard,
                                         Map<String, Object> defaults, Map<String, String> env, String what,
                                         String poolKey, int defaultPool) {
        String url = interpolate(string(own, "url", what), env, what + " url");
        if (url == null || url.isBlank()) throw bad(what + " needs a url");
        Object pool = own.containsKey("pool") ? own.get("pool") : setting(poolKey, own, shard, defaults);
        return new Connection(url,
                interpolate(setting("user", own, shard, defaults), env, what + " user"),
                interpolate(setting("password", own, shard, defaults), env, what + " password"),
                poolOf(pool, what, defaultPool));
    }

    private static long lagOf(Object v, String what) {
        if (v == null) return DEFAULT_MAX_REPLICA_LAG_MILLIS;
        if (!(v instanceof Number n) || n.longValue() < 0 || n.doubleValue() != n.longValue()) {
            throw bad(what + " maxReplicaLagMillis must be a non-negative integer, got " + v);
        }
        return n.longValue();
    }

    private static ReplicatedStorage.Fallback fallbackOf(String v, String what) {
        if (v == null || v.isBlank()) return ReplicatedStorage.Fallback.PRIMARY;
        return enumOf(ReplicatedStorage.Fallback.class, v, what);
    }

    /** A connection setting: the primary's, else the shard's, else the document default. */
    private static Object setting(String key, Map<String, Object> primary, Map<String, Object> shard,
                                  Map<String, Object> defaults) {
        if (primary.containsKey(key)) return primary.get(key);
        if (shard.containsKey(key)) return shard.get(key);
        return defaults.get(key);
    }

    private static int poolOf(Object v, String what, int defaultPool) {
        if (v == null) return defaultPool;
        if (!(v instanceof Number n) || n.intValue() < 1 || n.doubleValue() != n.intValue()) {
            throw bad(what + " pool must be a positive integer, got " + v);
        }
        return n.intValue();
    }

    private static Generation generation(Map<String, Object> g) {
        long id = intOf(g, "id", "a generation");
        String what = "generation " + id;
        long activeFrom;
        try {
            activeFrom = Instant.parse(string(g, "activeFrom", what)).toEpochMilli();
        } catch (DateTimeParseException | NullPointerException e) {
            throw bad(what + " needs activeFrom as an ISO-8601 instant, e.g. 2026-10-01T00:00:00Z");
        }
        Map<Integer, Integer> weights = new LinkedHashMap<>();
        for (Map.Entry<String, Object> w : object(g, "weights").entrySet()) {
            int shard;
            try {
                shard = Integer.parseInt(w.getKey());
            } catch (NumberFormatException e) {
                throw bad(what + " weighs '" + w.getKey() + "', which is not a shard id");
            }
            if (!(w.getValue() instanceof Number n) || n.intValue() < 0 || n.doubleValue() != n.intValue()) {
                throw bad(what + " gives shard " + shard + " a weight that is not a non-negative integer");
            }
            weights.put(shard, n.intValue());
        }
        return new Generation(id, activeFrom, weights);
    }

    /** The home shard also carries auth when no shard claims it. */
    private static List<Shard> withAuth(List<Shard> shards) {
        if (shards.stream().anyMatch(s -> s.has(Role.AUTH))) return shards;
        List<Shard> out = new ArrayList<>();
        for (Shard s : shards) {
            if (!s.has(Role.HOME)) {
                out.add(s);
                continue;
            }
            Set<Role> roles = EnumSet.copyOf(s.roles());
            roles.add(Role.AUTH);
            out.add(new Shard(s.id(), s.state(), roles, s.primary(), s.replicas(), s.maxReplicaLagMillis(),
                    s.replicaFallback()));
        }
        return out;
    }

    private static void validate(Topology t) {
        if (t.shards().isEmpty()) throw bad("the topology lists no shards");
        Set<Integer> ids = new HashSet<>();
        for (Shard s : t.shards()) {
            if (s.id() < 0) throw bad("shard ids are not negative: " + s.id());
            if (!ids.add(s.id())) throw bad("shard " + s.id() + " is listed twice");
        }
        for (Role role : List.of(Role.HOME, Role.AUTH)) {
            long n = t.shards().stream().filter(s -> s.has(role)).count();
            if (n != 1) {
                throw bad("exactly one shard carries the " + lower(role) + " role; " + n + " do");
            }
        }
        Shard home = t.only(Role.HOME);
        if (home.state() != ShardState.ACTIVE) throw bad("the home shard is " + home.state() + "; it stays ACTIVE");
        if (t.shards().stream().noneMatch(s -> s.has(Role.INSTANCES) && s.state() != ShardState.RETIRED)) {
            throw bad("no shard carries the instances role");
        }
        if (t.generations().isEmpty()) throw bad("the topology has no generations, so nothing says where to mint");
        Generation previous = null;
        for (Generation g : t.generations()) {
            if (previous != null && (g.id() <= previous.id() || g.activeFrom() <= previous.activeFrom())) {
                throw bad("generation " + g.id() + " must come after generation " + previous.id()
                        + " in both id and activeFrom");
            }
            if (g.weights().values().stream().noneMatch(w -> w > 0)) {
                throw bad("generation " + g.id() + " gives no shard a positive weight");
            }
            for (Map.Entry<Integer, Integer> w : g.weights().entrySet()) {
                if (w.getValue() == 0) continue;
                Shard s = t.shard(w.getKey()).orElseThrow(() ->
                        bad("generation " + g.id() + " weighs shard " + w.getKey() + ", which is not listed"));
                if (!s.has(Role.INSTANCES)) {
                    throw bad("generation " + g.id() + " weighs shard " + s.id() + ", which holds no instances");
                }
                if (s.state() != ShardState.ACTIVE) {
                    throw bad("generation " + g.id() + " weighs shard " + s.id() + ", which is " + s.state());
                }
            }
            previous = g;
        }
    }

    private static String interpolate(Object value, Map<String, String> env, String what) {
        if (value == null) return null;
        Matcher m = VAR.matcher(String.valueOf(value));
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String v = env.get(m.group(1));
            if (v == null) throw bad(what + " refers to ${" + m.group(1) + "}, which is not set");
            m.appendReplacement(out, Matcher.quoteReplacement(v));
        }
        m.appendTail(out);
        return out.toString();
    }

    private static <E extends Enum<E>> E enumOf(Class<E> type, String value, String what) {
        if (value == null) throw bad(what + " is missing");
        try {
            return Enum.valueOf(type, value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw bad(what + " '" + value + "' is not one of " + List.of(type.getEnumConstants()));
        }
    }

    private static int intOf(Map<String, Object> m, String key, String what) {
        Object v = m.get(key);
        if (!(v instanceof Number n) || n.doubleValue() != n.longValue()) throw bad(what + " needs an integer " + key);
        return Math.toIntExact(n.longValue());
    }

    private static String string(Map<String, Object> m, String key, String what) {
        Object v = m.get(key);
        if (v == null) return null;
        if (!(v instanceof String s)) throw bad(what + " " + key + " must be a string");
        return s;
    }

    private static Map<String, Object> object(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v != null && !(v instanceof Map)) throw bad("'" + key + "' must be a JSON object");
        return Json.asObject(v);
    }

    private static List<Object> array(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v != null && !(v instanceof List)) throw bad("'" + key + "' must be a JSON array");
        return Json.asArray(v);
    }

    private static String lower(Role r) {
        return r.name().toLowerCase(Locale.ROOT);
    }

    private static boolean set(String v) {
        return v != null && !v.isBlank();
    }

    private static IllegalArgumentException bad(String message) {
        return new IllegalArgumentException("storage topology: " + message);
    }
}
