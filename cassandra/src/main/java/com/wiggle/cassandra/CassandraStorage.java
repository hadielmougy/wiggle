package com.wiggle.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.Json;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.*;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Cassandra-backed store. Cassandra has no cross-partition transactions and no
 * {@code SELECT ... FOR UPDATE}, so the engine's per-instance serialisation is provided
 * <em>optimistically</em>: all of an instance's rows (the instance itself and its tokens) live in
 * one partition ({@code instance_state}, partition key {@code instance_id}), and a transaction's
 * writes are buffered and flushed as a single-partition {@code LOGGED} batch whose instance-row
 * update is a lightweight transaction (LWT) conditioned on the instance {@code revision}. A losing
 * writer's batch does not apply and {@link #inTx} retries the whole unit -- the same guarantee the
 * SQL backends get from a row lock, minus the blocking.
 *
 * <p>Dispatch, timers, leases and signal waits are discovered through denormalised index tables
 * partitioned so the hot paths never scan: task dispatch is partitioned by {@code queue}; timer /
 * lease / signal sweeps by a small fixed shard. The authoritative claim is always a per-token LWT
 * ({@code IF status='READY'}), so a stale index hint can never cause a token to run twice.
 *
 * <p><b>Known limits (documented, not silent):</b> sub-workflows advance a parent instance inside
 * the child's completing transaction -- two partitions -- so that step is flushed as two batches
 * (conditional ones first); it is atomic only when uncontended. Very wide fan-outs concentrate many
 * token rows in one instance partition. {@code listInstances}/{@code countInstances} read a single
 * bounded index partition. See the module README.
 */
public final class CassandraStorage implements Storage, com.wiggle.server.coord.CoordinatorStoreProvider {

    private static final int SHARDS = 8;

    private final CqlSession session;
    private final Map<String, PreparedStatement> prepared = new ConcurrentHashMap<>();

    CassandraStorage(CqlSession session) { this.session = session; }

    /** Stable per-cell identity: every node bound to this keyspace shares it, distinct keyspaces differ. */
    @Override public String fingerprint() {
        return session.getKeyspace().map(k -> "cql-" + k.asInternal()).orElse(null);
    }

    /** Parses {@code cassandra://host[:port][,host...]/keyspace?dc=<dc>&rf=<n>} and connects. */
    public static CassandraStorage fromUrl(String url, String user, String password) {
        if (url == null || !url.startsWith("cassandra://")) {
            throw new IllegalArgumentException("not a cassandra URL: " + url);
        }
        String rest = url.substring("cassandra://".length());
        String query = "";
        int q = rest.indexOf('?');
        if (q >= 0) { query = rest.substring(q + 1); rest = rest.substring(0, q); }
        int slash = rest.indexOf('/');
        String authority = slash >= 0 ? rest.substring(0, slash) : rest;
        String keyspace = slash >= 0 ? rest.substring(slash + 1) : "wiggle";
        if (keyspace.isBlank()) keyspace = "wiggle";
        Map<String, String> params = new HashMap<>();
        for (String kv : query.split("&")) {
            if (kv.isBlank()) continue;
            int eq = kv.indexOf('=');
            if (eq > 0) params.put(kv.substring(0, eq), kv.substring(eq + 1));
        }
        String dc = params.getOrDefault("dc", "datacenter1");
        int rf = Integer.parseInt(params.getOrDefault("rf", "1"));

        List<InetSocketAddress> contacts = new ArrayList<>();
        for (String hp : authority.split(",")) {
            if (hp.isBlank()) continue;
            int c = hp.lastIndexOf(':');
            String host = c >= 0 ? hp.substring(0, c) : hp;
            int port = c >= 0 ? Integer.parseInt(hp.substring(c + 1)) : 9042;
            contacts.add(new InetSocketAddress(host, port));
        }
        if (contacts.isEmpty()) contacts.add(new InetSocketAddress("127.0.0.1", 9042));

        // Connect without a keyspace first so we can create it, then re-open bound to it.
        var adminBuilder = CqlSession.builder().addContactPoints(contacts).withLocalDatacenter(dc);
        if (user != null) adminBuilder.withAuthCredentials(user, password == null ? "" : password);
        try (CqlSession admin = adminBuilder.build()) {
            admin.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':" + rf + "}");
        }
        var builder = CqlSession.builder().addContactPoints(contacts).withLocalDatacenter(dc).withKeyspace(keyspace);
        if (user != null) builder.withAuthCredentials(user, password == null ? "" : password);
        return new CassandraStorage(builder.build());
    }

    private PreparedStatement ps(String cql) {
        return prepared.computeIfAbsent(cql, session::prepare);
    }

    private static int shard(String id) { return Math.floorMod(id.hashCode(), SHARDS); }

    @Override public void migrate() {
        for (String ddl : SCHEMA) session.execute(ddl);
    }

    /** The coordinator store over this keyspace: create the {@code coord_*} tables (idempotent), then a
     *  CQL store (policy CAS via LWT) sharing this session. Reached via {@link CoordinatorStoreProvider},
     *  never via {@code Storage} -- the engine and the coordinator are decoupled. */
    @Override public com.wiggle.server.coord.CoordinatorStore coordinatorStore() {
        for (String ddl : COORD_SCHEMA) session.execute(ddl);
        return new CassandraCoordinatorStore(session);
    }

    /** Coordinator control-plane tables (bounded state): policy, node roster, definition + namespace registries. */
    private static final List<String> COORD_SCHEMA = List.of(
            "CREATE TABLE IF NOT EXISTS coord_leader (id text PRIMARY KEY, holder text, expires_at bigint)",
            """
            CREATE TABLE IF NOT EXISTS coord_policy (
              namespace text PRIMARY KEY, current_epoch bigint, epochs text, revision bigint)""",
            """
            CREATE TABLE IF NOT EXISTS coord_node (
              id text PRIMARY KEY, namespace text, cell_id text, endpoint text, region text,
              engine_version text, cell_fingerprint text, config_generation bigint, last_heartbeat bigint)""",
            // Cell-identity binding: one partition per (namespace, cell_id), so the duplicate-cell-id guard
            // is a single-partition LWT (INSERT ... IF NOT EXISTS), atomic without a roster scan.
            """
            CREATE TABLE IF NOT EXISTS coord_cell (
              namespace text, cell_id text, fingerprint text, PRIMARY KEY ((namespace, cell_id)))""",
            """
            CREATE TABLE IF NOT EXISTS coord_definition (
              namespace text, name text, version int, hash text, registered_at bigint,
              PRIMARY KEY ((namespace), name))""",
            """
            CREATE TABLE IF NOT EXISTS coord_namespace (
              namespace text PRIMARY KEY, state text, scheme text, jdbc_url text, db_user text,
              secret_ref text, pool_size int, replicas int, region text, endpoint text, error text,
              updated_at bigint)""");

    private static final List<String> SCHEMA = List.of(
            """
            CREATE TABLE IF NOT EXISTS instance_state (
              instance_id text, row_kind text, row_id text,
              revision bigint, workflow text, version int, status text, updated_at bigint, created_at bigint,
              correlation_id text, term_reason text, error text, context text, parent_token_id text,
              node_id text, kind text, activity text, queue text, attempt int, available_at bigint,
              lease_owner text, lease_expires bigint, join_stack text, last_error text, payload text,
              PRIMARY KEY ((instance_id), row_kind, row_id))""",
            "CREATE TABLE IF NOT EXISTS token_index (token_id text PRIMARY KEY, instance_id text)",
            """
            CREATE TABLE IF NOT EXISTS dispatch (queue text, available_at bigint, token_id text, instance_id text,
              PRIMARY KEY ((queue), available_at, token_id))""",
            "CREATE TABLE IF NOT EXISTS queues (queue text PRIMARY KEY)",
            """
            CREATE TABLE IF NOT EXISTS timer (shard int, available_at bigint, token_id text, instance_id text,
              PRIMARY KEY ((shard), available_at, token_id))""",
            """
            CREATE TABLE IF NOT EXISTS lease (shard int, lease_expires bigint, token_id text, instance_id text,
              PRIMARY KEY ((shard), lease_expires, token_id))""",
            """
            CREATE TABLE IF NOT EXISTS signal_wait (shard int, created_at bigint, token_id text, instance_id text,
              deadline bigint, PRIMARY KEY ((shard), created_at, token_id))""",
            """
            CREATE TABLE IF NOT EXISTS instance_index (bucket int, created_at bigint, id text, workflow text,
              status text, PRIMARY KEY ((bucket), created_at, id)) WITH CLUSTERING ORDER BY (created_at DESC)""",
            "CREATE TABLE IF NOT EXISTS child_by_parent_token (parent_token_id text PRIMARY KEY, child_id text)",
            """
            CREATE TABLE IF NOT EXISTS definition (name text, version int, body text, registered_at bigint,
              PRIMARY KEY ((name), version)) WITH CLUSTERING ORDER BY (version DESC)""",
            "CREATE TABLE IF NOT EXISTS definition_names (bucket int, name text, PRIMARY KEY ((bucket), name))",
            """
            CREATE TABLE IF NOT EXISTS graph_node (workflow text, version int, node_id text, kind text, name text,
              activity text, queue text, retry_json text, sleep_millis bigint, expected int, success int,
              reason text, is_start int, items_key text, item_key text,
              PRIMARY KEY ((workflow, version), node_id))""",
            """
            CREATE TABLE IF NOT EXISTS graph_edge (workflow text, version int, from_node text, ordinal int,
              to_node text, cond text, PRIMARY KEY ((workflow, version, from_node), ordinal))""",
            """
            CREATE TABLE IF NOT EXISTS graph_start (workflow text, version int, node_id text,
              PRIMARY KEY ((workflow, version)))""",
            """
            CREATE TABLE IF NOT EXISTS schedule (id text PRIMARY KEY, workflow text, interval_millis bigint,
              cron text, context text, next_fire_at bigint, created_at bigint)""",
            "CREATE TABLE IF NOT EXISTS schedule_by_workflow (workflow text PRIMARY KEY, id text)",
            "CREATE TABLE IF NOT EXISTS schedule_index (bucket int, id text, next_fire_at bigint, PRIMARY KEY ((bucket), id))",
            """
            CREATE TABLE IF NOT EXISTS node (id text PRIMARY KEY, name text, first_heartbeat bigint,
              last_heartbeat bigint, workers int, leader int)""",
            "CREATE TABLE IF NOT EXISTS node_index (bucket int, id text, PRIMARY KEY ((bucket), id))",
            "CREATE TABLE IF NOT EXISTS processed (bucket bigint PRIMARY KEY, cnt counter)");

    @Override public <R> R inTx(Function<Tx, R> work) {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 50; attempt++) {
            CassandraTx tx = new CassandraTx();
            try {
                R r = work.apply(tx);
                tx.commit();
                return r;
            } catch (RetryConflict e) {
                last = e;
                // brief backoff to break livelock between contending writers
                try { Thread.sleep(Math.min(50, 1L + attempt)); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new StorageException("interrupted during retry", ie);
                }
            }
        }
        throw new StorageException("optimistic retry exhausted", last);
    }

    @Override public void close() { session.close(); }

    /** Thrown when an instance's revision CAS fails; {@link #inTx} retries the unit. */
    private static final class RetryConflict extends RuntimeException {
        private static final long serialVersionUID = 1L;

        RetryConflict() { super(null, null, false, false); }
    }

    public static final class StorageException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public StorageException(String m, Throwable c) { super(m, c); }
    }

    // ------------------------------------------------------------------------------------------
    // Transaction
    // ------------------------------------------------------------------------------------------

    private final class CassandraTx implements Tx {

        /** Buffered instance writes (id -> instance), flushed at commit. */
        private final Map<String, Instance> instBuf = new LinkedHashMap<>();
        /** Instances created this tx (INSERT ... IF NOT EXISTS rather than a revision CAS). */
        private final Set<String> inserted = new HashSet<>();
        /** Revision read at lockInstance, the CAS guard for an existing instance's flush. */
        private final Map<String, Long> lockedRev = new HashMap<>();
        /** Buffered token writes (token id -> token). */
        private final Map<String, Token> tokBuf = new LinkedHashMap<>();

        // ---- graph / definitions (immutable reference data; read/write straight through) ----

        @Override public void putDefinition(String name, int version, String json) {
            session.execute(ps("INSERT INTO definition (name,version,body,registered_at) VALUES (?,?,?,?) IF NOT EXISTS")
                    .bind(name, version, json, System.currentTimeMillis()));
            session.execute(ps("INSERT INTO definition_names (bucket,name) VALUES (0,?)").bind(name));
        }

        @Override public Optional<String> definition(String name, int version) {
            Row r = session.execute(ps("SELECT body FROM definition WHERE name=? AND version=?").bind(name, version)).one();
            return r == null ? Optional.empty() : Optional.of(r.getString("body"));
        }

        @Override public Optional<Integer> latestVersion(String name) {
            // Clustering order is version DESC, so the first row is the highest version.
            Row r = session.execute(ps("SELECT version FROM definition WHERE name=? LIMIT 1").bind(name)).one();
            return r == null ? Optional.empty() : Optional.of(r.getInt("version"));
        }

        @Override public List<String> definitionNames() {
            List<String> out = new ArrayList<>();
            for (Row r : session.execute(ps("SELECT name FROM definition_names WHERE bucket=0").bind())) {
                out.add(r.getString("name"));
            }
            Collections.sort(out);
            return out;
        }

        @Override public void putGraph(WorkflowDefinition def) {
            Row probe = session.execute(ps("SELECT node_id FROM graph_node WHERE workflow=? AND version=? LIMIT 1")
                    .bind(def.name(), def.version())).one();
            if (probe != null) return;   // already stored (content-hash version => identical)
            for (Node n : def.nodes().values()) {
                session.execute(ps("INSERT INTO graph_node (workflow,version,node_id,kind,name,activity,queue," +
                        "retry_json,sleep_millis,expected,success,reason,is_start,items_key,item_key) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").bind(
                        def.name(), def.version(), n.id(), n.kind().name(), n.name(), n.activity(), n.queue(),
                        n.retry() == null ? null : Json.write(n.retry().toJson()), n.sleepMillis(), n.expected(),
                        n.success() ? 1 : 0, n.reason(), n.id().equals(def.startNode()) ? 1 : 0,
                        n.itemsKey(), n.itemKey()));
                int ordinal = 0;
                for (Edge e : edgesOf(n)) {
                    session.execute(ps("INSERT INTO graph_edge (workflow,version,from_node,ordinal,to_node,cond) " +
                            "VALUES (?,?,?,?,?,?)").bind(def.name(), def.version(), n.id(), ordinal++, e.to, e.cond));
                }
            }
            session.execute(ps("INSERT INTO graph_start (workflow,version,node_id) VALUES (?,?,?)")
                    .bind(def.name(), def.version(), def.startNode()));
        }

        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            Row r = session.execute(ps("SELECT * FROM graph_node WHERE workflow=? AND version=? AND node_id=?")
                    .bind(workflow, version, nodeId)).one();
            if (r == null) return Optional.empty();
            NodeKind kind = NodeKind.valueOf(r.getString("kind"));
            String retryJson = r.getString("retry_json");
            RetryPolicy retry = retryJson == null ? null : RetryPolicy.fromJson(Json.parse(retryJson));
            EdgeTargets targets = new EdgeTargets(kind);
            for (Row er : session.execute(ps("SELECT to_node,cond FROM graph_edge " +
                    "WHERE workflow=? AND version=? AND from_node=?").bind(workflow, version, nodeId))) {
                targets.absorb(er.getString("to_node"), er.getString("cond"));
            }
            return Optional.of(new Node(nodeId, kind, r.getString("name"), r.getString("activity"),
                    r.getString("queue"), retry, r.getLong("sleep_millis"), targets.next, targets.altNext,
                    List.copyOf(targets.branches), r.getInt("expected"), r.getInt("success") != 0,
                    r.getString("reason"), r.getString("items_key"), r.getString("item_key")));
        }

        @Override public Optional<String> graphStartNode(String workflow, int version) {
            Row r = session.execute(ps("SELECT node_id FROM graph_start WHERE workflow=? AND version=?")
                    .bind(workflow, version)).one();
            return r == null ? Optional.empty() : Optional.of(r.getString("node_id"));
        }

        // ---- instances (buffered) ----

        @Override public void insertInstance(Instance i) {
            instBuf.put(i.id, i.clone());
            inserted.add(i.id);
            if (i.parentTokenId != null) {
                session.execute(ps("INSERT INTO child_by_parent_token (parent_token_id,child_id) VALUES (?,?)")
                        .bind(i.parentTokenId, i.id));
            }
        }

        @Override public Optional<Instance> lockInstance(String id) {
            Optional<Instance> found = findInstance(id);
            found.ifPresent(i -> lockedRev.putIfAbsent(id, i.revision));
            return found;
        }

        @Override public Optional<Instance> findInstance(String id) {
            if (instBuf.containsKey(id)) return Optional.of(instBuf.get(id).clone());
            Row r = session.execute(ps("SELECT * FROM instance_state WHERE instance_id=? AND row_kind='INST' AND row_id=''")
                    .bind(id)).one();
            return r == null ? Optional.empty() : Optional.of(readInstance(r));
        }

        @Override public void updateInstance(Instance i) { instBuf.put(i.id, i.clone()); }

        @Override public List<Instance> listInstances(String workflow, InstanceStatus status, int limit) {
            List<Instance> out = new ArrayList<>();
            for (Row r : session.execute(ps("SELECT id FROM instance_index WHERE bucket=0").bind())) {
                if (out.size() >= limit) break;
                findInstance(r.getString("id")).ifPresent(inst -> {
                    if ((workflow == null || workflow.equals(inst.workflow))
                            && (status == null || status == inst.status)) out.add(inst);
                });
            }
            return out.stream().sorted(Comparator.comparingLong((Instance i) -> i.createdAt).reversed())
                    .limit(limit).toList();
        }

        @Override public int countInstances(InstanceStatus status) {
            int n = 0;
            for (Row r : session.execute(ps("SELECT id,status FROM instance_index WHERE bucket=0").bind())) {
                if (status.name().equals(r.getString("status"))) n++;
            }
            return n;
        }

        // ---- tokens (buffered) ----

        @Override public void insertToken(Token t) { tokBuf.put(t.id, t.clone()); }

        @Override public void updateToken(Token t) { tokBuf.put(t.id, t.clone()); }

        @Override public Optional<Token> findToken(String id) {
            if (tokBuf.containsKey(id)) return Optional.of(tokBuf.get(id).clone());
            Row idx = session.execute(ps("SELECT instance_id FROM token_index WHERE token_id=?").bind(id)).one();
            if (idx == null) return Optional.empty();
            Row r = session.execute(ps("SELECT * FROM instance_state WHERE instance_id=? AND row_kind='TOK' AND row_id=?")
                    .bind(idx.getString("instance_id"), id)).one();
            return r == null ? Optional.empty() : Optional.of(readToken(r));
        }

        @Override public List<Token> tokensOf(String instanceId) {
            Map<String, Token> byId = new LinkedHashMap<>();
            for (Row r : session.execute(ps("SELECT * FROM instance_state WHERE instance_id=? AND row_kind='TOK'")
                    .bind(instanceId))) {
                Token t = readToken(r);
                byId.put(t.id, t);
            }
            for (Token t : tokBuf.values()) {          // overlay this tx's own writes
                if (t.instanceId.equals(instanceId)) byId.put(t.id, t.clone());
            }
            return byId.values().stream().sorted(Comparator.comparing(t -> t.id)).toList();
        }

        // ---- claim: dispatch index + per-token LWT (exactly once) ----

        @Override public List<Token> claimTasks(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            Collection<String> qs = (queues == null || queues.isEmpty()) ? allQueues() : queues;
            List<Token> out = new ArrayList<>();
            for (String queue : qs) {
                if (out.size() >= max) break;
                ResultSet candidates = session.execute(ps("SELECT available_at,token_id,instance_id FROM dispatch " +
                        "WHERE queue=? AND available_at<=? ORDER BY available_at LIMIT ?")
                        .bind(queue, now, (max - out.size()) * 4));
                for (Row c : candidates) {
                    if (out.size() >= max) break;
                    long availableAt = c.getLong("available_at");
                    String tokenId = c.getString("token_id");
                    String instanceId = c.getString("instance_id");
                    boolean won = session.execute(ps("UPDATE instance_state SET status='RUNNING', lease_owner=?, " +
                            "lease_expires=?, updated_at=? WHERE instance_id=? AND row_kind='TOK' AND row_id=? " +
                            "IF status='READY'").bind(workerId, leaseUntil, now, instanceId, tokenId)).wasApplied();
                    session.execute(ps("DELETE FROM dispatch WHERE queue=? AND available_at=? AND token_id=?")
                            .bind(queue, availableAt, tokenId));
                    if (won) {
                        Row r = session.execute(ps("SELECT * FROM instance_state WHERE instance_id=? AND " +
                                "row_kind='TOK' AND row_id=?").bind(instanceId, tokenId)).one();
                        if (r != null) {
                            Token t = readToken(r);
                            session.execute(ps("INSERT INTO lease (shard,lease_expires,token_id,instance_id) " +
                                    "VALUES (?,?,?,?)").bind(shard(tokenId), leaseUntil, tokenId, instanceId));
                            out.add(t);
                        }
                    }
                }
            }
            return out;
        }

        private Collection<String> allQueues() {
            List<String> out = new ArrayList<>();
            for (Row r : session.execute(ps("SELECT queue FROM queues").bind())) out.add(r.getString("queue"));
            return out;
        }

        // ---- sweeps: index tables, self-cleaning of stale hints, engine re-validates ----

        @Override public List<Token> dueTimers(long now, int max) {
            return sweepIndex("timer", "available_at", now, max,
                    t -> t.status == TokenStatus.WAITING && t.kind == NodeKind.SLEEP);
        }

        @Override public List<Token> expiredLeases(long now, int max) {
            List<Token> out = new ArrayList<>();
            for (int s = 0; s < SHARDS && out.size() < max; s++) {
                for (Row r : session.execute(ps("SELECT lease_expires,token_id,instance_id FROM lease " +
                        "WHERE shard=? AND lease_expires<? LIMIT ?").bind(s, now, max - out.size()))) {
                    String tokenId = r.getString("token_id");
                    Optional<Token> t = findToken(tokenId);
                    if (t.isPresent() && t.get().status == TokenStatus.RUNNING
                            && t.get().leaseExpiresAt > 0 && t.get().leaseExpiresAt < now) {
                        out.add(t.get());
                    } else {
                        session.execute(ps("DELETE FROM lease WHERE shard=? AND lease_expires=? AND token_id=?")
                                .bind(s, r.getLong("lease_expires"), tokenId));
                    }
                    if (out.size() >= max) break;
                }
            }
            return out;
        }

        @Override public List<Token> pendingSignals(int max) {
            List<Token> out = new ArrayList<>();
            for (int s = 0; s < SHARDS && out.size() < max; s++) {
                for (Row r : session.execute(ps("SELECT created_at,token_id FROM signal_wait WHERE shard=? LIMIT ?")
                        .bind(s, max - out.size()))) {
                    String tokenId = r.getString("token_id");
                    Optional<Token> t = findToken(tokenId);
                    if (t.isPresent() && t.get().status == TokenStatus.AWAITING && t.get().kind == NodeKind.SIGNAL) {
                        out.add(t.get());
                    } else {
                        session.execute(ps("DELETE FROM signal_wait WHERE shard=? AND created_at=? AND token_id=?")
                                .bind(s, r.getLong("created_at"), tokenId));
                    }
                    if (out.size() >= max) break;
                }
            }
            out.sort(Comparator.comparingLong((Token t) -> t.createdAt).thenComparing(t -> t.id));
            return out;
        }

        @Override public List<Token> dueSignals(long now, int max) {
            List<Token> out = new ArrayList<>();
            for (int s = 0; s < SHARDS && out.size() < max; s++) {
                for (Row r : session.execute(ps("SELECT created_at,token_id,deadline FROM signal_wait WHERE shard=? LIMIT ?")
                        .bind(s, 500))) {
                    if (out.size() >= max) break;
                    long deadline = r.getLong("deadline");
                    if (deadline <= 0 || deadline > now) continue;
                    Optional<Token> t = findToken(r.getString("token_id"));
                    if (t.isPresent() && t.get().status == TokenStatus.AWAITING && t.get().kind == NodeKind.SIGNAL
                            && t.get().availableAt > 0 && t.get().availableAt <= now) {
                        out.add(t.get());
                    }
                }
            }
            out.sort(Comparator.comparingLong((Token t) -> t.availableAt));
            return out;
        }

        private List<Token> sweepIndex(String table, String timeCol, long now, int max,
                                       java.util.function.Predicate<Token> valid) {
            List<Token> out = new ArrayList<>();
            for (int s = 0; s < SHARDS && out.size() < max; s++) {
                for (Row r : session.execute(ps("SELECT " + timeCol + ",token_id FROM " + table +
                        " WHERE shard=? AND " + timeCol + "<=? LIMIT ?").bind(s, now, max - out.size()))) {
                    String tokenId = r.getString("token_id");
                    Optional<Token> t = findToken(tokenId);
                    if (t.isPresent() && valid.test(t.get())) {
                        out.add(t.get());
                    } else {
                        session.execute(ps("DELETE FROM " + table + " WHERE shard=? AND " + timeCol + "=? AND token_id=?")
                                .bind(s, r.getLong(timeCol), tokenId));
                    }
                    if (out.size() >= max) break;
                }
            }
            return out;
        }

        @Override public List<String> childInstanceIds(String parentInstanceId) {
            List<String> out = new ArrayList<>();
            for (Token t : tokensOf(parentInstanceId)) {
                Row r = session.execute(ps("SELECT child_id FROM child_by_parent_token WHERE parent_token_id=?")
                        .bind(t.id)).one();
                if (r != null) out.add(r.getString("child_id"));
            }
            Collections.sort(out);
            return out;
        }

        // ---- schedules ----

        @Override public void putSchedule(Rows.Schedule s) {
            session.execute(ps("INSERT INTO schedule (id,workflow,interval_millis,cron,context,next_fire_at,created_at) " +
                    "VALUES (?,?,?,?,?,?,?)").bind(s.id, s.workflow, s.intervalMillis, s.cron, s.contextJson,
                    s.nextFireAt, s.createdAt));
            session.execute(ps("INSERT INTO schedule_by_workflow (workflow,id) VALUES (?,?)").bind(s.workflow, s.id));
            session.execute(ps("INSERT INTO schedule_index (bucket,id,next_fire_at) VALUES (0,?,?)")
                    .bind(s.id, s.nextFireAt));
        }

        @Override public Optional<Rows.Schedule> scheduleByWorkflow(String workflow) {
            Row idx = session.execute(ps("SELECT id FROM schedule_by_workflow WHERE workflow=?").bind(workflow)).one();
            return idx == null ? Optional.empty() : scheduleById(idx.getString("id"));
        }

        private Optional<Rows.Schedule> scheduleById(String id) {
            Row r = session.execute(ps("SELECT * FROM schedule WHERE id=?").bind(id)).one();
            return r == null ? Optional.empty() : Optional.of(readSchedule(r));
        }

        @Override public void deleteSchedule(String id) {
            Optional<Rows.Schedule> s = scheduleById(id);
            session.execute(ps("DELETE FROM schedule WHERE id=?").bind(id));
            session.execute(ps("DELETE FROM schedule_index WHERE bucket=0 AND id=?").bind(id));
            s.ifPresent(sch -> session.execute(ps("DELETE FROM schedule_by_workflow WHERE workflow=?").bind(sch.workflow)));
        }

        @Override public List<Rows.Schedule> schedules() {
            List<Rows.Schedule> out = new ArrayList<>();
            for (Row r : session.execute(ps("SELECT id FROM schedule_index WHERE bucket=0").bind())) {
                scheduleById(r.getString("id")).ifPresent(out::add);
            }
            out.sort(Comparator.comparing(s -> s.id));
            return out;
        }

        @Override public List<Rows.Schedule> dueSchedules(long now, int max) {
            List<Rows.Schedule> out = new ArrayList<>();
            for (Row r : session.execute(ps("SELECT id,next_fire_at FROM schedule_index WHERE bucket=0").bind())) {
                if (r.getLong("next_fire_at") <= now) scheduleById(r.getString("id")).ifPresent(out::add);
            }
            out.sort(Comparator.comparingLong(s -> s.nextFireAt));
            return out.size() > max ? out.subList(0, max) : out;
        }

        @Override public boolean claimSchedule(String id, long expectedFireAt, long nextFireAt) {
            boolean won = session.execute(ps("UPDATE schedule SET next_fire_at=? WHERE id=? IF next_fire_at=?")
                    .bind(nextFireAt, id, expectedFireAt)).wasApplied();
            if (won) session.execute(ps("INSERT INTO schedule_index (bucket,id,next_fire_at) VALUES (0,?,?)")
                    .bind(id, nextFireAt));
            return won;
        }

        // ---- cluster nodes ----

        @Override public void upsertNode(ServerNode n) {
            Row existing = session.execute(ps("SELECT first_heartbeat FROM node WHERE id=?").bind(n.id)).one();
            long first = existing == null ? n.firstHeartbeat : existing.getLong("first_heartbeat");
            int leader = existing == null ? 0
                    : session.execute(ps("SELECT leader FROM node WHERE id=?").bind(n.id)).one().getInt("leader");
            session.execute(ps("INSERT INTO node (id,name,first_heartbeat,last_heartbeat,workers,leader) " +
                    "VALUES (?,?,?,?,?,?)").bind(n.id, n.name, first, n.lastHeartbeat, n.workers, leader));
            session.execute(ps("INSERT INTO node_index (bucket,id) VALUES (0,?)").bind(n.id));
        }

        @Override public List<ServerNode> nodes() {
            List<ServerNode> out = new ArrayList<>();
            for (Row idx : session.execute(ps("SELECT id FROM node_index WHERE bucket=0").bind())) {
                Row r = session.execute(ps("SELECT * FROM node WHERE id=?").bind(idx.getString("id"))).one();
                if (r != null) {
                    ServerNode n = new ServerNode();
                    n.id = r.getString("id");
                    n.name = r.getString("name");
                    n.firstHeartbeat = r.getLong("first_heartbeat");
                    n.lastHeartbeat = r.getLong("last_heartbeat");
                    n.workers = r.getInt("workers");
                    n.leader = r.getInt("leader") == 1;
                    out.add(n);
                }
            }
            out.sort(Comparator.comparingLong((ServerNode n) -> n.firstHeartbeat).thenComparing(n -> n.id));
            return out;
        }

        @Override public void deleteNodesOlderThan(long before) {
            for (Row idx : session.execute(ps("SELECT id FROM node_index WHERE bucket=0").bind())) {
                String id = idx.getString("id");
                Row r = session.execute(ps("SELECT last_heartbeat FROM node WHERE id=?").bind(id)).one();
                if (r == null || r.getLong("last_heartbeat") < before) {
                    session.execute(ps("DELETE FROM node WHERE id=?").bind(id));
                    session.execute(ps("DELETE FROM node_index WHERE bucket=0 AND id=?").bind(id));
                }
            }
        }

        @Override public void setLeader(String nodeId, boolean leader) {
            session.execute(ps("UPDATE node SET leader=? WHERE id=?").bind(leader ? 1 : 0, nodeId));
        }

        // ---- monitoring / housekeeping ----

        @Override public Rows.QueueDepth queueDepth(long now) {
            int count = 0;
            long oldest = 0;
            for (String queue : allQueues()) {
                for (Row r : session.execute(ps("SELECT available_at FROM dispatch WHERE queue=? AND available_at<=?")
                        .bind(queue, now))) {
                    long at = r.getLong("available_at");
                    count++;
                    if (oldest == 0 || at < oldest) oldest = at;
                }
            }
            return new Rows.QueueDepth(count, oldest);
        }

        @Override public int countProcessedSince(long since) {
            int total = 0;
            long fromBucket = since / 60_000;
            long toBucket = System.currentTimeMillis() / 60_000;
            for (long b = fromBucket; b <= toBucket; b++) {
                Row r = session.execute(ps("SELECT cnt FROM processed WHERE bucket=?").bind(b)).one();
                if (r != null && !r.isNull("cnt")) total += (int) r.getLong("cnt");
            }
            return total;
        }

        @Override public int deleteTerminalInstancesBefore(long updatedBefore, int limit) {
            List<String> victims = new ArrayList<>();
            for (Row r : session.execute(ps("SELECT created_at,id,status FROM instance_index WHERE bucket=0").bind())) {
                if (victims.size() >= limit) break;
                String status = r.getString("status");
                if (status != null && !status.equals(InstanceStatus.RUNNING.name())) {
                    findInstance(r.getString("id")).ifPresent(inst -> {
                        if (inst.updatedAt < updatedBefore) victims.add(inst.id);
                    });
                }
            }
            for (String id : victims) {
                Instance inst = findInstance(id).orElse(null);
                for (Token t : tokensOf(id)) {
                    session.execute(ps("DELETE FROM token_index WHERE token_id=?").bind(t.id));
                }
                session.execute(ps("DELETE FROM instance_state WHERE instance_id=?").bind(id));
                if (inst != null) {
                    session.execute(ps("DELETE FROM instance_index WHERE bucket=0 AND created_at=? AND id=?")
                            .bind(inst.createdAt, id));
                }
            }
            return victims.size();
        }

        // ---- commit: flush buffered writes ----

        void commit() {
            // Group buffered token writes by owning instance.
            Map<String, List<Token>> tokensByInstance = new LinkedHashMap<>();
            for (Token t : tokBuf.values()) {
                tokensByInstance.computeIfAbsent(t.instanceId, k -> new ArrayList<>()).add(t);
            }
            Set<String> partitions = new LinkedHashSet<>();
            partitions.addAll(instBuf.keySet());
            partitions.addAll(tokensByInstance.keySet());

            // Index side-effects first (idempotent, self-cleaning) so a retried CAS leaves only
            // harmless orphan hints, never a missing one.
            for (Token t : tokBuf.values()) writeTokenIndexes(t);
            for (Instance i : instBuf.values()) writeInstanceIndex(i);

            // Existing (revision-CAS) instances first, so a conflict aborts before any new-instance
            // INSERT is written -- avoids orphaning a freshly-inserted sub-workflow child on retry.
            List<String> ordered = new ArrayList<>();
            for (String id : partitions) if (!inserted.contains(id)) ordered.add(id);
            for (String id : partitions) if (inserted.contains(id)) ordered.add(id);

            for (String id : ordered) {
                flushPartition(id, tokensByInstance.getOrDefault(id, List.of()));
            }
            // Throughput counter only on the winning attempt (after CAS), to avoid double counting.
            for (Token t : tokBuf.values()) {
                if (t.status == TokenStatus.DONE && (t.kind == NodeKind.TASK || t.kind == NodeKind.PREDICATE)) {
                    session.execute(ps("UPDATE processed SET cnt = cnt + 1 WHERE bucket=?")
                            .bind(t.updatedAt / 60_000));
                }
            }
        }

        private void flushPartition(String instanceId, List<Token> tokens) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
            boolean conditional = false;
            Instance inst = instBuf.get(instanceId);
            if (inst != null && inserted.contains(instanceId)) {
                batch.addStatement(insertInstanceRow(inst));
                conditional = true;  // IF NOT EXISTS
            } else if (inst != null) {
                long rev = lockedRev.getOrDefault(instanceId, inst.revision);
                batch.addStatement(updateInstanceRow(inst, rev));
                conditional = true;  // IF revision=rev
            }
            for (Token t : tokens) batch.addStatement(insertTokenRow(t));
            if (inst == null && tokens.isEmpty()) return;
            ResultSet rs = session.execute(batch.build());
            if (conditional && !rs.wasApplied()) throw new RetryConflict();
        }

        private com.datastax.oss.driver.api.core.cql.BoundStatement insertInstanceRow(Instance i) {
            return ps("INSERT INTO instance_state (instance_id,row_kind,row_id,revision,workflow,version," +
                    "correlation_id,status,term_reason,error,context,parent_token_id,created_at,updated_at) " +
                    "VALUES (?,'INST','',?,?,?,?,?,?,?,?,?,?,?) IF NOT EXISTS").bind(
                    i.id, 0L, i.workflow, i.version, i.correlationId, i.status.name(), i.terminationReason,
                    i.error, i.contextJson, i.parentTokenId, i.createdAt, i.updatedAt);
        }

        private com.datastax.oss.driver.api.core.cql.BoundStatement updateInstanceRow(Instance i, long expectedRev) {
            return ps("UPDATE instance_state SET revision=?, workflow=?, version=?, correlation_id=?, status=?, " +
                    "term_reason=?, error=?, context=?, parent_token_id=?, created_at=?, updated_at=? " +
                    "WHERE instance_id=? AND row_kind='INST' AND row_id='' IF revision=?").bind(
                    expectedRev + 1, i.workflow, i.version, i.correlationId, i.status.name(), i.terminationReason,
                    i.error, i.contextJson, i.parentTokenId, i.createdAt, i.updatedAt, i.id, expectedRev);
        }

        private com.datastax.oss.driver.api.core.cql.BoundStatement insertTokenRow(Token t) {
            return ps("INSERT INTO instance_state (instance_id,row_kind,row_id,workflow,version,node_id,kind,status," +
                    "activity,queue,attempt,available_at,lease_owner,lease_expires,join_stack,last_error,payload," +
                    "created_at,updated_at) VALUES (?,'TOK',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").bind(
                    t.instanceId, t.id, t.workflow, t.version, t.nodeId, t.kind.name(), t.status.name(),
                    t.activity, t.queue, t.attempt, t.availableAt, t.leaseOwner, t.leaseExpiresAt,
                    t.joinStack == null ? "" : t.joinStack, t.lastError, t.payloadJson, t.createdAt, t.updatedAt);
        }

        private void writeTokenIndexes(Token t) {
            session.execute(ps("INSERT INTO token_index (token_id,instance_id) VALUES (?,?)").bind(t.id, t.instanceId));
            if (t.status == TokenStatus.READY && (t.kind == NodeKind.TASK || t.kind == NodeKind.PREDICATE)) {
                session.execute(ps("INSERT INTO dispatch (queue,available_at,token_id,instance_id) VALUES (?,?,?,?)")
                        .bind(t.queue, t.availableAt, t.id, t.instanceId));
                session.execute(ps("INSERT INTO queues (queue) VALUES (?)").bind(t.queue));
            } else if (t.status == TokenStatus.WAITING && t.kind == NodeKind.SLEEP) {
                session.execute(ps("INSERT INTO timer (shard,available_at,token_id,instance_id) VALUES (?,?,?,?)")
                        .bind(shard(t.id), t.availableAt, t.id, t.instanceId));
            } else if (t.status == TokenStatus.AWAITING && t.kind == NodeKind.SIGNAL) {
                session.execute(ps("INSERT INTO signal_wait (shard,created_at,token_id,instance_id,deadline) " +
                        "VALUES (?,?,?,?,?)").bind(shard(t.id), t.createdAt, t.id, t.instanceId, t.availableAt));
            } else if (t.status == TokenStatus.RUNNING && t.leaseExpiresAt > 0) {
                session.execute(ps("INSERT INTO lease (shard,lease_expires,token_id,instance_id) VALUES (?,?,?,?)")
                        .bind(shard(t.id), t.leaseExpiresAt, t.id, t.instanceId));
            }
        }

        private void writeInstanceIndex(Instance i) {
            session.execute(ps("INSERT INTO instance_index (bucket,created_at,id,workflow,status) VALUES (0,?,?,?,?)")
                    .bind(i.createdAt, i.id, i.workflow, i.status.name()));
        }
    }

    // ------------------------------------------------------------------------------------------
    // Row mapping and edge (de)normalisation
    // ------------------------------------------------------------------------------------------

    private static Instance readInstance(Row r) {
        Instance i = new Instance();
        i.id = r.getString("instance_id");
        i.workflow = r.getString("workflow");
        i.version = r.getInt("version");
        i.correlationId = r.getString("correlation_id");
        i.status = InstanceStatus.valueOf(r.getString("status"));
        i.terminationReason = r.getString("term_reason");
        i.error = r.getString("error");
        i.contextJson = r.getString("context");
        i.parentTokenId = r.getString("parent_token_id");
        i.createdAt = r.getLong("created_at");
        i.updatedAt = r.getLong("updated_at");
        i.revision = r.getLong("revision");
        return i;
    }

    private static Token readToken(Row r) {
        Token t = new Token();
        t.id = r.getString("row_id");
        t.instanceId = r.getString("instance_id");
        t.workflow = r.getString("workflow");
        t.version = r.getInt("version");
        t.nodeId = r.getString("node_id");
        t.kind = NodeKind.valueOf(r.getString("kind"));
        t.status = TokenStatus.valueOf(r.getString("status"));
        t.activity = r.getString("activity");
        t.queue = r.getString("queue");
        t.attempt = r.getInt("attempt");
        t.availableAt = r.getLong("available_at");
        t.leaseOwner = r.getString("lease_owner");
        t.leaseExpiresAt = r.getLong("lease_expires");
        String js = r.getString("join_stack");
        t.joinStack = js == null ? "" : js;
        t.lastError = r.getString("last_error");
        t.payloadJson = r.getString("payload");
        t.createdAt = r.getLong("created_at");
        t.updatedAt = r.getLong("updated_at");
        return t;
    }

    private static Rows.Schedule readSchedule(Row r) {
        Rows.Schedule s = new Rows.Schedule();
        s.id = r.getString("id");
        s.workflow = r.getString("workflow");
        s.intervalMillis = r.getLong("interval_millis");
        s.cron = r.getString("cron");
        s.contextJson = r.getString("context");
        s.nextFireAt = r.getLong("next_fire_at");
        s.createdAt = r.getLong("created_at");
        return s;
    }

    private record Edge(String to, String cond) { }

    /** Flattens a node's typed successors into ordered edge rows (mirrors the JDBC store). */
    private static List<Edge> edgesOf(Node n) {
        List<Edge> out = new ArrayList<>();
        switch (n.kind()) {
            case PREDICATE -> {
                if (n.next() != null) out.add(new Edge(n.next(), "true"));
                if (n.altNext() != null) out.add(new Edge(n.altNext(), "false"));
            }
            case FORK -> { for (String b : n.branches()) out.add(new Edge(b, "branch")); }
            case DYN_FORK -> {
                out.add(new Edge(n.branches().getFirst(), "branch"));
                if (n.next() != null) out.add(new Edge(n.next(), null));
            }
            case SIGNAL -> {
                if (n.next() != null) out.add(new Edge(n.next(), null));
                if (n.altNext() != null) out.add(new Edge(n.altNext(), "escalate"));
            }
            default -> { if (n.next() != null) out.add(new Edge(n.next(), null)); }
        }
        return out;
    }

    /** Folds edge rows back into a node's typed successor slots (mirrors the JDBC store). */
    private static final class EdgeTargets {
        private final NodeKind kind;
        String next;
        String altNext;
        final List<String> branches = new ArrayList<>();

        EdgeTargets(NodeKind kind) { this.kind = kind; }

        void absorb(String to, String cond) {
            if (kind == NodeKind.FORK || (kind == NodeKind.DYN_FORK && "branch".equals(cond))) {
                branches.add(to);
            } else if ((kind == NodeKind.PREDICATE && "false".equals(cond))
                    || (kind == NodeKind.SIGNAL && "escalate".equals(cond))) {
                altNext = to;
            } else {
                next = to;
            }
        }
    }
}
