package com.wiggle.postgres;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Doc;
import com.wiggle.core.InstanceStatus;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.Op;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.jdbc.StatementListener;
import com.wiggle.server.auth.Scope;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.store.WriteRepositories;
import com.wiggle.tests.Scenarios;
import com.wiggle.tests.TestDb;
import com.wiggle.tests.WatchedRepositories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * No statement the store issues reads a whole table that grows with use. Every conformance scenario
 * runs on a PostgreSQL store, and then every repository operation is called directly, each optional
 * filter both set and unset; a store operation this test does not call fails it. Each distinct query,
 * insert, update and delete issued is then planned generically ({@code EXPLAIN (GENERIC_PLAN)},
 * PostgreSQL 16+) with sequential scans priced out, so a {@code Seq Scan} left in a plan is one no
 * index can replace. Only the tables in {@link #BOUNDED} may be read whole.
 *
 * Migrations, which run once against the schema they are changing, are not observed. Runs in a
 * schema of its own, dropped after. Opt-in, as {@link PostgresClaimTest}:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.postgres.NoTableScanTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class NoTableScanTest {

    /** Tables sized by the cluster and its configuration (nodes, accounts, consumers), not by its workload. */
    private static final Set<String> BOUNDED = Set.of("wf_schema_version", "wf_node", "wf_auth_user", "wf_auth_role",
            "wf_event_cursor");

    private static final Pattern PLANNABLE = Pattern.compile("^\\s*(SELECT|INSERT|UPDATE|DELETE|WITH|MERGE)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern SEQ_SCAN = Pattern.compile("Seq Scan on (\\w+)");

    private static final String WF = "noscan";
    private static final String QUEUE = "noscan-q";

    /** Every distinct statement text executed while attached. */
    private static final class Seen implements StatementListener {
        final Set<String> sql = ConcurrentHashMap.newKeySet();

        @Override public void executed(String s) {
            sql.add(s.replaceAll("\\s+", " ").trim());
        }

        @Override public void committed() { }
    }

    /** One step, for a graph to store. */
    interface OneStep {
        Map<String, Object> a(Map<String, Object> ctx);
    }

    @Test @DisplayName("no statement the store issues reads a growing table whole")
    void noStatementScansAGrowingTable() throws Exception {
        try (Connection c = connect(TestDb.url("PG"))) {
            assumeTrue(c.getMetaData().getDatabaseMajorVersion() >= 16, "EXPLAIN (GENERIC_PLAN) needs PostgreSQL 16");
        }
        String schema = "noscan_" + System.nanoTime();
        try (Connection c = connect(TestDb.url("PG")); Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA " + schema);
        }
        String url = TestDb.url("PG") + (TestDb.url("PG").contains("?") ? "&" : "?") + "currentSchema=" + schema;
        try {
            Seen seen = new Seen();
            runScenarios(url, seen);
            callEveryStoreOperation(url, seen);
            assertNoGrowingTableScanned(url, seen);
        } finally {
            try (Connection c = connect(TestDb.url("PG")); Statement st = c.createStatement()) {
                st.execute("DROP SCHEMA " + schema + " CASCADE");
            }
        }
    }

    private static void runScenarios(String url, Seen seen) throws Exception {
        Scenarios.useStorage(config -> {
            JdbcStorage storage = storage(url);
            storage.migrate();
            storage.observe(seen);
            return storage;
        });
        try {
            for (Method m : scenarios()) {
                try {
                    m.invoke(null);
                } catch (InvocationTargetException e) {
                    throw new AssertionError("scenario " + m.getName() + " failed", e.getCause());
                }
            }
        } finally {
            Scenarios.useStorage(null);
        }
    }

    private static void callEveryStoreOperation(String url, Seen seen) {
        Set<String> called = ConcurrentHashMap.newKeySet();
        try (JdbcStorage storage = storage(url)) {
            storage.migrate();
            JdbcStorage.Observation observation = storage.observe(seen);
            try {
                storage.inShard(0, r -> {
                    everyOperation(WatchedRepositories.watch(r, (m, args, out) -> called.add(signature(m))));
                    return null;
                });
            } finally {
                observation.close();
            }
        }
        Set<String> missed = new TreeSet<>();
        for (Method area : WriteRepositories.class.getMethods()) {
            if (area.getParameterCount() != 0 || !area.getReturnType().isInterface()) continue;
            for (Method m : area.getReturnType().getMethods()) {
                if (!Modifier.isStatic(m.getModifiers()) && !called.contains(signature(m))) missed.add(signature(m));
            }
        }
        assertEquals(Set.of(), missed, "store operations this test does not call, so cannot check");
    }

    private static void assertNoGrowingTableScanned(String url, Seen seen) throws SQLException {
        List<String> statements = seen.sql.stream()
                .filter(s -> PLANNABLE.matcher(s).find() && s.toLowerCase(Locale.ROOT).contains("wf_"))
                .sorted().toList();
        List<String> scans = new ArrayList<>();
        // Simple-query mode, so the server receives $1 as text for EXPLAIN (GENERIC_PLAN) to plan, not
        // as a parameter awaiting a value.
        try (Connection c = connect(url + "&preferQueryMode=simple"); Statement st = c.createStatement()) {
            st.execute("SET enable_seqscan = off");
            for (String sql : statements) {
                String plan = plan(st, sql);
                Set<String> whole = new TreeSet<>();
                Matcher m = SEQ_SCAN.matcher(plan);
                while (m.find()) if (!BOUNDED.contains(m.group(1))) whole.add(m.group(1));
                if (!whole.isEmpty()) scans.add(whole + " read whole by: " + sql + "\n" + plan);
            }
        }
        assertTrue(scans.isEmpty(), scans.size() + " of " + statements.size() + " statements read a table whole:\n\n"
                + String.join("\n", scans));
    }

    /**
     * Each repository operation at least once, and each optional filter both set and unset, so every
     * shape of statement the store builds is issued.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void everyOperation(WriteRepositories r) {
        long now = System.currentTimeMillis();
        FlowSpec flow = FlowSpec.define(WF, 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::a));
        WorkflowDefinition def = flow.definition();
        String node = def.startNode();
        Scope names = Scope.of(WF);
        Scope prefixed = Scope.parse(List.of("noscan.*", "other"));

        r.shards().claimShardIdentity(0);
        r.shards().writeShardBeat(now);
        r.shards().shardIdentity();
        r.shards().shardBeat();
        r.shards().putShardRecord(new Rows.ShardRecord(0, ShardState.ACTIVE, now, null));
        r.shards().shardRegistry();
        r.events().advanceEventPosition("c", 0, 1);
        r.events().eventPositions("c");
        r.events().oldestEventPosition(0);

        r.graph().putDefinition(WF, 1, "{}", "fp", "sha256");
        r.graph().replaceDefinition(WF, 1, "{}", "fp", "sha256");
        r.graph().definitionFingerprint(WF, 1);
        r.graph().definition(WF, 1);
        r.graph().latestVersion(WF);
        r.graph().definitionNames();
        r.graph().definitionVersions(WF);
        r.graph().putGraph(def);
        r.graph().graphNode(WF, 1, node);
        r.graph().graphStartNode(WF, 1);
        r.graph().graphNodeCount(WF, 1);

        Instance inst = instance("wfi-noscan-1", now);
        Instance other = instance("wfi-noscan-2", now);
        r.instances().insertInstance(inst);
        r.instances().insertInstance(other);
        r.instances().lockInstance(inst.id);
        r.instances().lockInstances(List.of(inst.id, other.id));
        r.instances().updateInstance(inst);
        r.instances().updateInstances(List.of(inst, other));
        r.instances().findInstance(inst.id);
        r.instances().listInstances(null, null, 10);
        r.instances().listInstances(WF, null, 10);
        r.instances().listInstances(null, InstanceStatus.RUNNING, 10);
        r.instances().listInstances(WF, InstanceStatus.RUNNING, 10);
        r.instances().findByCorrelation("corr", 10);
        r.instances().countInstances(InstanceStatus.RUNNING);
        r.instances().childInstanceIds(inst.id);
        r.dynamicNodes().insertDynNodes(inst.id, List.copyOf(def.nodes().values()));
        r.dynamicNodes().dynNode(node);
        r.dynamicNodes().dynNodeCount(inst.id);

        Token task = token("t-noscan-1", inst, node, now);
        task.attributes = Map.of("region", "eu", "n", 7L);
        task.attributesChanged = true;
        Token second = token("t-noscan-2", inst, node, now);
        Token third = token("t-noscan-3", inst, node, now);
        r.tokens().insertToken(task);
        r.tokens().insertTokens(List.of(second, third));
        r.tokens().updateToken(task);
        r.tokens().updateTokens(List.of(second, third));
        r.tokens().findToken(task.id);
        r.tokens().findTokens(List.of(task.id, second.id));
        r.tokens().tokensOf(inst.id);
        r.locks().lockInstanceOf(task.id);
        r.locks().lockTask(task.id);
        r.tokens().joinStacksAt(inst.id, node);
        r.tokens().joinedAt(inst.id, node);
        r.tokens().awaitingSignal(inst.id, "s");
        r.tokens().pendingSignals(10);
        r.tokens().hasActiveTokens(inst.id);
        r.tokens().queueDepth(now);
        r.tokens().backlogByVersion(now, 10);
        r.tokens().countReady(WF, 1, QUEUE, null, now, 100);
        r.tokens().countReady(WF, 1, QUEUE, QueueCriteria.from(QUEUE).where(cb -> cb.like(Task_.CORRELATION_ID, "eu-%")).where(),
                now, 100);
        r.tokens().countReady(WF, 1, QUEUE, QueueCriteria.from(QUEUE)
                .where(cb -> cb.equal(Task_.attr("region", String.class), "eu")).where(), now, 100);
        r.tokens().claimTasks("w", Set.of(QUEUE), null, 10, now, now + 60_000);
        r.tokens().claimTasks("w", Set.of(QUEUE), Set.of(new WorkflowVersion(WF, 1)), 10, now, now + 60_000);
        r.tokens().claimTasks("w", new ClaimFilter(List.of(
                QueueCriteria.from(QUEUE).where(cb -> cb.equal(Task_.attr("region", String.class), "eu")),
                QueueCriteria.from("noscan-other").where(cb -> cb.like(Task_.CORRELATION_ID, "eu-%"))), null),
                10, now, now + 60_000);
        r.tokens().renewLease(task.id, "w", now + 60_000, now);
        r.tokens().dueTimers(now, 10);
        r.tokens().dueRetries(now, 10);
        r.tokens().dueSignals(now, 10);
        r.tokens().expiredLeases(now, 10);
        r.tokens().stepDurations(WF, 1, now - 60_000, 10);

        Rows.CompLog comp = new Rows.CompLog();
        comp.instanceId = inst.id;
        comp.seq = 1;
        comp.nodeId = node;
        comp.activity = "a";
        comp.queue = QUEUE;
        comp.input = Doc.EMPTY;
        comp.result = Doc.EMPTY;
        r.compensation().appendCompensation(comp);
        r.compensation().compensationLog(inst.id);
        r.compensation().markCompensated(inst.id, 1);
        r.tokens().cancelActiveTokens(other.id, now);

        Rows.Schedule schedule = new Rows.Schedule();
        schedule.id = "sch-noscan";
        schedule.workflow = WF;
        schedule.intervalMillis = 60_000;
        schedule.nextFireAt = now;
        schedule.createdAt = now;
        r.schedules().putSchedule(schedule);
        r.schedules().schedules();
        r.schedules().scheduleByWorkflow(WF);
        r.schedules().dueSchedules(now, 10);
        r.schedules().claimSchedule(schedule.id, now, now + 60_000);
        r.schedules().deleteSchedule(schedule.id);

        Rows.ServerNode server = new Rows.ServerNode();
        server.id = "node-noscan";
        server.name = "noscan";
        server.firstHeartbeat = now;
        server.lastHeartbeat = now;
        r.cluster().upsertNode(server);
        r.cluster().setLeader(server.id, true);
        r.cluster().nodes();
        r.cluster().deleteNodesOlderThan(now - 60_000);

        long seq = r.events().appendEvent(new Rows.Event(0, inst.id, WF, 1, "corr", "STARTED", node, 1, "{}", now));
        r.events().createEventCursorIfAbsent(new Rows.EventCursor("c", 0, now, now));
        r.events().advanceEventCursor("c", seq, now);
        r.events().eventCursor("c");
        r.events().eventsAfter(0, 10);
        r.events().eventsAfter(0, now, 10);
        r.events().latestEventSeq();
        r.events().oldestAckedSeq();
        r.events().deleteEvents(now - 60_000, null, 10);
        r.events().deleteEvents(now - 60_000, seq, 10);

        r.auth().putAuthRole(new Rows.AuthRole("noscan-role", Set.of("instances.read"), false, now, now));
        r.auth().authRoles();
        r.auth().putAuthUser(new Rows.AuthUser("noscan-user", "h", "s", 1, false, now, now));
        r.auth().findAuthUser("noscan-user");
        r.auth().authUsers();
        r.auth().setAuthRolesOf("noscan-user", List.of("noscan-role"));
        r.auth().authRolesOf("noscan-user");
        r.auth().insertAuthCredential(new Rows.AuthCredential("cred-noscan", Rows.AuthCredential.API_KEY, "kh", null,
                "noscan-role", now, null));
        r.auth().authCredentials();
        r.auth().findAuthCredentialByKeyHash("kh");
        r.auth().findAuthCredentialBySubject("CN=noscan");
        r.auth().insertAuthSession(new Rows.AuthSession("sess-noscan", "noscan-user", now + 60_000, now));
        r.auth().findAuthSession("sess-noscan");
        r.auth().deleteAuthSessionsOf("noscan-user", "keep");
        r.auth().deleteExpiredAuthSessions(now, 10);
        r.auth().deleteAuthSession("sess-noscan");
        long audit = r.auth().appendAuthAudit(new Rows.AuthAudit(0, now, "noscan-user", "user.create", "noscan-user", null));
        r.auth().authAuditAfter(audit - 1, 10);
        r.auth().authAuditHead();
        r.auth().authAuditHas("user.create");
        r.auth().deleteAuthCredential("cred-noscan");
        r.auth().deleteAuthUser("noscan-user");
        r.auth().deleteAuthRole("noscan-role");

        Rows.SearchDoc doc = new Rows.SearchDoc(inst.id, WF, 1, "RUNNING", "corr", "order shipped", now, now);
        r.search().upsertSearchDoc(doc);
        r.search().searchDocs(new Rows.SearchQuery(null, null, null, null, null, 10));
        r.search().searchDocs(new Rows.SearchQuery("shipped", null, null, null, null, 10));
        r.search().searchDocs(new Rows.SearchQuery(null, names, "RUNNING", now - 60_000, now + 60_000, 10));
        r.search().searchDocs(new Rows.SearchQuery("shipped", prefixed, "RUNNING", now - 60_000, now + 60_000, 10));
        r.search().upsertSearchDoc(new Rows.SearchDoc(inst.id, WF, 1, "RUNNING", "corr", "order shipped", now, now,
                Map.of("region", "EU", "total", 5L, "rate", new java.math.BigDecimal("0.5"), "vip", true)));
        Scope wf = Scope.of(WF);
        r.search().searchDocs(new Rows.SearchQuery(null, wf, null, null, null, 10, List.of(
                new Rows.AttrFilter("region", Op.EQUAL, "EU"))));
        r.search().searchDocs(new Rows.SearchQuery(null, wf, null, null, null, 10, List.of(
                new Rows.AttrFilter("total", Op.GREATER_THAN_OR_EQUAL, 5L))));
        r.search().searchDocs(new Rows.SearchQuery(null, wf, null, null, null, 10, List.of(
                new Rows.AttrFilter("rate", Op.LESS_THAN, new java.math.BigDecimal("1")))));
        r.search().searchDocs(new Rows.SearchQuery(null, wf, null, null, null, 10, List.of(
                new Rows.AttrFilter("vip", Op.EQUAL, true))));
        r.search().searchDocs(new Rows.SearchQuery("shipped", wf, "RUNNING", now - 60_000, now + 60_000, 10, List.of(
                new Rows.AttrFilter("region", Op.EQUAL, "EU"), new Rows.AttrFilter("total", Op.LESS_THAN, 9L))));
        r.search().searchDocsAfter("", 10);
        r.search().deleteSearchDocsBefore(now - 60_000, 10);
        r.search().deleteSearchDoc(inst.id);

        r.instances().deleteTerminalInstancesBefore(now - 60_000, 10);
        r.graph().deleteGraph(WF, 1);
    }

    private static Instance instance(String id, long now) {
        Instance i = new Instance();
        i.id = id;
        i.workflow = WF;
        i.version = 1;
        i.correlationId = "corr";
        i.createdAt = now;
        i.updatedAt = now;
        return i;
    }

    private static Token token(String id, Instance inst, String node, long now) {
        Token t = new Token();
        t.id = id;
        t.instanceId = inst.id;
        t.workflow = WF;
        t.version = 1;
        t.nodeId = node;
        t.kind = NodeKind.TASK;
        t.status = TokenStatus.READY;
        t.activity = WF + "#a";
        t.queue = QUEUE;
        t.availableAt = now;
        t.instCreatedAt = inst.createdAt;
        t.correlationId = "eu-1";
        t.createdAt = now;
        t.updatedAt = now;
        return t;
    }

    private static String signature(Method m) {
        return m.getName() + Arrays.stream(m.getParameterTypes()).map(Class::getSimpleName).toList();
    }

    /** The public no-argument scenarios, in name order. */
    private static List<Method> scenarios() {
        List<Method> out = new ArrayList<>();
        for (Method m : Scenarios.class.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers()) && Modifier.isStatic(m.getModifiers())
                    && m.getParameterCount() == 0 && m.getReturnType() == void.class) {
                out.add(m);
            }
        }
        out.sort((a, b) -> a.getName().compareTo(b.getName()));
        return out;
    }

    /** {@code sql}'s generic plan, its JDBC placeholders numbered as PostgreSQL's. */
    private static String plan(Statement st, String sql) {
        StringBuilder plan = new StringBuilder();
        try (ResultSet rs = st.executeQuery("EXPLAIN (GENERIC_PLAN) " + numbered(sql))) {
            while (rs.next()) plan.append(rs.getString(1)).append('\n');
        } catch (SQLException e) {
            throw new AssertionError("cannot plan: " + sql, e);
        }
        return plan.toString();
    }

    /** {@code sql} with each {@code ?} outside a quoted literal replaced by {@code $1}, {@code $2}, ... */
    private static String numbered(String sql) {
        StringBuilder out = new StringBuilder(sql.length() + 8);
        boolean quoted = false;
        int n = 0;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '\'') quoted = !quoted;
            if (ch == '?' && !quoted) out.append('$').append(++n);
            else out.append(ch);
        }
        return out.toString();
    }

    private static JdbcStorage storage(String url) {
        return new JdbcStorage(url, TestDb.user("PG"), TestDb.password("PG"), 8, new PostgresDialect());
    }

    private static Connection connect(String url) throws SQLException {
        return DriverManager.getConnection(url, TestDb.user("PG"), TestDb.password("PG"));
    }
}
