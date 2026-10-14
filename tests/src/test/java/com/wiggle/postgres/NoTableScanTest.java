package com.wiggle.postgres;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Doc;
import com.wiggle.core.InstanceStatus;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.WorkflowVersion;
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
import com.wiggle.server.store.Tx;
import com.wiggle.tests.Scenarios;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
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
 * runs on a PostgreSQL store, and then every {@link Tx} operation is called directly, each optional
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

    /** Tables sized by the cluster and its configuration (nodes, accounts, consumers, models), not by its workload. */
    private static final Set<String> BOUNDED = Set.of("wf_schema_version", "wf_node", "wf_auth_user", "wf_auth_role",
            "wf_event_cursor", "wf_search_model");

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
                storage.inTxVoid(tx -> everyOperation(recording(tx, called)));
            } finally {
                observation.close();
            }
        }
        Set<String> missed = new TreeSet<>();
        for (Method m : Tx.class.getMethods()) {
            if (!Modifier.isStatic(m.getModifiers()) && !m.getName().equals("transactional")
                    && !called.contains(signature(m))) {
                missed.add(signature(m));
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
     * Each {@link Tx} operation at least once, and each optional filter both set and unset, so every
     * shape of statement the store builds is issued.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void everyOperation(Tx tx) {
        long now = System.currentTimeMillis();
        FlowSpec flow = FlowSpec.define(WF, 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::a));
        WorkflowDefinition def = flow.definition();
        String node = def.startNode();
        Scope names = Scope.of(WF);
        Scope prefixed = Scope.parse(List.of("noscan.*", "other"));

        tx.claimShardIdentity(0);
        tx.writeShardBeat(now);
        tx.shardIdentity();
        tx.shardBeat();
        tx.putShardRecord(new Rows.ShardRecord(0, ShardState.ACTIVE, now, null));
        tx.shardRegistry();
        tx.advanceEventPosition("c", 0, 1);
        tx.eventPositions("c");
        tx.oldestEventPosition(0);

        tx.putDefinition(WF, 1, "{}", "fp", "sha256");
        tx.replaceDefinition(WF, 1, "{}", "fp", "sha256");
        tx.definitionFingerprint(WF, 1);
        tx.definition(WF, 1);
        tx.latestVersion(WF);
        tx.definitionNames();
        tx.definitionVersions(WF);
        tx.putGraph(def);
        tx.graphNode(WF, 1, node);
        tx.graphStartNode(WF, 1);
        tx.graphNodeCount(WF, 1);

        Instance inst = instance("wfi-noscan-1", now);
        Instance other = instance("wfi-noscan-2", now);
        tx.insertInstance(inst);
        tx.insertInstance(other);
        tx.lockInstance(inst.id);
        tx.lockInstances(List.of(inst.id, other.id));
        tx.updateInstance(inst);
        tx.updateInstances(List.of(inst, other));
        tx.findInstance(inst.id);
        tx.listInstances(null, null, 10);
        tx.listInstances(WF, null, 10);
        tx.listInstances(null, InstanceStatus.RUNNING, 10);
        tx.listInstances(WF, InstanceStatus.RUNNING, 10);
        tx.findByCorrelation("corr", 10);
        tx.countInstances(InstanceStatus.RUNNING);
        tx.childInstanceIds(inst.id);
        tx.insertDynNodes(inst.id, List.copyOf(def.nodes().values()));
        tx.dynNode(node);
        tx.dynNodeCount(inst.id);

        Token task = token("t-noscan-1", inst, node, now);
        task.attributes = Map.of("region", "eu", "n", 7L);
        task.attributesChanged = true;
        Token second = token("t-noscan-2", inst, node, now);
        Token third = token("t-noscan-3", inst, node, now);
        tx.insertToken(task);
        tx.insertTokens(List.of(second, third));
        tx.updateToken(task);
        tx.updateTokens(List.of(second, third));
        tx.findToken(task.id);
        tx.findTokens(List.of(task.id, second.id));
        tx.tokensOf(inst.id);
        tx.lockInstanceOf(task.id);
        tx.lockTask(task.id);
        tx.joinStacksAt(inst.id, node);
        tx.joinedAt(inst.id, node);
        tx.awaitingSignal(inst.id, "s");
        tx.pendingSignals(10);
        tx.hasActiveTokens(inst.id);
        tx.queueDepth(now);
        tx.backlogByVersion(now, 10);
        tx.countReady(WF, 1, QUEUE, null, now, 100);
        tx.countReady(WF, 1, QUEUE, QueueCriteria.from(QUEUE).where(cb -> cb.like(Task_.CORRELATION_ID, "eu-%")).where(),
                now, 100);
        tx.countReady(WF, 1, QUEUE, QueueCriteria.from(QUEUE)
                .where(cb -> cb.equal(Task_.attr("region", String.class), "eu")).where(), now, 100);
        tx.claimTasks("w", Set.of(QUEUE), null, 10, now, now + 60_000);
        tx.claimTasks("w", Set.of(QUEUE), Set.of(new WorkflowVersion(WF, 1)), 10, now, now + 60_000);
        tx.claimTasks("w", new ClaimFilter(List.of(
                QueueCriteria.from(QUEUE).where(cb -> cb.equal(Task_.attr("region", String.class), "eu")),
                QueueCriteria.from("noscan-other").where(cb -> cb.like(Task_.CORRELATION_ID, "eu-%"))), null),
                10, now, now + 60_000);
        tx.renewLease(task.id, "w", now + 60_000, now);
        tx.dueTimers(now, 10);
        tx.dueRetries(now, 10);
        tx.dueSignals(now, 10);
        tx.expiredLeases(now, 10);
        tx.stepDurations(WF, 1, now - 60_000, 10);

        Rows.CompLog comp = new Rows.CompLog();
        comp.instanceId = inst.id;
        comp.seq = 1;
        comp.nodeId = node;
        comp.activity = "a";
        comp.queue = QUEUE;
        comp.input = Doc.EMPTY;
        comp.result = Doc.EMPTY;
        tx.appendCompensation(comp);
        tx.compensationLog(inst.id);
        tx.markCompensated(inst.id, 1);
        tx.cancelActiveTokens(other.id, now);

        Rows.Schedule schedule = new Rows.Schedule();
        schedule.id = "sch-noscan";
        schedule.workflow = WF;
        schedule.intervalMillis = 60_000;
        schedule.nextFireAt = now;
        schedule.createdAt = now;
        tx.putSchedule(schedule);
        tx.schedules();
        tx.scheduleByWorkflow(WF);
        tx.dueSchedules(now, 10);
        tx.claimSchedule(schedule.id, now, now + 60_000);
        tx.deleteSchedule(schedule.id);

        Rows.ServerNode server = new Rows.ServerNode();
        server.id = "node-noscan";
        server.name = "noscan";
        server.firstHeartbeat = now;
        server.lastHeartbeat = now;
        tx.upsertNode(server);
        tx.setLeader(server.id, true);
        tx.nodes();
        tx.deleteNodesOlderThan(now - 60_000);

        long seq = tx.appendEvent(new Rows.Event(0, inst.id, WF, 1, "corr", "STARTED", node, 1, "{}", now));
        tx.createEventCursorIfAbsent(new Rows.EventCursor("c", 0, now, now));
        tx.advanceEventCursor("c", seq, now);
        tx.eventCursor("c");
        tx.eventsAfter(0, 10);
        tx.eventsAfter(0, now, 10);
        tx.latestEventSeq();
        tx.oldestAckedSeq();
        tx.deleteEvents(now - 60_000, null, 10);
        tx.deleteEvents(now - 60_000, seq, 10);

        tx.putAuthRole(new Rows.AuthRole("noscan-role", Set.of("instances.read"), false, now, now));
        tx.authRoles();
        tx.putAuthUser(new Rows.AuthUser("noscan-user", "h", "s", 1, false, now, now));
        tx.findAuthUser("noscan-user");
        tx.authUsers();
        tx.setAuthRolesOf("noscan-user", List.of("noscan-role"));
        tx.authRolesOf("noscan-user");
        tx.insertAuthCredential(new Rows.AuthCredential("cred-noscan", Rows.AuthCredential.API_KEY, "kh", null,
                "noscan-role", now, null));
        tx.authCredentials();
        tx.findAuthCredentialByKeyHash("kh");
        tx.findAuthCredentialBySubject("CN=noscan");
        tx.insertAuthSession(new Rows.AuthSession("sess-noscan", "noscan-user", now + 60_000, now));
        tx.findAuthSession("sess-noscan");
        tx.deleteAuthSessionsOf("noscan-user", "keep");
        tx.deleteExpiredAuthSessions(now, 10);
        tx.deleteAuthSession("sess-noscan");
        long audit = tx.appendAuthAudit(new Rows.AuthAudit(0, now, "noscan-user", "user.create", "noscan-user", null));
        tx.authAuditAfter(audit - 1, 10);
        tx.authAuditHead();
        tx.authAuditHas("user.create");
        tx.deleteAuthCredential("cred-noscan");
        tx.deleteAuthUser("noscan-user");
        tx.deleteAuthRole("noscan-role");

        Rows.SearchDoc doc = new Rows.SearchDoc(inst.id, WF, 1, "RUNNING", "corr", "order shipped", now, now);
        tx.upsertSearchDoc(doc);
        tx.searchDocs(new Rows.SearchQuery(null, null, null, null, null, 10));
        tx.searchDocs(new Rows.SearchQuery("shipped", null, null, null, null, 10));
        tx.searchDocs(new Rows.SearchQuery(null, names, "RUNNING", now - 60_000, now + 60_000, 10));
        tx.searchDocs(new Rows.SearchQuery("shipped", prefixed, "RUNNING", now - 60_000, now + 60_000, 10));
        tx.searchDocsAfter("", 10);
        tx.putSearchModel(new Rows.SearchModel("m", 3, Rows.SearchModel.BUILDING, now, null));
        tx.searchModels();
        tx.ensureVectorIndex("m", 3);
        tx.upsertSearchVectors(List.of(new Rows.SearchVector(inst.id, "m", new float[]{1, 0, 0}, now)));
        tx.searchVectorsOf(List.of(inst.id));
        tx.searchVectors(new Rows.VectorQuery("m", new float[]{1, 0, 0}, null, null, null, null, 10));
        tx.searchVectors(new Rows.VectorQuery("m", new float[]{1, 0, 0}, names, "RUNNING", now - 60_000,
                now + 60_000, 10));
        tx.docsNeedingVector("m", 10);
        tx.countDocsWithoutVector("m", now + 60_000);
        tx.deleteSearchVectors("m", 10);
        tx.deleteSearchDocsBefore(now - 60_000, 10);
        tx.deleteSearchDoc(inst.id);

        tx.deleteTerminalInstancesBefore(now - 60_000, 10);
        tx.deleteGraph(WF, 1);
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

    /** {@code tx}, adding the signature of each operation called on it to {@code called}. */
    private static Tx recording(Tx tx, Set<String> called) {
        return (Tx) Proxy.newProxyInstance(NoTableScanTest.class.getClassLoader(), new Class<?>[]{Tx.class},
                (proxy, m, args) -> {
                    called.add(signature(m));
                    try {
                        return m.invoke(tx, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
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
