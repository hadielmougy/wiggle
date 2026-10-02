package com.wiggle.tests;

import com.wiggle.core.Ids;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.PayloadCodec;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.TokenPayload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The narrow token writes: an update leaves the payload column alone while the row's payload is
 * the object the store last read or wrote, and a lease renewal writes only the expiry. Each case
 * plants a payload out of band first, so a statement that rewrote the column would be caught
 * overwriting it. Runs on H2, and on PostgreSQL too when {@code WIGGLE_TEST_PG_URL} is set.
 */
class JdbcTokenWritesTest {

    record Db(String name, String url, String user, String password, boolean postgres) {
        @Override public String toString() { return name; }

        JdbcStorage open() {
            JdbcStorage s = new JdbcStorage(url, user, password, 4, postgres ? new PostgresDialect() : new H2Dialect());
            s.migrate();
            return s;
        }
    }

    static List<Db> databases() {
        List<Db> dbs = new ArrayList<>();
        dbs.add(new Db("h2", "jdbc:h2:mem:token-writes-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
                "sa", "", false));
        String pg = TestDb.url("PG");
        if (pg != null) dbs.add(new Db("postgres", pg, TestDb.user("PG"), TestDb.password("PG"), true));
        return dbs;
    }

    private static final TokenPayload PLANTED = TokenPayload.EMPTY.withLoopCount("planted", 7);

    private static Token running(String owner) {
        Token t = new Token();
        t.id = Ids.next("tok");
        t.instanceId = Ids.next("wfi");
        t.workflow = "token-writes";
        t.version = 1;
        t.nodeId = "n";
        t.kind = NodeKind.TASK;
        t.status = TokenStatus.RUNNING;
        t.queue = "q";
        t.leaseOwner = owner;
        t.leaseExpiresAt = 1_000;
        t.createdAt = 1;
        t.updatedAt = 1;
        return t;
    }

    private static boolean renew(JdbcStorage storage, String id, String owner, long until, long now) {
        Boolean renewed = storage.inTx(tx -> tx.renewLease(id, owner, until, now));
        return renewed;
    }

    private static void plant(Db db, String tokenId) throws Exception {
        try (Connection c = DriverManager.getConnection(db.url(), db.user(), db.password());
             PreparedStatement ps = c.prepareStatement("UPDATE wf_token SET payload=? WHERE id=?")) {
            ps.setString(1, PayloadCodec.encode(PLANTED));
            ps.setString(2, tokenId);
            assertEquals(1, ps.executeUpdate());
        }
    }

    private static TokenPayload storedPayload(Db db, String tokenId) throws Exception {
        try (Connection c = DriverManager.getConnection(db.url(), db.user(), db.password());
             PreparedStatement ps = c.prepareStatement("SELECT payload FROM wf_token WHERE id=?")) {
            ps.setString(1, tokenId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return PayloadCodec.decode(rs.getString(1));
            }
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("an update whose payload is unchanged leaves the column as the store holds it")
    void unchangedPayloadIsNotRewritten(Db db) throws Exception {
        try (JdbcStorage storage = db.open()) {
            Token t = running("w1");
            storage.inTxVoid(tx -> tx.insertToken(t));
            plant(db, t.id);

            storage.inTxVoid(tx -> {
                Token live = tx.findToken(t.id).orElseThrow();
                live.status = TokenStatus.DONE;
                live.updatedAt = 2;
                tx.updateToken(live);
            });

            assertEquals(PLANTED, storedPayload(db, t.id), "the payload column was not written");
            Token back = storage.inTx(tx -> tx.findToken(t.id)).orElseThrow();
            assertEquals(TokenStatus.DONE, back.status, "the other columns were");
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("a row carried in from another transaction has its payload written")
    void rowFromAnotherTransactionIsWrittenWhole(Db db) throws Exception {
        try (JdbcStorage storage = db.open()) {
            Token t = running("w1");
            storage.inTxVoid(tx -> tx.insertToken(t));
            plant(db, t.id);

            t.status = TokenStatus.DONE;
            storage.inTxVoid(tx -> tx.updateToken(t));

            assertEquals(t.payload, storedPayload(db, t.id), "the record from the insert's transaction is not trusted");
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("a payload written by a transaction that rolled back is written again by the next")
    void payloadFromRolledBackTransactionIsRewritten(Db db) throws Exception {
        try (JdbcStorage storage = db.open()) {
            Token t = running("w1");
            storage.inTxVoid(tx -> tx.insertToken(t));
            TokenPayload next = TokenPayload.EMPTY.withLoopCount("loop", 9);
            AtomicReference<Token> carried = new AtomicReference<>();

            assertThrows(IllegalStateException.class, () -> storage.inTxVoid(tx -> {
                Token live = tx.findToken(t.id).orElseThrow();
                live.payload = next;
                tx.updateToken(live);
                carried.set(live);
                throw new IllegalStateException("roll back");
            }));
            assertEquals(TokenPayload.EMPTY, storedPayload(db, t.id), "the rolled-back write did not land");

            storage.inTxVoid(tx -> tx.updateToken(carried.get()));
            assertEquals(next, storedPayload(db, t.id), "the next transaction wrote the payload");
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("an update whose payload changed writes it")
    void changedPayloadIsWritten(Db db) throws Exception {
        try (JdbcStorage storage = db.open()) {
            Token t = running("w1");
            storage.inTxVoid(tx -> tx.insertToken(t));
            plant(db, t.id);

            TokenPayload next = TokenPayload.EMPTY.withLoopCount("loop", 3);
            t.payload = next;
            storage.inTxVoid(tx -> tx.updateToken(t));

            assertEquals(next, storedPayload(db, t.id));
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("a row read from the store and updated untouched keeps its payload; a batch splits by change")
    void batchSplitsByPayloadChange(Db db) throws Exception {
        try (JdbcStorage storage = db.open()) {
            Token kept = running("w1");
            Token changed = running("w1");
            storage.inTxVoid(tx -> tx.insertTokens(List.of(kept, changed)));
            plant(db, kept.id);
            plant(db, changed.id);

            TokenPayload next = TokenPayload.EMPTY.withLoopCount("loop", 5);
            storage.inTxVoid(tx -> {
                Token k = tx.findToken(kept.id).orElseThrow();
                Token c = tx.findToken(changed.id).orElseThrow();
                k.status = TokenStatus.DONE;
                c.status = TokenStatus.DONE;
                c.payload = next;
                tx.updateTokens(List.of(k, c));
            });

            assertEquals(PLANTED, storedPayload(db, kept.id), "read then updated untouched: not rewritten");
            assertEquals(next, storedPayload(db, changed.id));
            assertEquals(TokenStatus.DONE, storage.inTx(tx -> tx.findToken(kept.id)).orElseThrow().status);
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("a batch naming one row twice still applies both writes in order")
    void batchWithRepeatedRowKeepsOrder(Db db) {
        try (JdbcStorage storage = db.open()) {
            Token t = running("w1");
            storage.inTxVoid(tx -> tx.insertToken(t));

            storage.inTxVoid(tx -> {
                Token first = tx.findToken(t.id).orElseThrow();
                Token second = tx.findToken(t.id).orElseThrow();
                first.payload = TokenPayload.EMPTY.withLoopCount("loop", 1);
                first.status = TokenStatus.READY;
                second.status = TokenStatus.DONE;
                tx.updateTokens(List.of(first, second));
            });

            assertEquals(TokenStatus.DONE, storage.inTx(tx -> tx.findToken(t.id)).orElseThrow().status,
                    "the later write of the row wins");
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("a lease renewal writes only the expiry, and only for the holder of a RUNNING token")
    void renewLeaseIsNarrow(Db db) throws Exception {
        try (JdbcStorage storage = db.open()) {
            Token t = running("w1");
            storage.inTxVoid(tx -> tx.insertToken(t));
            plant(db, t.id);

            assertFalse(renew(storage, t.id, "w2", 9_000, 5), "another worker's lease");
            assertFalse(renew(storage, "tok-missing", "w1", 9_000, 5), "no such task");
            assertTrue(renew(storage, t.id, "w1", 9_000, 5));
            assertTrue(renew(storage, t.id, null, 9_500, 6), "a null owner matches any holder");

            Token back = storage.inTx(tx -> tx.findToken(t.id)).orElseThrow();
            assertEquals(9_500, back.leaseExpiresAt);
            assertEquals(6, back.updatedAt);
            assertEquals("w1", back.leaseOwner);
            assertEquals(PLANTED, storedPayload(db, t.id), "the payload column was not written");

            back.status = TokenStatus.DONE;
            storage.inTxVoid(tx -> tx.updateToken(back));
            assertFalse(renew(storage, t.id, "w1", 10_000, 7), "a settled token");
        }
    }

    @ParameterizedTest @MethodSource("databases")
    @DisplayName("a refused heartbeat still says why: unknown task, someone else's lease, not running")
    void extendLeaseRefusals(Db db) {
        try (JdbcStorage storage = db.open()) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            Token t = running("w1");
            storage.inTxVoid(tx -> tx.insertToken(t));

            long until = engine.extendLease(t.id, "w1", 60_000);
            assertTrue(until > System.currentTimeMillis());
            assertEquals(until, storage.inTx(tx -> tx.findToken(t.id)).orElseThrow().leaseExpiresAt);

            assertEquals(404, assertThrows(EngineException.class,
                    () -> engine.extendLease("tok-missing", "w1", 1_000)).statusCode());
            assertEquals(409, assertThrows(EngineException.class,
                    () -> engine.extendLease(t.id, "w2", 1_000)).statusCode());

            t.status = TokenStatus.DONE;
            storage.inTxVoid(tx -> tx.updateToken(t));
            assertEquals(409, assertThrows(EngineException.class,
                    () -> engine.extendLease(t.id, "w1", 1_000)).statusCode());
        }
    }
}
