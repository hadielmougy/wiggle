package com.wiggle.server.store;

import com.wiggle.core.InstanceStatus;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The status enums are the one authority on what a status means, so every other spelling of those
 * rules has to agree with them. The Java copies delegate and cannot drift. The SQL copies cannot
 * delegate -- a query can only restate the rule -- so the active-token queries are driven against a
 * real database and compared to the enum, which is the only way a dropped status in a WHERE clause
 * gets caught.
 */
class StatusRulesTest {

    @Test
    @DisplayName("an instance is live while going forwards or undoing, and terminal otherwise")
    void instanceLiveness() {
        Set<InstanceStatus> live = EnumSet.of(InstanceStatus.RUNNING, InstanceStatus.COMPENSATING);
        for (InstanceStatus s : InstanceStatus.values()) {
            assertEquals(live.contains(s), s.live(), s + ".live()");
            assertEquals(s == InstanceStatus.RUNNING, s.running(), s + ".running()");
            assertEquals(s == InstanceStatus.COMPENSATING, s.compensating(), s + ".compensating()");
            assertFalse(s.running() && s.compensating(), s + " cannot go both ways at once");
            assertEquals(s.live(), s.running() || s.compensating(), s + ": live is exactly the two phases");
        }
    }

    @Test
    @DisplayName("a status this build cannot name is never treated as still running")
    void unknownStatusIsNotLive() {
        for (InstanceStatus s : InstanceStatus.values()) {
            assertEquals(s.live(), InstanceStatus.liveByName(s.name()), s + " by name");
            assertEquals(s.running(), InstanceStatus.runningByName(s.name()), s + " by name");
        }
        assertFalse(InstanceStatus.liveByName("SOMETHING_NEWER"));
        assertFalse(InstanceStatus.runningByName("SOMETHING_NEWER"));
        assertFalse(InstanceStatus.liveByName(null));
    }

    @Test
    @DisplayName("a token is active while it is doing something or waiting to")
    void tokenLiveness() {
        Set<TokenStatus> active = EnumSet.of(TokenStatus.READY, TokenStatus.RUNNING,
                TokenStatus.WAITING, TokenStatus.AWAITING, TokenStatus.JOINED);
        for (TokenStatus s : TokenStatus.values()) {
            assertEquals(active.contains(s), s.active(), s + ".active()");
        }
    }

    @Test
    @DisplayName("the JDBC active-token queries agree with the enum, one status at a time")
    void jdbcActiveTokensAgreeWithTheEnum() throws Exception {
        withJdbc(storage -> {
            for (TokenStatus s : TokenStatus.values()) {
                String id = "wfi_" + s.name();
                storage.inTxVoid(tx -> {
                    tx.insertInstance(instance(id, InstanceStatus.RUNNING));
                    tx.insertToken(token(id, s));
                });
                assertEquals(s.active(), storage.inTx(tx -> tx.hasActiveTokens(id)),
                        "hasActiveTokens disagrees with " + s + ".active()");
            }
            for (TokenStatus s : TokenStatus.values()) {
                String id = "wfi_" + s.name();
                storage.inTxVoid(tx -> tx.cancelActiveTokens(id, 2_000));
                TokenStatus after = storage.inTx(tx -> tx.tokensOf(id).getFirst().status);
                assertEquals(s.active() ? TokenStatus.CANCELLED : s, after,
                        "cancelActiveTokens touched the wrong set for " + s);
                boolean stillActive = storage.inTx(tx -> tx.hasActiveTokens(id));
                assertFalse(stillActive, "nothing active after a cancel");
            }
        });
    }

    private interface Body { void run(Storage storage) throws Exception; }

    private static void withJdbc(Body body) throws Exception {
        String url = "jdbc:h2:mem:status-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (com.wiggle.jdbc.JdbcStorage storage = new com.wiggle.jdbc.JdbcStorage(
                url, "sa", "", 2, new com.wiggle.postgres.H2Dialect())) {
            storage.migrate();
            body.run(storage);
        }
    }

    private static Instance instance(String id, InstanceStatus status) {
        Instance i = new Instance();
        i.id = id;
        i.workflow = "w";
        i.version = 1;
        i.status = status;
        i.createdAt = 0;
        i.updatedAt = 0;
        return i;
    }

    private static Token token(String instanceId, TokenStatus status) {
        Token t = new Token();
        t.id = "tok_" + instanceId;
        t.instanceId = instanceId;
        t.workflow = "w";
        t.version = 1;
        t.nodeId = "n";
        t.kind = NodeKind.TASK;
        t.status = status;
        t.queue = "q";
        t.createdAt = 0;
        t.updatedAt = 0;
        return t;
    }
}
