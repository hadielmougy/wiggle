package com.wiggle.postgres;

import com.wiggle.core.Ids;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The PostgreSQL claim for a worker on several queues, which picks each queue's earliest rows and
 * merges them. Opt-in like {@link PostgresClaimTest}: set {@code WIGGLE_TEST_PG_URL}.
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PostgresMultiQueueClaimTest {

    private static final long NOW = 1_000_000;
    private static final long LEASE = NOW + 30_000;

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(TestDb.url("PG"), TestDb.user("PG"), TestDb.password("PG"), 8,
                new PostgresDialect());
        storage.migrate();
        return storage;
    }

    private static Token ready(String queue, String workflow, int version, long availableAt) {
        Token t = new Token();
        t.id = Ids.next("tok");
        t.instanceId = Ids.next("wfi");
        t.workflow = workflow;
        t.version = version;
        t.nodeId = "n";
        t.kind = NodeKind.TASK;
        t.status = TokenStatus.READY;
        t.queue = queue;
        t.availableAt = availableAt;
        t.createdAt = 1;
        t.updatedAt = 1;
        return t;
    }

    private static List<Token> claim(JdbcStorage storage, String worker, Set<String> queues,
                                     Set<WorkflowVersion> versions, int max) {
        return storage.inTx(tx -> tx.claimTasks(worker, queues, versions, max, NOW, LEASE));
    }

    private static List<Long> times(List<Token> tokens) {
        return tokens.stream().map(t -> t.availableAt).sorted().toList();
    }

    @Test @DisplayName("several queues are claimed in one availability order across them")
    void mergesQueuesInAvailabilityOrder() {
        String q1 = Ids.next("mq"), q2 = Ids.next("mq");
        try (JdbcStorage storage = storage()) {
            List<Token> rows = List.of(ready(q1, "w", 1, 10), ready(q1, "w", 1, 40), ready(q1, "w", 1, 50),
                    ready(q2, "w", 1, 20), ready(q2, "w", 1, 30));
            storage.inTxVoid(tx -> tx.insertTokens(rows));

            List<Token> first = claim(storage, "w1", Set.of(q1, q2), null, 3);
            assertEquals(List.of(10L, 20L, 30L), times(first), "the three earliest, from both queues");
            assertTrue(first.stream().allMatch(t -> t.status == TokenStatus.RUNNING && "w1".equals(t.leaseOwner)
                    && t.leaseExpiresAt == LEASE), "claimed rows come back leased");
            assertEquals(List.of(40L, 50L), times(claim(storage, "w1", Set.of(q1, q2), null, 10)));
            assertTrue(claim(storage, "w1", Set.of(q1, q2), null, 10).isEmpty(), "nothing left");
        }
    }

    @Test @DisplayName("only due, READY task rows of the named queues and versions are claimed")
    void respectsTheFilter() {
        String q1 = Ids.next("mq"), q2 = Ids.next("mq"), q3 = Ids.next("mq");
        try (JdbcStorage storage = storage()) {
            Token due = ready(q1, "w", 2, 10);
            Token otherVersion = ready(q2, "w", 1, 11);
            Token notDue = ready(q2, "w", 2, NOW + 1);
            Token otherQueue = ready(q3, "w", 2, 12);
            Token waiting = ready(q1, "w", 2, 13);
            waiting.status = TokenStatus.WAITING;
            Token join = ready(q2, "w", 2, 14);
            join.kind = NodeKind.JOIN;
            Token alsoDue = ready(q2, "w", 2, 15);
            storage.inTxVoid(tx -> tx.insertTokens(List.of(due, otherVersion, notDue, otherQueue, waiting, join, alsoDue)));

            List<Token> claimed = claim(storage, "w1", Set.of(q1, q2), Set.of(new WorkflowVersion("w", 2)), 10);
            assertEquals(Set.of(due.id, alsoDue.id), Set.copyOf(claimed.stream().map(t -> t.id).toList()));
        }
    }

    @Test @DisplayName("concurrent claimers on several queues take every row exactly once")
    void concurrentClaimsAreExclusive() throws Exception {
        String q1 = Ids.next("mq"), q2 = Ids.next("mq"), q3 = Ids.next("mq");
        int perQueue = 40;
        try (JdbcStorage storage = storage()) {
            List<Token> rows = new ArrayList<>();
            for (int i = 0; i < perQueue; i++) {
                rows.add(ready(q1, "w", 1, 100 + i));
                rows.add(ready(q2, "w", 1, 100 + i));
                rows.add(ready(q3, "w", 1, 100 + i));
            }
            storage.inTxVoid(tx -> tx.insertTokens(rows));

            CountDownLatch go = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(4);
            try {
                List<Future<List<Token>>> claimers = new ArrayList<>();
                for (int w = 0; w < 4; w++) {
                    String worker = "w" + w;
                    claimers.add(pool.submit(() -> {
                        go.await();
                        List<Token> mine = new ArrayList<>();
                        for (List<Token> got = claim(storage, worker, Set.of(q1, q2, q3), null, 8); !got.isEmpty();
                             got = claim(storage, worker, Set.of(q1, q2, q3), null, 8)) {
                            mine.addAll(got);
                        }
                        return mine;
                    }));
                }
                go.countDown();
                Set<String> seen = new HashSet<>();
                for (Future<List<Token>> f : claimers) {
                    for (Token t : f.get()) assertTrue(seen.add(t.id), "token " + t.id + " was claimed twice");
                }
                assertEquals(3 * perQueue, seen.size(), "every row claimed");
            } finally {
                pool.shutdownNow();
            }
        }
    }
}
