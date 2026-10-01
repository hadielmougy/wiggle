package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.StorageException.Classification;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a caller is told when the store fails under an RPC, and what they are told it means.
 *
 * <p>The two cases must not look alike on the wire. A failure that applied nothing is
 * {@code UNAVAILABLE}: the client's own retry is built on that status precisely because it promises
 * the call never took effect, so the caller rides out a database blip. A commit whose outcome is
 * unknown is {@code INTERNAL} and is not retried by anyone -- the work may be durable, and
 * re-sending a non-idempotent start would double it.
 *
 * <p>Neither description may carry the driver's text: SQL, table and constraint names, host and
 * schema all leak through an exception message. The client gets a correlation id and the server log
 * gets the detail.
 */
class StorageFailureStatusTest {

    interface Steps {
        Map<String, Object> work(Map<String, Object> c);
    }

    private static ServerConfig config(String url) {
        return new ServerConfig(0, "node-0", url, TestStorage.user(), TestStorage.password(), 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    /** Fails every transaction with {@code failWith}, once a test has set it. */
    private record FlakyStorage(Storage delegate, AtomicReference<Classification> failWith,
                                AtomicInteger attempts) implements Storage {

        FlakyStorage(Storage delegate) { this(delegate, new AtomicReference<>(), new AtomicInteger()); }

        @Override public void migrate() { delegate.migrate(); }

        @Override public <R> R inTx(Function<Tx, R> work) {
            Classification how = failWith.get();
            if (how == null) return delegate.inTx(work);
            attempts.incrementAndGet();
            throw new StorageException("relation \"wf_instance\" on host db-7: connection reset", null, how);
        }

        @Override public String fingerprint() { return delegate.fingerprint(); }

        @Override public void close() { delegate.close(); }
    }

    private static FlowSpec spec() {
        return FlowSpec.define("storage-status", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    }

    /**
     * Runs {@code start} against a server whose store fails as {@code how}, with the client's own
     * retry bound to two quick attempts so an UNAVAILABLE run exercises it without a long wait.
     */
    private static Failure startAgainstAFailingStore(Classification how) throws Exception {
        String attempts = System.getProperty("wiggle.rpc.maxAttempts");
        String delay = System.getProperty("wiggle.rpc.retryDelayMillis");
        System.setProperty("wiggle.rpc.maxAttempts", "2");
        System.setProperty("wiggle.rpc.retryDelayMillis", "1");
        ServerConfig config = config(TestStorage.url("storage-status"));
        FlakyStorage storage = new FlakyStorage(new WiggleStorageFactory().create(config));
        try (WiggleServer server = new WiggleServer(config, c -> storage).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec());
            storage.failWith().set(how);
            storage.attempts().set(0);
            WiggleClient.WiggleApiException e = assertThrows(WiggleClient.WiggleApiException.class,
                    () -> client.start(spec(), Map.of("k", "v")));
            return new Failure(e, storage.attempts().get());
        } finally {
            storage.failWith().set(null);
            restore("wiggle.rpc.maxAttempts", attempts);
            restore("wiggle.rpc.retryDelayMillis", delay);
        }
    }

    private static void restore(String prop, String previous) {
        if (previous == null) System.clearProperty(prop);
        else System.setProperty(prop, previous);
    }

    private record Failure(WiggleClient.WiggleApiException thrown, int serverAttempts) { }

    @Test @DisplayName("a store failure that applied nothing is UNAVAILABLE, and the client retries it")
    void transientFailureIsUnavailable() throws Exception {
        Failure f = startAgainstAFailingStore(Classification.TRANSIENT);

        assertEquals(0, f.thrown().status(), "UNAVAILABLE is status 0: not reachable, nothing applied");
        assertTrue(f.thrown().getMessage().contains("storage temporarily unavailable"), f.thrown().getMessage());
        assertTrue(f.serverAttempts() > 1, "the client retried the call: " + f.serverAttempts() + " attempt(s)");
    }

    @Test @DisplayName("a store failure whose commit outcome is unknown is INTERNAL, and nobody retries it")
    void ambiguousFailureIsInternal() throws Exception {
        Failure f = startAgainstAFailingStore(Classification.AMBIGUOUS);

        assertEquals(500, f.thrown().status(), "an unknown commit is not a retry condition");
        assertTrue(f.thrown().getMessage().startsWith("internal error (ref "), f.thrown().getMessage());
        assertEquals(1, f.serverAttempts(), "the call was made once and not re-sent");
    }

    @Test @DisplayName("neither status carries the driver's text back to the caller")
    void nothingLeaksThroughTheDescription() throws Exception {
        for (Classification how : new Classification[] { Classification.TRANSIENT, Classification.AMBIGUOUS }) {
            String message = startAgainstAFailingStore(how).thrown().getMessage();
            assertTrue(message.contains("(ref "), how + " keeps a correlation id: " + message);
            assertTrue(!message.contains("wf_instance") && !message.contains("db-7"),
                    how + " leaked store internals: " + message);
        }
    }
}
