package com.wiggle.server.auth;

import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A node's cache of accounts and sessions: how long an entry lives, what drops it, and an outage. */
class AuthCacheTest {

    /** An auth shard that can be taken down. */
    private static final class Flaky implements Storage {
        final InMemoryStorage mem = new InMemoryStorage();
        volatile boolean down;
        int reads;

        @Override public void migrate() { mem.migrate(); }

        @Override public <R> R inTx(Function<Tx, R> work) {
            if (down) throw new StorageException("auth shard down", null, StorageException.Classification.TRANSIENT);
            reads++;
            return mem.inTx(work);
        }

        @Override public void close() { }
    }

    private final AtomicLong clock = new AtomicLong(1_000_000);

    private Accounts accounts(Flaky store) {
        store.migrate();
        Accounts a = new Accounts(store, clock::get);
        a.bootstrap();
        a.create(null, "dana", "dana-password", List.of("admin"), Set.of(), true);
        return a;
    }

    @Test @DisplayName("an entry is served from memory until it is older than the TTL, then read again")
    void ttl() {
        Flaky store = new Flaky();
        AuthCache cache = new AuthCache(accounts(store), 30_000, clock::get);
        cache.account("dana").orElseThrow();
        int reads = store.reads;
        cache.account("dana").orElseThrow();
        assertEquals(reads, store.reads, "a hit reads nothing");
        clock.addAndGet(30_000);
        cache.account("dana").orElseThrow();
        assertEquals(reads + 1, store.reads, "an expired entry is read from the auth primary");
    }

    @Test @DisplayName("a change made through any node is dropped from this one at its next poll")
    void auditPollDrops() {
        Flaky store = new Flaky();
        Accounts a = accounts(store);
        AuthCache cache = new AuthCache(a, 30_000, clock::get);
        cache.poll();
        String token = a.openSession("dana", 60_000);
        assertTrue(cache.session(Accounts.tokenHash(token)).isPresent());
        assertEquals(Set.of("*"), cache.account("dana").orElseThrow().permissions());

        a.setRoles(null, "dana", List.of("viewer"), true);
        assertEquals(Set.of("*"), cache.account("dana").orElseThrow().permissions(), "until the poll, the cache holds");
        cache.poll();
        assertEquals(Set.of("portal.read"), cache.account("dana").orElseThrow().permissions());

        a.closeSession(token);
        cache.poll();
        assertTrue(cache.session(Accounts.tokenHash(token)).isEmpty(), "a closed session is gone everywhere");

        a.delete(null, "dana", true);
        cache.poll();
        assertTrue(cache.account("dana").isEmpty());
        assertFalse(cache.anyAccount());
    }

    @Test @DisplayName("with the auth shard down, what is cached keeps working and a miss fails")
    void outage() {
        Flaky store = new Flaky();
        Accounts a = accounts(store);
        AuthCache cache = new AuthCache(a, 30_000, clock::get);
        String token = a.openSession("dana", 60_000);
        cache.session(Accounts.tokenHash(token)).orElseThrow();
        cache.account("dana").orElseThrow();

        store.down = true;
        clock.addAndGet(60_000);
        assertTrue(cache.account("dana").isPresent(), "a stale entry is served while the shard is down");
        assertTrue(cache.session(Accounts.tokenHash(token)).isPresent());
        assertThrows(StorageException.class, () -> cache.account("rey"), "a miss cannot be answered");
        assertThrows(StorageException.class, () -> a.openSession("dana", 60_000), "and nobody new signs in");
    }
}
