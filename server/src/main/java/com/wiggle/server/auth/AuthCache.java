package com.wiggle.server.auth;

import com.wiggle.server.store.Rows;
import com.wiggle.server.store.StorageException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * This node's copy of the accounts and sessions it has looked up. A miss, and an entry older than
 * {@code ttlMillis}, is read from the auth primary. When that read fails, an entry already held is
 * served as it is, so signed-in people keep working while the auth shard is down; with nothing
 * held, the failure propagates.
 *
 * <p>{@link #poll} reads the audit entries appended since the last poll and drops what they name,
 * so a password change, a role change or a deletion takes effect on every node within one poll.
 */
public final class AuthCache implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(AuthCache.class.getName());
    private static final int POLL_BATCH = 500;
    private static final String ANY = "";

    private record Entry<T>(T value, long loadedAt) { }

    private final Accounts accounts;
    private final long ttlMillis;
    private final LongSupplier clock;
    private final Map<String, Entry<Optional<Accounts.Account>>> users = new ConcurrentHashMap<>();
    private final Map<String, Entry<Optional<Rows.AuthSession>>> sessions = new ConcurrentHashMap<>();
    private final Map<String, Entry<Boolean>> anyAccount = new ConcurrentHashMap<>();
    private final Map<String, Entry<Optional<Accounts.Machine>>> keys = new ConcurrentHashMap<>();
    private final Map<String, Entry<Optional<Accounts.Machine>>> subjects = new ConcurrentHashMap<>();
    private volatile long seenSeq = -1;
    private ScheduledExecutorService poller;

    public AuthCache(Accounts accounts, long ttlMillis, LongSupplier clock) {
        if (ttlMillis < 0) throw new IllegalArgumentException("ttlMillis is not negative: " + ttlMillis);
        this.accounts = accounts;
        this.ttlMillis = ttlMillis;
        this.clock = clock;
    }

    /** Polls the audit every {@code intervalMillis} on a daemon thread until {@link #close}; a second call does nothing. */
    public synchronized AuthCache start(long intervalMillis) {
        if (poller != null) return this;
        poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wiggle-auth-cache");
            t.setDaemon(true);
            return t;
        });
        poller.scheduleWithFixedDelay(this::pollQuietly, 0, intervalMillis, TimeUnit.MILLISECONDS);
        return this;
    }

    public Optional<Accounts.Account> account(String name) {
        return cached(users, name, () -> accounts.account(name));
    }

    /** The session stored under {@code idHash}, if any; expiry is the caller's to check. */
    public Optional<Rows.AuthSession> session(String idHash) {
        return cached(sessions, idHash, () -> accounts.session(idHash));
    }

    /** The credential an API key belongs to, if any; expiry is the caller's to check. */
    public Optional<Accounts.Machine> machineByKey(String key) {
        String hash = Accounts.tokenHash(key);
        return cached(keys, hash, () -> accounts.machineByKeyHash(hash));
    }

    /** The credential for a client certificate subject, if any; expiry is the caller's to check. */
    public Optional<Accounts.Machine> machineBySubject(String subject) {
        return cached(subjects, subject, () -> accounts.machineBySubject(subject));
    }

    /** Whether any account exists. */
    public boolean anyAccount() {
        return cached(anyAccount, ANY, () -> !accounts.isEmpty());
    }

    /** Drops what this node holds about {@code user}: the account and its sessions. */
    public void forgetUser(String user) {
        users.remove(user);
        sessions.values().removeIf(e -> e.value().map(s -> s.user().equals(user)).orElse(false));
        anyAccount.clear();
    }

    public void forgetSession(String idHash) {
        sessions.remove(idHash);
    }

    /** Reads the audit since the last poll and drops every entry it names. */
    public void poll() {
        if (seenSeq < 0) {
            seenSeq = accounts.auditHead();
            return;
        }
        List<Rows.AuthAudit> entries;
        do {
            entries = accounts.auditAfter(seenSeq, POLL_BATCH);
            for (Rows.AuthAudit e : entries) {
                if (e.action().startsWith("role.")) {
                    users.clear();
                    keys.clear();
                    subjects.clear();
                } else if (e.action().startsWith("credential.")) {
                    keys.clear();
                    subjects.clear();
                } else if (e.action().equals("session.close")) {
                    if (e.detail() != null) sessions.remove(e.detail());
                } else if (e.target() != null) {
                    forgetUser(e.target());
                }
                seenSeq = e.seq();
            }
        } while (entries.size() == POLL_BATCH);
    }

    private void pollQuietly() {
        try {
            poll();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "auth audit poll failed: " + e);
        }
    }

    private <T> T cached(Map<String, Entry<T>> map, String key, Supplier<T> load) {
        long now = clock.getAsLong();
        Entry<T> held = map.get(key);
        if (held != null && now - held.loadedAt() < ttlMillis) return held.value();
        try {
            T value = load.get();
            map.put(key, new Entry<>(value, now));
            return value;
        } catch (StorageException e) {
            if (held == null) throw e;
            LOG.log(System.Logger.Level.DEBUG, () -> "auth shard unreachable; serving a cached entry: " + e);
            return held.value();
        }
    }

    @Override public synchronized void close() {
        if (poller != null) poller.shutdownNow();
    }
}
