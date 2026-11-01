package com.wiggle.server.api;

import com.wiggle.server.ServerConfig.ApiAuth;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.auth.Permissions;
import com.wiggle.server.auth.Scope;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Who an API call is, and whether it may do what it asks. A caller presents an API key as
 * {@code Authorization: Bearer <key>}, or its client certificate when the server requires mTLS; a
 * key wins when both are present. Either is resolved through the node's {@link AuthCache}, so a
 * call reads the auth shard only on a cache miss.
 *
 * <p>In {@link ApiAuth#OFF} nothing is read and every check passes. In {@link ApiAuth#LOG} every
 * call is served, and each distinct refusal enforcement would make is logged once. In
 * {@link ApiAuth#ENFORCE} a call with no known credential is {@code UNAUTHENTICATED} and one its
 * role does not allow is {@code PERMISSION_DENIED}. {@code HealthCheck} needs no credential.
 */
public final class Authorizer {

    private static final System.Logger LOG = System.getLogger(Authorizer.class.getName());
    private static final ThreadLocal<Caller> CALLER = new ThreadLocal<>();
    private static final String OPEN_METHOD = "HealthCheck";

    /** A call's credential and what its role grants. {@code id} is null when none was recognised. */
    record Caller(String id, Set<String> permissions) {
        static final Caller NONE = new Caller(null, Set.of());
    }

    /** Checks nothing. */
    public static final Authorizer OFF = new Authorizer(ApiAuth.OFF, null);

    private final ApiAuth mode;
    private final AuthCache cache;
    /** Refusals already logged in LOG mode, so a busy worker does not repeat one per call. */
    private final Set<String> logged = ConcurrentHashMap.newKeySet();

    public Authorizer(ApiAuth mode, AuthCache cache) {
        if (mode != ApiAuth.OFF && cache == null) throw new IllegalArgumentException("checking calls needs an AuthCache");
        this.mode = mode;
        this.cache = cache;
    }

    ApiAuth mode() { return mode; }

    /**
     * Runs {@code body} as the caller presenting {@code authorization} (the header's value, or null)
     * and the client certificate whose subject is {@code certificateSubject} (null without one). Throws {@link UnauthenticatedException} when enforcing
     * and the caller is not recognised; a {@link com.wiggle.server.store.StorageException} from the
     * auth shard propagates.
     */
    <T> T as(String method, String authorization, String certificateSubject, Callable<T> body) throws Exception {
        Caller caller = Caller.NONE;
        if (mode != ApiAuth.OFF && !OPEN_METHOD.equals(method)) {
            try {
                caller = identify(authorization, certificateSubject);
            } catch (Refused e) {
                if (mode == ApiAuth.ENFORCE) throw new UnauthenticatedException(e.getMessage());
                logOnce("unauthenticated " + method + ": " + e.getMessage());
            }
        }
        CALLER.set(caller);
        try {
            return body.call();
        } finally {
            CALLER.remove();
        }
    }

    private Caller identify(String header, String subject) {
        long now = System.currentTimeMillis();
        if (header != null) {
            if (!header.regionMatches(true, 0, "Bearer ", 0, 7)) throw new Refused("the authorization header is not a Bearer key");
            Accounts.Machine m = cache.machineByKey(header.substring(7).trim())
                    .orElseThrow(() -> new Refused("unknown API key"));
            if (m.expired(now)) throw new Refused("API key '" + m.id() + "' has expired");
            return new Caller(m.id(), m.permissions());
        }
        if (subject == null) throw new Refused("no credential: send an API key or a client certificate");
        Accounts.Machine m = cache.machineBySubject(subject)
                .orElseThrow(() -> new Refused("no credential for certificate subject '" + subject + "'"));
        if (m.expired(now)) throw new Refused("certificate credential '" + m.id() + "' has expired");
        return new Caller(m.id(), m.permissions());
    }

    /** Refuses the current call unless its caller may do {@code action} on {@code scope} (null: unscoped). */
    void require(String action, String scope) {
        if (mode == ApiAuth.OFF) return;
        Caller c = caller();
        if (Permissions.allows(c.permissions(), action, scope)) return;
        refuse(c, action + (scope == null ? "" : ":" + scope));
    }

    /** Refuses the current call unless its caller may do {@code action} on at least one scope. */
    void requireAny(String action) {
        if (mode == ApiAuth.OFF) return;
        Caller c = caller();
        Set<String> p = c.permissions();
        if (p.contains(Permissions.ALL) || p.contains(action)) return;
        if (p.stream().anyMatch(x -> x.startsWith(action + ":"))) return;
        refuse(c, action);
    }

    /** The names the current caller may do {@code action} on; every name unless checks are enforced. */
    Scope scope(String action) {
        if (mode != ApiAuth.ENFORCE) return Scope.ALL;
        return Permissions.scope(caller().permissions(), action);
    }

    private static Caller caller() {
        Caller c = CALLER.get();
        return c == null ? Caller.NONE : c;
    }

    private void refuse(Caller c, String needed) {
        String who = c.id() == null ? "an unauthenticated caller" : "credential '" + c.id() + "'";
        if (mode == ApiAuth.ENFORCE) throw new PermissionDeniedException(who + " lacks permission '" + needed + "'");
        logOnce(who + " would be refused: it lacks permission '" + needed + "'");
    }

    private void logOnce(String message) {
        if (logged.size() < 10_000 && logged.add(message)) {
            LOG.log(System.Logger.Level.WARNING, () -> "API auth (log mode): " + message);
        }
    }

    private static final class Refused extends RuntimeException {
        Refused(String message) { super(message, null, false, false); }
    }

    /** A call with no credential the server recognises; the API answers UNAUTHENTICATED. */
    static final class UnauthenticatedException extends RuntimeException {
        UnauthenticatedException(String message) { super(message, null, false, false); }
    }

    /** A call its caller is not allowed to make; the API answers PERMISSION_DENIED. */
    static final class PermissionDeniedException extends RuntimeException {
        PermissionDeniedException(String message) { super(message, null, false, false); }
    }
}
