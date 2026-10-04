package com.wiggle.server.grpc;

import com.wiggle.server.ServerConfig.GrpcAuth;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.auth.Permissions;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Who a gRPC call is, and whether it may do what it asks. A caller presents an API key as
 * {@code authorization: Bearer <key>}, or its client certificate when the server requires mTLS; a
 * key wins when both are present. Either is resolved through the node's {@link AuthCache}, so a
 * call reads the auth shard only on a cache miss.
 *
 * <p>In {@link GrpcAuth#OFF} nothing is read and every check passes. In {@link GrpcAuth#LOG} every
 * call is served, and each distinct refusal enforcement would make is logged once. In
 * {@link GrpcAuth#ENFORCE} a call with no known credential is {@code UNAUTHENTICATED} and one its
 * role does not allow is {@code PERMISSION_DENIED}. {@code HealthCheck} needs no credential.
 */
public final class Authorizer implements ServerInterceptor {

    private static final System.Logger LOG = System.getLogger(Authorizer.class.getName());
    static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final Context.Key<Caller> CALLER = Context.key("wiggle-caller");
    private static final String OPEN_METHOD = "com.wiggle.proto.WiggleControlPlane/HealthCheck";

    /** A call's credential and what its role grants. {@code id} is null when none was recognised. */
    record Caller(String id, Set<String> permissions) {
        static final Caller NONE = new Caller(null, Set.of());
    }

    /** Checks nothing. */
    public static final Authorizer OFF = new Authorizer(GrpcAuth.OFF, null);

    private final GrpcAuth mode;
    private final AuthCache cache;
    /** Refusals already logged in LOG mode, so a busy worker does not repeat one per call. */
    private final Set<String> logged = ConcurrentHashMap.newKeySet();

    public Authorizer(GrpcAuth mode, AuthCache cache) {
        if (mode != GrpcAuth.OFF && cache == null) throw new IllegalArgumentException("checking calls needs an AuthCache");
        this.mode = mode;
        this.cache = cache;
    }

    GrpcAuth mode() { return mode; }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        if (mode == GrpcAuth.OFF || OPEN_METHOD.equals(call.getMethodDescriptor().getFullMethodName())) {
            return next.startCall(call, headers);
        }
        Caller caller;
        try {
            caller = identify(call, headers);
        } catch (com.wiggle.server.store.StorageException e) {
            call.close(Status.UNAVAILABLE.withDescription("the auth shard is unreachable; try again shortly"), new Metadata());
            return new ServerCall.Listener<>() { };
        } catch (Refused e) {
            if (mode == GrpcAuth.ENFORCE) {
                call.close(Status.UNAUTHENTICATED.withDescription(e.getMessage()), new Metadata());
                return new ServerCall.Listener<>() { };
            }
            logOnce("unauthenticated " + call.getMethodDescriptor().getBareMethodName() + ": " + e.getMessage());
            caller = Caller.NONE;
        }
        return Contexts.interceptCall(Context.current().withValue(CALLER, caller), call, headers, next);
    }

    private Caller identify(ServerCall<?, ?> call, Metadata headers) {
        long now = System.currentTimeMillis();
        String header = headers.get(AUTHORIZATION);
        if (header != null) {
            if (!header.regionMatches(true, 0, "Bearer ", 0, 7)) throw new Refused("the authorization header is not a Bearer key");
            Accounts.Machine m = cache.machineByKey(header.substring(7).trim())
                    .orElseThrow(() -> new Refused("unknown API key"));
            if (m.expired(now)) throw new Refused("API key '" + m.id() + "' has expired");
            return new Caller(m.id(), m.permissions());
        }
        String subject = subject(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION));
        if (subject == null) throw new Refused("no credential: send an API key or a client certificate");
        Accounts.Machine m = cache.machineBySubject(subject)
                .orElseThrow(() -> new Refused("no credential for certificate subject '" + subject + "'"));
        if (m.expired(now)) throw new Refused("certificate credential '" + m.id() + "' has expired");
        return new Caller(m.id(), m.permissions());
    }

    /** The RFC 2253 subject of the peer's certificate, or null without one. */
    static String subject(SSLSession session) {
        if (session == null) return null;
        try {
            Certificate[] chain = session.getPeerCertificates();
            if (chain.length == 0 || !(chain[0] instanceof X509Certificate x)) return null;
            return x.getSubjectX500Principal().getName();
        } catch (SSLPeerUnverifiedException e) {
            return null;
        }
    }

    /** Refuses the current call unless its caller may do {@code action} on {@code scope} (null: unscoped). */
    void require(String action, String scope) {
        if (mode == GrpcAuth.OFF) return;
        Caller c = caller();
        if (Permissions.allows(c.permissions(), action, scope)) return;
        refuse(c, action + (scope == null ? "" : ":" + scope));
    }

    /** Refuses the current call unless its caller may do {@code action} on at least one scope. */
    void requireAny(String action) {
        if (mode == GrpcAuth.OFF) return;
        Caller c = caller();
        Set<String> p = c.permissions();
        if (p.contains(Permissions.ALL) || p.contains(action)) return;
        if (p.stream().anyMatch(x -> x.startsWith(action + ":"))) return;
        refuse(c, action);
    }

    private static Caller caller() {
        Caller c = CALLER.get();
        return c == null ? Caller.NONE : c;
    }

    private void refuse(Caller c, String needed) {
        String who = c.id() == null ? "an unauthenticated caller" : "credential '" + c.id() + "'";
        if (mode == GrpcAuth.ENFORCE) throw new PermissionDeniedException(who + " lacks permission '" + needed + "'");
        logOnce(who + " would be refused: it lacks permission '" + needed + "'");
    }

    private void logOnce(String message) {
        if (logged.size() < 10_000 && logged.add(message)) {
            LOG.log(System.Logger.Level.WARNING, () -> "gRPC auth (log mode): " + message);
        }
    }

    private static final class Refused extends RuntimeException {
        Refused(String message) { super(message, null, false, false); }
    }

    /** A call its caller is not allowed to make; the API answers PERMISSION_DENIED. */
    static final class PermissionDeniedException extends RuntimeException {
        PermissionDeniedException(String message) { super(message, null, false, false); }
    }
}
