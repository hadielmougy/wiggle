package com.wiggle.server.api;

import com.wiggle.core.Json;
import com.wiggle.core.wire.ControlPlane;
import com.wiggle.core.wire.Msg;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.store.ShardRetiredException;
import com.wiggle.server.store.StorageException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The {@link ControlPlaneApi} as a servlet: {@code POST /v1/<Method>} with a JSON body, as
 * {@link ControlPlane} describes, mapped to {@code ControlPlane.PREFIX + "*"}.
 */
public final class ApiServlet extends HttpServlet {

    private static final System.Logger LOG = System.getLogger(ApiServlet.class.getName());
    private static final long serialVersionUID = 1L;
    /** The largest request body served. */
    public static final int MAX_BODY_BYTES = 4 * 1024 * 1024;

    private final transient ControlPlaneApi api;
    private final transient Authorizer authz;
    private final transient MemoryGuard memory;

    public ApiServlet(ControlPlaneApi api, Authorizer authz, MemoryGuard memory) {
        this.api = api;
        this.authz = authz;
        this.memory = memory;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String path = req.getRequestURI();
        ControlPlane.Method method = ControlPlane.BY_NAME.get(path.substring(ControlPlane.PREFIX.length()));
        if (method == null) {
            error(resp, 404, "NOT_FOUND", "no such method: " + path);
            return;
        }
        if (!"POST".equals(req.getMethod())) {
            resp.setHeader("Allow", "POST");
            error(resp, 405, "METHOD_NOT_ALLOWED", "use POST");
            return;
        }
        long held = 0;
        try {
            byte[] body;
            try (InputStream in = req.getInputStream()) {
                body = in.readNBytes(MAX_BODY_BYTES + 1);
            }
            if (body.length > MAX_BODY_BYTES) {
                error(resp, 413, "RESOURCE_EXHAUSTED", "request larger than " + MAX_BODY_BYTES + " bytes");
                return;
            }
            held += body.length;
            memory.add(body.length);
            Msg request;
            try {
                request = Msg.parse(new String(body, StandardCharsets.UTF_8));
            } catch (IllegalArgumentException e) {
                error(resp, 400, "INVALID_ARGUMENT", "malformed request: " + e.getMessage());
                return;
            }
            Msg response;
            try {
                response = authz.as(method.name(), req.getHeader("Authorization"), subject(req),
                        () -> api.call(method, request));
            } catch (Exception e) {
                fail(resp, e);
                return;
            }
            byte[] out = response.toString().getBytes(StandardCharsets.UTF_8);
            held += out.length;
            memory.add(out.length);
            send(resp, 200, out);
        } finally {
            memory.release(held);
        }
    }

    /** The RFC 2253 subject of the client certificate the request came with, or null without one. */
    private static String subject(HttpServletRequest req) {
        if (req.getAttribute("jakarta.servlet.request.X509Certificate") instanceof X509Certificate[] chain
                && chain.length > 0) {
            return chain[0].getSubjectX500Principal().getName();
        }
        return null;
    }

    /** Answers a call that threw, with the status its exception stands for. */
    private static void fail(HttpServletResponse resp, Exception e) throws IOException {
        switch (e) {
            case Authorizer.UnauthenticatedException u -> {
                LOG.log(System.Logger.Level.DEBUG, () -> "call refused: " + u.getMessage());
                resp.setHeader("WWW-Authenticate", "Bearer");
                error(resp, 401, "UNAUTHENTICATED", u.getMessage());
            }
            case Authorizer.PermissionDeniedException d -> {
                LOG.log(System.Logger.Level.DEBUG, () -> "call refused: " + d.getMessage());
                error(resp, 403, "PERMISSION_DENIED", d.getMessage());
            }
            case EngineException x -> {
                LOG.log(System.Logger.Level.DEBUG, () -> "call failed with " + x.statusCode() + ": " + x.getMessage());
                switch (x.statusCode()) {
                    case 400 -> error(resp, 400, "INVALID_ARGUMENT", x.getMessage());
                    case 404 -> error(resp, 404, "NOT_FOUND", x.getMessage());
                    case 409 -> error(resp, 409, "FAILED_PRECONDITION", x.getMessage());
                    default -> internal(resp, x);
                }
            }
            case IllegalArgumentException x -> {
                LOG.log(System.Logger.Level.DEBUG, () -> "call failed with bad request: " + x.getMessage());
                error(resp, 400, "INVALID_ARGUMENT", String.valueOf(x.getMessage()));
            }
            case ShardRetiredException x -> error(resp, 404, "NOT_FOUND", x.getMessage());
            case StorageException x when x.repeatable() -> {
                // A store failure that applied nothing: the call did not take effect, which is exactly
                // the client's retry condition, so a database blip costs a caller a pause rather than
                // a failed operation. An unknown commit is not that, and takes the internal path.
                String errorId = UUID.randomUUID().toString().substring(0, 8);
                LOG.log(System.Logger.Level.WARNING, "storage unavailable [" + errorId + "]", x);
                error(resp, 503, "UNAVAILABLE", "storage temporarily unavailable (ref " + errorId + ")");
            }
            default -> internal(resp, e);
        }
    }

    /**
     * An engine bug, or an infrastructure failure whose effect is not known (a commit that may have
     * landed). The full detail is logged server-side and only a generic message goes back: the
     * exception can leak internals (SQL text, table/constraint names, driver codes, host/schema), and
     * an unknown commit must never be reported as a retryable "nothing happened". A short correlation
     * id ties the client's error to this log line.
     */
    private static void internal(HttpServletResponse resp, Exception e) throws IOException {
        String errorId = UUID.randomUUID().toString().substring(0, 8);
        LOG.log(System.Logger.Level.ERROR, "unhandled error [" + errorId + "]", e);
        error(resp, 500, "INTERNAL", "internal error (ref " + errorId + ")");
    }

    private static void error(HttpServletResponse resp, int status, String code, String message) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", code);
        body.put("message", message);
        send(resp, status, Json.write(body).getBytes(StandardCharsets.UTF_8));
    }

    private static void send(HttpServletResponse resp, int status, byte[] body) throws IOException {
        resp.setStatus(status);
        resp.setContentType(ControlPlane.JSON);
        resp.setContentLength(body.length);
        try (OutputStream os = resp.getOutputStream()) {
            os.write(body);
        }
    }
}
