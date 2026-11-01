package com.wiggle.server.api;

import com.wiggle.core.Json;
import com.wiggle.core.Tls;
import com.wiggle.core.wire.ControlPlane;
import com.wiggle.core.wire.Msg;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.ShardRetiredException;
import com.wiggle.server.store.StorageException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Serves the {@link ControlPlaneApi} on embedded Tomcat: {@code POST /v1/<Method>} with a JSON body,
 * as {@link ControlPlane} describes. HTTPS when a keystore is configured, requiring client
 * certificates when a truststore is too. Every call runs on its own virtual thread, so a long poll
 * holds no platform thread.
 */
public final class HttpApi implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(HttpApi.class.getName());
    /** The largest request body served. */
    static final int MAX_BODY_BYTES = 4 * 1024 * 1024;

    private final Tomcat tomcat;
    private final ExecutorService pool;
    private final ControlPlaneApi api;
    private final Authorizer authz;
    private final MemoryGuard memory;

    public HttpApi(WorkflowEngine engine, ClusterManager cluster, int port, long maxLongPollMillis)
            throws IOException {
        this(engine, cluster, port, maxLongPollMillis, Tls.Options.DISABLED, ServerConfig.Memory.DISABLED, Authorizer.OFF,
                null);
    }

    public HttpApi(WorkflowEngine engine, ClusterManager cluster, int port, long maxLongPollMillis, Tls.Options tls,
                   ServerConfig.Memory memoryConfig, Authorizer authz, com.wiggle.server.search.Search search)
            throws IOException {
        this.authz = authz;
        this.memory = new MemoryGuard(memoryConfig);
        this.api = new ControlPlaneApi(engine, cluster, maxLongPollMillis, memory, authz, search);
        this.pool = Executors.newVirtualThreadPerTaskExecutor();

        this.tomcat = new Tomcat();
        this.tomcat.setBaseDir(Files.createTempDirectory("wiggle-api").toString());
        Connector connector = new Connector("HTTP/1.1");
        connector.setPort(port);
        connector.getProtocolHandler().setExecutor(pool);
        if (tls.hasKeyStore()) {
            configureTls(connector, tls);
        } else {
            LOG.log(System.Logger.Level.WARNING, "API is PLAINTEXT; set WIGGLE_TLS_KEYSTORE to enable TLS");
        }
        tomcat.setConnector(connector);

        Context ctx = tomcat.addContext("", null);
        Tomcat.addServlet(ctx, "api", new ApiServlet());
        ctx.addServletMappingDecoded(ControlPlane.PREFIX + "*", "api");
    }

    private static void configureTls(Connector connector, Tls.Options tls) {
        connector.setSecure(true);
        connector.setScheme("https");
        connector.setProperty("SSLEnabled", "true");
        SSLHostConfig ssl = new SSLHostConfig();
        SSLHostConfigCertificate cert = new SSLHostConfigCertificate(ssl, SSLHostConfigCertificate.Type.UNDEFINED);
        cert.setCertificateKeystoreFile(tls.keyStorePath());
        if (tls.keyStorePassword() != null) cert.setCertificateKeystorePassword(tls.keyStorePassword());
        ssl.addCertificate(cert);
        if (tls.hasTrustStore()) {
            ssl.setTruststoreFile(tls.trustStorePath());
            if (tls.trustStorePassword() != null) ssl.setTruststorePassword(tls.trustStorePassword());
            ssl.setCertificateVerification("required");
            LOG.log(System.Logger.Level.INFO, "API TLS enabled with required client certificates (mTLS)");
        } else {
            LOG.log(System.Logger.Level.INFO, "API TLS enabled (server-side)");
        }
        connector.addSslHostConfig(ssl);
    }

    public void start() {
        try {
            tomcat.start();
        } catch (LifecycleException e) {
            throw new IllegalStateException("failed to start the API on Tomcat: " + e.getMessage(), e);
        }
        LOG.log(System.Logger.Level.DEBUG, () -> "API listening on port " + port());
    }

    public int port() {
        return tomcat.getConnector().getLocalPort();
    }

    @Override public void close() {
        pool.shutdownNow();
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch (LifecycleException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "API stop: " + e.getMessage());
        }
    }

    private final class ApiServlet extends HttpServlet {
        private static final long serialVersionUID = 1L;

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
