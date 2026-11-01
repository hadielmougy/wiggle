package com.wiggle.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.wiggle.core.Json;
import com.wiggle.core.Tls;
import com.wiggle.proto.ControlPlane;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * Calls the control plane over HTTP: {@code POST /v1/<Method>} with a protobuf body, as
 * {@link ControlPlane} describes. HTTPS when TLS is configured or required.
 */
final class HttpTransport implements AutoCloseable {

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient http;
    private final String base;
    private final String authorization;

    HttpTransport(String target, Tls.Options tls, boolean requireTls, String apiKey) {
        boolean secure = requireTls || tls.any();
        HttpClient.Builder b = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(CONNECT_TIMEOUT);
        if (secure) b.sslContext(Tls.sslContext(tls));   // no truststore: the JVM default trust
        this.http = b.build();
        this.base = (secure ? "https://" : "http://") + stripScheme(target);
        this.authorization = apiKey == null || apiKey.isBlank() ? null : "Bearer " + apiKey.trim();
    }

    private static String stripScheme(String target) {
        int i = target.indexOf("://");
        String hostPort = i < 0 ? target : target.substring(i + 3);
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    /** Calls {@code method} with {@code request}, answering its response or throwing {@link RpcException}. */
    <Q extends Message, R extends Message> R call(ControlPlane.Method<Q, R> method, Q request) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + method.path()))
                .header("Content-Type", ControlPlane.PROTOBUF)
                .header("Accept", ControlPlane.PROTOBUF)
                .POST(HttpRequest.BodyPublishers.ofByteArray(request.toByteArray()));
        if (authorization != null) b.header("Authorization", authorization);
        HttpResponse<byte[]> resp;
        try {
            resp = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (HttpTimeoutException e) {
            throw new RpcException(0, "DEADLINE_EXCEEDED", e.getMessage(), false, e);
        } catch (IOException e) {
            throw new RpcException(0, "UNAVAILABLE", "server unreachable: " + e, true, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RpcException(0, "CANCELLED", "interrupted", false, e);
        }
        if (resp.statusCode() == 200) {
            try {
                return method.response().parseFrom(resp.body());
            } catch (InvalidProtocolBufferException e) {
                throw new RpcException(500, "INTERNAL", "malformed response: " + e.getMessage(), false, e);
            }
        }
        throw failure(resp);
    }

    private static RpcException failure(HttpResponse<byte[]> resp) {
        int status = resp.statusCode();
        String code = "UNKNOWN";
        String message = "HTTP " + status;
        try {
            if (Json.parse(new String(resp.body(), StandardCharsets.UTF_8)) instanceof Map<?, ?> m) {
                if (m.get("code") instanceof String c) code = c;
                if (m.get("message") instanceof String s) message = s;
            }
        } catch (RuntimeException notJson) {
            // a proxy's error page: the status alone says what happened
        }
        return new RpcException(status, code, message, status == 502 || status == 503, null);
    }

    @Override public void close() {
        http.shutdownNow();
    }

    /**
     * A failed call. {@code status} is the HTTP status, or 0 when none arrived; {@code unavailable}
     * marks a call that almost certainly did not run, so retrying it is safe.
     */
    static final class RpcException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final int status;
        final String code;
        final boolean unavailable;

        RpcException(int status, String code, String message, boolean unavailable, Throwable cause) {
            super(message, cause);
            this.status = status;
            this.code = code;
            this.unavailable = unavailable;
        }
    }
}
