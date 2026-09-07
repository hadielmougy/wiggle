package com.wiggle.server.http;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * A minimal, dependency-free HTTP endpoint for Kubernetes liveness/readiness probes:
 * {@code GET /healthz -> 200 "ok"}, always open. The full ops dashboard now lives in the standalone
 * {@code console} process; a cell keeps only this probe so an orchestrator can health-check the node.
 */
public final class HealthServer implements AutoCloseable {

    private final HttpServer http;

    public HealthServer(int port) throws IOException {
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        this.http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.http.createContext("/healthz", ex -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) {
                os.write(body);
            }
        });
    }

    public HealthServer start() {
        http.start();
        return this;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override public void close() {
        http.stop(0);
    }
}
