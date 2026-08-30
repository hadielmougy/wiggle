package com.wiggle.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.wiggle.core.Json;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Small, dependency-free HTTP helpers shared by the dashboard and by {@link DashboardAuth}
 * implementations -- including ones living outside this repository. Writing JSON, text and
 * redirects, reading a JSON body, parsing the query string, and a handler wrapper that turns a
 * thrown exception into a clean 400/500 instead of a dropped connection.
 */
public final class DashboardHttp {

    private static final System.Logger LOG = System.getLogger(DashboardHttp.class.getName());

    /** Content-Type for an HTML page. */
    public static final String HTML = "text/html; charset=utf-8";
    /** Content-Type for a JSON response. */
    public static final String JSON = "application/json; charset=utf-8";

    private DashboardHttp() {}

    /** A request handler that may throw; see {@link #guarded}. */
    @FunctionalInterface
    public interface ThrowingHandler { void handle(HttpExchange ex) throws IOException; }

    /** Wraps a handler so an {@link IllegalArgumentException} becomes 400 and any other error 500. */
    public static HttpHandler guarded(ThrowingHandler h) {
        return ex -> {
            try {
                h.handle(ex);
            } catch (IllegalArgumentException e) {
                sendError(ex, 400, e.getMessage());
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "dashboard request failed: " + e);
                sendError(ex, 500, e.getMessage());
            }
        };
    }

    public static void sendJson(HttpExchange ex, int status, Object body) throws IOException {
        sendBytes(ex, status, JSON, Json.write(body).getBytes(StandardCharsets.UTF_8));
    }

    public static void sendError(HttpExchange ex, int status, String message) throws IOException {
        sendBytes(ex, status, JSON, Json.write(Map.of("error", message == null ? "error" : message))
                .getBytes(StandardCharsets.UTF_8));
    }

    public static void sendText(HttpExchange ex, int status, String contentType, String body) throws IOException {
        sendBytes(ex, status, contentType, body.getBytes(StandardCharsets.UTF_8));
    }

    public static void sendBytes(HttpExchange ex, int status, String contentType, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    public static void redirect(HttpExchange ex, String location) throws IOException {
        ex.getResponseHeaders().set("Location", location);
        ex.sendResponseHeaders(302, -1);
        ex.close();
    }

    public static void requireGet(HttpExchange ex) {
        if (!ex.getRequestMethod().equals("GET")) throw new IllegalArgumentException("GET required");
    }

    /** Parses the request body as JSON; an empty body is an empty object. */
    public static Object readJsonBody(HttpExchange ex) throws IOException {
        byte[] raw = ex.getRequestBody().readAllBytes();
        String body = new String(raw, StandardCharsets.UTF_8).trim();
        return body.isEmpty() ? Map.of() : Json.parse(body);
    }

    public static Map<String, String> query(URI uri) {
        Map<String, String> out = new LinkedHashMap<>();
        String q = uri.getQuery();
        if (q == null) return out;
        for (String pair : q.split("&")) {
            int i = pair.indexOf('=');
            if (i < 0) out.put(pair, "");
            else out.put(pair.substring(0, i), URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8));
        }
        return out;
    }

    public static int parseInt(String s, int def) {
        try { return s == null ? def : Integer.parseInt(s.trim()); } catch (NumberFormatException e) { return def; }
    }

    public static String emptyToNull(String s) { return s == null || s.isBlank() ? null : s; }

    /** Constant-time string compare, so a match can't be timed out character by character. */
    public static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
