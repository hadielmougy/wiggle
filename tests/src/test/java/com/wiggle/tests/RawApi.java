package com.wiggle.tests;

import com.wiggle.core.Json;
import com.wiggle.core.wire.ControlPlane;
import com.wiggle.core.wire.Msg;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/** Calls a server's API directly, below {@code WiggleClient}, to see the status and error it answers. */
public final class RawApi {
    private RawApi() {}

    /** An answer: the HTTP status, the error code and message when it failed, and the raw body. */
    public record Answer(int status, String code, String message, String body) {
        public Msg msg() { return Msg.parse(body); }
    }

    /** POSTs {@code request} to {@code method} on {@code target}, presenting {@code bearer} when set. */
    public static Answer call(String target, ControlPlane.Method method, Msg request, String bearer) throws Exception {
        return post(target, method, request.toString(), bearer);
    }

    /** POSTs {@code json} as it is to {@code method} on {@code target}. */
    public static Answer callJson(String target, ControlPlane.Method method, String json) throws Exception {
        return post(target, method, json, null);
    }

    private static Answer post(String target, ControlPlane.Method method, String body, String bearer) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://" + target + method.path()))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", ControlPlane.JSON)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (bearer != null) b.header("Authorization", "Bearer " + bearer);
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() == 200) return new Answer(200, null, null, resp.body());
            Map<String, Object> err = Json.asObject(Json.parse(resp.body()));
            return new Answer(resp.statusCode(), (String) err.get("code"), (String) err.get("message"), resp.body());
        }
    }
}
