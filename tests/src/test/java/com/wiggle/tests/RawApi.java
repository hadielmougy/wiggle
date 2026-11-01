package com.wiggle.tests;

import com.google.protobuf.Message;
import com.wiggle.core.Json;
import com.wiggle.proto.ControlPlane;

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
    public record Answer(int status, String code, String message, byte[] body) {}

    /** POSTs {@code request} to {@code method} on {@code target} as protobuf, presenting {@code bearer} when set. */
    public static Answer call(String target, ControlPlane.Method<?, ?> method, Message request, String bearer)
            throws Exception {
        return post(target, method, ControlPlane.PROTOBUF, request.toByteArray(), bearer);
    }

    /** POSTs a JSON body to {@code method} on {@code target}. */
    public static Answer callJson(String target, ControlPlane.Method<?, ?> method, String json) throws Exception {
        return post(target, method, ControlPlane.JSON, json.getBytes(StandardCharsets.UTF_8), null);
    }

    private static Answer post(String target, ControlPlane.Method<?, ?> method, String contentType, byte[] body,
                               String bearer) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://" + target + method.path()))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (bearer != null) b.header("Authorization", "Bearer " + bearer);
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            HttpResponse<byte[]> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 200) return new Answer(200, null, null, resp.body());
            Map<String, Object> err = Json.asObject(Json.parse(new String(resp.body(), StandardCharsets.UTF_8)));
            return new Answer(resp.statusCode(), (String) err.get("code"), (String) err.get("message"), resp.body());
        }
    }
}
