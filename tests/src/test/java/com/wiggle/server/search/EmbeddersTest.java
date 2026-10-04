package com.wiggle.server.search;

import com.sun.net.httpserver.HttpServer;
import com.wiggle.core.Json;
import com.wiggle.server.store.Vectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The embedders that ship, and how the environment picks them. */
class EmbeddersTest {

    @Test @DisplayName("the HTTP embedder speaks the OpenAI-compatible API: model, inputs, key, vectors back in input order")
    void http() throws Exception {
        AtomicReference<String> auth = new AtomicReference<>();
        AtomicReference<Map<String, Object>> sent = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/embeddings", ex -> {
            auth.set(ex.getRequestHeaders().getFirst("Authorization"));
            Map<String, Object> body = Json.asObject(Json.parse(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            sent.set(body);
            List<Object> data = new ArrayList<>();
            List<Object> inputs = Json.asArray(body.get("input"));
            for (int i = inputs.size() - 1; i >= 0; i--) {   // out of order, as some servers answer
                data.add(Map.of("index", i, "embedding", List.of(i, i + 0.5, -1)));
            }
            byte[] out = Json.write(Map.of("data", data)).getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, out.length);
            ex.getResponseBody().write(out);
            ex.close();
        });
        server.start();
        try {
            String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/";
            HttpEmbedder e = new HttpEmbedder(base, "text-embedding-3-small", 3, "sk-test");
            List<float[]> v = e.embed(List.of("first", "second"));
            assertEquals("Bearer sk-test", auth.get());
            assertEquals("text-embedding-3-small", sent.get().get("model"));
            assertEquals(List.of("first", "second"), sent.get().get("input"));
            assertArrayEquals(new float[] {0, 0.5f, -1}, v.get(0));
            assertArrayEquals(new float[] {1, 1.5f, -1}, v.get(1));

            HttpEmbedder wrongSize = new HttpEmbedder(base, "m", 4, null);
            IllegalStateException bad = assertThrows(IllegalStateException.class, () -> wrongSize.embed(List.of("x")));
            assertTrue(bad.getMessage().contains("3 dimensions"), bad.getMessage());
        } finally {
            server.stop(0);
        }
        HttpEmbedder down = new HttpEmbedder("http://127.0.0.1:1/v1", "m", 3, null);
        assertThrows(IllegalStateException.class, () -> down.embed(List.of("x")), "an unreachable service is a failure, retried later");
    }

    @Test @DisplayName("the hashing embedder is unit length, and texts sharing words point the same way")
    void hashing() {
        HashingEmbedder e = new HashingEmbedder(128);
        List<float[]> v = e.embed(List.of("damaged parcel refund", "refund damaged parcel please", "invoice paid", ""));
        assertEquals(1.0, Vectors.cosine(v.get(0), v.get(0)), 1e-6);
        assertTrue(Vectors.cosine(v.get(0), v.get(1)) > 0.8);
        assertTrue(Vectors.cosine(v.get(0), v.get(2)) < 0.3);
        assertEquals(0.0, Vectors.cosine(v.get(0), v.get(3)), "an empty text is the zero vector");
        assertEquals("hashing-128", e.model());
    }

    @Test @DisplayName("the environment picks none, hashing, or HTTP with the model it replaces")
    void fromEnvironment() {
        assertEquals(List.of(), Embedders.fromEnvironment(Map.of()));
        assertEquals("hashing-256", Embedders.fromEnvironment(Map.of("WIGGLE_EMBEDDER", "hashing")).getFirst().model());
        List<Embedder> http = Embedders.fromEnvironment(Map.of("WIGGLE_EMBEDDER", "http",
                "WIGGLE_EMBEDDER_URL", "http://localhost:11434/v1", "WIGGLE_EMBEDDER_MODEL", "nomic-embed-text",
                "WIGGLE_EMBEDDER_DIMENSION", "768", "WIGGLE_EMBEDDER_PREVIOUS_MODEL", "all-minilm",
                "WIGGLE_EMBEDDER_PREVIOUS_DIMENSION", "384"));
        assertEquals(List.of("nomic-embed-text", "all-minilm"), http.stream().map(Embedder::model).toList());
        assertEquals(List.of(768, 384), http.stream().map(Embedder::dimension).toList());
        assertThrows(IllegalArgumentException.class, () -> Embedders.fromEnvironment(Map.of("WIGGLE_EMBEDDER", "http",
                "WIGGLE_EMBEDDER_URL", "http://x/v1", "WIGGLE_EMBEDDER_MODEL", "m")), "the dimension is required");
        assertThrows(IllegalArgumentException.class, () -> Embedders.fromEnvironment(Map.of("WIGGLE_EMBEDDER", "magic")));
    }

    @Test @DisplayName("vectors survive their stored forms")
    void storedForms() {
        float[] v = {0.25f, -1.5f, 3e-7f};
        assertArrayEquals(v, Vectors.decode(Vectors.encode(v)));
        assertArrayEquals(v, Vectors.parseLiteral(Vectors.literal(v)));
    }
}
