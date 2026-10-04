package com.wiggle.server.search;

import com.wiggle.core.Json;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An embedder over the OpenAI-compatible {@code POST /embeddings} API, which OpenAI, Azure OpenAI,
 * Ollama, vLLM, LiteLLM and most model servers speak. {@code baseUrl} is the API root, such as
 * {@code https://api.openai.com/v1} or {@code http://localhost:11434/v1}.
 */
public final class HttpEmbedder implements Embedder {

    private final URI endpoint;
    private final String model;
    private final int dimension;
    private final String apiKey;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    /** @param apiKey sent as a Bearer token; null sends none */
    public HttpEmbedder(String baseUrl, String model, int dimension, String apiKey) {
        if (baseUrl == null || baseUrl.isBlank()) throw new IllegalArgumentException("an embedder needs a URL");
        if (model == null || model.isBlank()) throw new IllegalArgumentException("an embedder needs a model");
        if (dimension <= 0) throw new IllegalArgumentException("dimension is positive: " + dimension);
        this.endpoint = URI.create(baseUrl.replaceAll("/+$", "") + "/embeddings");
        this.model = model;
        this.dimension = dimension;
        this.apiKey = apiKey == null || apiKey.isBlank() ? null : apiKey;
    }

    @Override public String model() {
        return model;
    }

    @Override public int dimension() {
        return dimension;
    }

    @Override public List<float[]> embed(List<String> texts) {
        if (texts.isEmpty()) return List.of();
        HttpRequest.Builder req = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(Json.write(Map.of("model", model, "input", texts))));
        if (apiKey != null) req.header("Authorization", "Bearer " + apiKey);
        HttpResponse<String> res;
        try {
            res = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("embedding service at " + endpoint + " did not answer: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while embedding", e);
        }
        if (res.statusCode() / 100 != 2) {
            String body = res.body();
            throw new IllegalStateException("embedding service answered " + res.statusCode() + ": "
                    + (body.length() > 300 ? body.substring(0, 300) + "…" : body));
        }
        List<Object> data = Json.asArray(Json.asObject(Json.parse(res.body())).get("data"));
        float[][] out = new float[texts.size()][];
        for (Object item : data) {
            Map<String, Object> m = Json.asObject(item);
            int index = (int) Json.num(m, "index", 0);
            List<Object> values = Json.asArray(m.get("embedding"));
            if (values.size() != dimension) {
                throw new IllegalStateException("model " + model + " returned " + values.size() + " dimensions, not "
                        + dimension + "; set the dimension the model produces");
            }
            float[] v = new float[dimension];
            for (int i = 0; i < dimension; i++) v[i] = ((Number) values.get(i)).floatValue();
            out[index] = v;
        }
        List<float[]> result = new ArrayList<>(texts.size());
        for (int i = 0; i < out.length; i++) {
            if (out[i] == null) throw new IllegalStateException("embedding service returned no vector for input " + i);
            result.add(out[i]);
        }
        return result;
    }
}
