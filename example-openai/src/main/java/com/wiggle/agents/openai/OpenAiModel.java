package com.wiggle.agents.openai;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.errors.OpenAIServiceException;
import com.openai.models.responses.Response;
import com.openai.models.responses.ResponseCreateParams;
import com.openai.models.responses.ResponseOutputMessage;
import com.wiggle.agents.Model;
import com.wiggle.client.worker.PermanentActivityException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link Model} backed by the OpenAI Responses API: one request per agent turn, the agent's system
 * prompt as {@code instructions}, its input as {@code input}, the reply's output text joined.
 *
 * <p>The SDK's own retries are off, so the step's {@link com.wiggle.core.RetryPolicy} is the only
 * retry loop: timeouts, conflicts, rate limits and server errors are thrown as-is and retried by the
 * step; a refusal, or any other client error, fails the instance through
 * {@link PermanentActivityException}.
 */
public final class OpenAiModel implements Model {

    public static final String DEFAULT_MODEL = "gpt-5.5";

    private final OpenAIClient client;
    private final String model;

    public OpenAiModel(OpenAIClient client, String model) {
        this.client = client;
        this.model = model;
    }

    /** Credentials as the SDK resolves them; the model from {@code WIGGLE_OPENAI_MODEL}, else {@value #DEFAULT_MODEL}. */
    public static OpenAiModel fromEnvironment() {
        String model = System.getenv().getOrDefault("WIGGLE_OPENAI_MODEL", DEFAULT_MODEL);
        return new OpenAiModel(OpenAIOkHttpClient.builder().fromEnv().maxRetries(0).build(), model);
    }

    // docs:begin complete
    @Override
    public String complete(Prompt p) {
        ResponseCreateParams params = ResponseCreateParams.builder()
                .model(model)
                .maxOutputTokens(16000L)
                .instructions(p.system())
                .input(p.user())
                .build();
        Response reply;
        try {
            reply = client.responses().create(params);
        } catch (OpenAIServiceException e) {
            int status = e.statusCode();
            if (status == 408 || status == 409 || status == 429 || status >= 500) throw e;
            throw new PermanentActivityException(p.agent() + ": " + e.getMessage(), e);
        }
        List<ResponseOutputMessage.Content> content = reply.output().stream()
                .flatMap(item -> item.message().stream())
                .flatMap(message -> message.content().stream())
                .toList();
        if (content.stream().anyMatch(part -> part.refusal().isPresent())) {
            throw new PermanentActivityException(p.agent() + ": the model declined this turn");
        }
        return content.stream()
                .flatMap(part -> part.outputText().stream())
                .map(text -> text.text())
                .collect(Collectors.joining());
    }
    // docs:end complete
}
