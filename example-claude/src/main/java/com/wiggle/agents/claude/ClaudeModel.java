package com.wiggle.agents.claude;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.errors.AnthropicServiceException;
import com.anthropic.models.beta.AnthropicBeta;
import com.anthropic.models.beta.messages.BetaMessage;
import com.anthropic.models.beta.messages.BetaStopReason;
import com.anthropic.models.beta.messages.MessageCreateParams;
import com.wiggle.agents.Model;
import com.wiggle.client.worker.PermanentActivityException;

import java.util.stream.Collectors;

/**
 * A {@link Model} backed by the Claude Messages API: one request per agent turn, the agent's system
 * prompt as {@code system}, its input as the single user message, the reply's text blocks joined.
 *
 * <p>A refused turn is retried on a fallback model server-side ({@code fallbacks: "default"}). The
 * SDK's own retries are off, so the step's {@link com.wiggle.core.RetryPolicy} is the only retry
 * loop: timeouts, conflicts, rate limits, overload and server errors are thrown as-is and retried by
 * the step; a refusal that survives the fallback, or any other client error, fails the instance
 * through {@link PermanentActivityException}.
 */
public final class ClaudeModel implements Model {

    public static final String DEFAULT_MODEL = "claude-opus-5-5";

    private final AnthropicClient client;
    private final String model;

    public ClaudeModel(AnthropicClient client, String model) {
        this.client = client;
        this.model = model;
    }

    /** Credentials as the SDK resolves them; the model from {@code WIGGLE_CLAUDE_MODEL}, else {@value #DEFAULT_MODEL}. */
    public static ClaudeModel fromEnvironment() {
        String model = System.getenv().getOrDefault("WIGGLE_CLAUDE_MODEL", DEFAULT_MODEL);
        return new ClaudeModel(AnthropicOkHttpClient.builder().fromEnv().maxRetries(0).build(), model);
    }

    @Override
    public String complete(Prompt p) {
        MessageCreateParams params = MessageCreateParams.builder()
                .model(model)
                .maxTokens(16000L)
                .system(p.system())
                .addUserMessage(p.user())
                .fallbacksDefault()
                .addBeta(AnthropicBeta.SERVER_SIDE_FALLBACK_2026_07_01)
                .build();
        BetaMessage reply;
        try {
            reply = client.beta().messages().create(params);
        } catch (AnthropicServiceException e) {
            int status = e.statusCode();
            if (status == 408 || status == 409 || status == 429 || status >= 500) throw e;
            throw new PermanentActivityException(p.agent() + ": " + e.getMessage(), e);
        }
        if (reply.stopReason().filter(BetaStopReason.REFUSAL::equals).isPresent()) {
            throw new PermanentActivityException(p.agent() + ": the model declined this turn");
        }
        return reply.content().stream()
                .flatMap(block -> block.text().stream())
                .map(text -> text.text())
                .collect(Collectors.joining());
    }
}
