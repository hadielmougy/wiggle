package com.wiggle.agents;

/**
 * The one call an agent makes: a system prompt and a user turn in, the model's text out.
 *
 * <p>An implementation is invoked from inside a step, so it must be safe to call from several worker
 * threads at once, and it may throw: the step's {@link com.wiggle.core.RetryPolicy} decides whether
 * the call is tried again. It holds no conversation state — whatever an agent needs to remember
 * travels in the workflow's context, which is what makes a crashed worker's step re-runnable.
 */
public interface Model {

    /**
     * @param agent  the calling agent's name, for routing, logging or a scripted reply
     * @param system the agent's standing instructions
     * @param user   this turn's input
     */
    record Prompt(String agent, String system, String user) {}

    String complete(Prompt prompt);
}
