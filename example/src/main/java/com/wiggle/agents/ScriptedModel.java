package com.wiggle.agents;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A deterministic {@link Model}: each agent gets a canned reply built from its input, so the research
 * team runs offline, in tests, with a known outcome.
 *
 * <p>The critic rejects the first draft and approves the second, so one revision round always runs.
 * {@link #calls()} counts every completion, for asserting how many model calls a run made.
 */
public final class ScriptedModel implements Model {

    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public String complete(Prompt p) {
        calls.incrementAndGet();
        return switch (p.agent()) {
            case ResearchTeam.SUPERVISOR -> """
                    search: who are the main vendors of %1$s
                    numbers: what is the market size of %1$s
                    search: what are the open problems in %1$s
                    """.formatted(firstLine(p.user()));
            case ResearchTeam.RESEARCHER -> "notes on '" + p.user() + "'";
            case ResearchTeam.ANALYST -> "estimate for '" + p.user() + "': 42";
            case ResearchTeam.FACT_CHECKER -> p.user() + " (checked)";
            case ResearchTeam.WRITER -> p.user().contains("Critique:")
                    ? "revised draft" : "first draft";
            case ResearchTeam.CRITIC -> p.user().contains("revised draft")
                    ? ResearchTeam.APPROVED : "needs a summary paragraph";
            default -> throw new IllegalArgumentException("no script for agent " + p.agent());
        };
    }

    public int calls() { return calls.get(); }

    private static String firstLine(String s) {
        int nl = s.indexOf('\n');
        return nl < 0 ? s : s.substring(0, nl);
    }
}
