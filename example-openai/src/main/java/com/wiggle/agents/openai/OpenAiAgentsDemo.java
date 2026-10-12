package com.wiggle.agents.openai;

import com.wiggle.agents.AgentsDemo;

/**
 * Runs the {@link com.wiggle.agents.ResearchTeam} with an OpenAI model behind every agent.
 *
 * <pre>OPENAI_API_KEY=... ./gradlew :example-openai:runAgents --args="vector databases"</pre>
 */
public final class OpenAiAgentsDemo {

    public static void main(String[] args) throws Exception {
        AgentsDemo.run(OpenAiModel.fromEnvironment(),
                args.length > 0 ? String.join(" ", args) : "vector databases");
    }
}
