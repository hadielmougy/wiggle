package com.wiggle.agents.claude;

import com.wiggle.agents.AgentsDemo;

/**
 * Runs the {@link com.wiggle.agents.ResearchTeam} with Claude behind every agent.
 *
 * <pre>ANTHROPIC_API_KEY=... ./gradlew :example-claude:runAgents --args="vector databases"</pre>
 */
public final class ClaudeAgentsDemo {

    public static void main(String[] args) throws Exception {
        AgentsDemo.run(ClaudeModel.fromEnvironment(),
                args.length > 0 ? String.join(" ", args) : "vector databases");
    }
}
