package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;

/**
 * Every step is dispatched by the server and reported one at a time: one {@code complete} per
 * task, each its own transaction and its own instance lock. A worker in this mode never chains,
 * so {@code advance} is only reachable when a client reports a run against a SERVER definition;
 * it applies the steps exactly as reported.
 */
public class ServerRunningMode extends BaseRunningMode {

    ServerRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
        super(instances, nodeBehaviourFactory, definitions);
    }

    @Override
    public void complete(CompleteRunContext ctx) {
        completeStep(ctx);
    }

    @Override
    public AdvanceOutcome advance(AdvanceRunContext ctx) {
        return chainSteps(ctx);
    }
}
