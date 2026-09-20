package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.AdvanceOutcome;


public class LocalAsyncRunningMode extends BaseRunningMode {

    LocalAsyncRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
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
