package com.wiggle.server.engine;

/**
 * The worker runs a chain of steps locally and reports each one as it finishes, so a run arrives
 * a step at a time and the continuation is leased straight back. Nothing accumulates on the
 * worker, so there is nothing to batch: the procedure is one step per call under one lock.
 */
public class LocalSyncRunningMode extends BaseRunningMode {

    LocalSyncRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
        super(instances, nodeBehaviourFactory, definitions);
    }
}
