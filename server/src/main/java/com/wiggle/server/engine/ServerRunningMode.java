package com.wiggle.server.engine;

/**
 * Every step is dispatched by the server and reported one at a time: one worker-reported
 * completion per task, each its own transaction and its own instance lock. A worker in this mode
 * never chains, so the advance-shaped call is only reachable when a client reports a run against
 * a SERVER definition; it applies the steps exactly as reported.
 */
public class ServerRunningMode extends BaseRunningMode {

    ServerRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
        super(instances, nodeBehaviourFactory, definitions);
    }
}
