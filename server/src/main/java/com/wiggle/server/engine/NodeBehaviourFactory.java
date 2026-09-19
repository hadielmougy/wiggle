package com.wiggle.server.engine;

import com.wiggle.core.NodeKind;

import java.util.HashMap;
import java.util.Map;

final class NodeBehaviourFactory {

    private final Map<NodeKind, NodeBehaviour> nodeBehaviourMap = new HashMap<>();

    NodeBehaviourFactory(InstanceLifecycle instanceLifecycle, TokenLifecycle tokenLifecycle) {
        nodeBehaviourMap.put(NodeKind.TASK,         new NodeBehaviour.TaskNodeBehaviour(tokenLifecycle));
        nodeBehaviourMap.put(NodeKind.PREDICATE,    new NodeBehaviour.PredicateNodeBehaviour(tokenLifecycle));
        nodeBehaviourMap.put(NodeKind.SLEEP,        new NodeBehaviour.SleepNodeBehaviour());
        nodeBehaviourMap.put(NodeKind.FORK,         new NodeBehaviour.ForkNodeBehaviour());
        nodeBehaviourMap.put(NodeKind.DYN_FORK ,    new NodeBehaviour.DynForkNodeBehaviour(instanceLifecycle));
        nodeBehaviourMap.put(NodeKind.JOIN,         new NodeBehaviour.JoinNodeBehaviour());
        nodeBehaviourMap.put(NodeKind.SIGNAL,       new NodeBehaviour.SignalNodeBehaviour());
        nodeBehaviourMap.put(NodeKind.SUB_WORKFLOW, new NodeBehaviour.SubflowNodeBehaviour(instanceLifecycle));
        nodeBehaviourMap.put(NodeKind.END,          new NodeBehaviour.EndNodeBehaviour(instanceLifecycle));
        requireEveryKindRegistered();
    }

    private void requireEveryKindRegistered() {
        for (NodeKind kind : NodeKind.values()) getNodeBehaviour(kind);
    }

    public NodeBehaviour getNodeBehaviour(NodeKind kind) {
        NodeBehaviour behaviour = nodeBehaviourMap.get(kind);
        if (behaviour == null) {
            throw new IllegalArgumentException("No NodeBehaviour registered for NodeKind: " + kind);
        }
        return behaviour;
    }
}
