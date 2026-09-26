package com.wiggle.tests;

import com.wiggle.core.NodeKind;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.engine.WorkflowEngine;

import java.util.List;

/**
 * One finished step, reported the way a worker reports it, for cases that drive the engine by
 * hand. The run is final: a case stepping the engine itself never takes a continuation.
 */
public final class Reports {

    private Reports() {}

    public static void one(WorkflowEngine engine, TaskActivation task, Object result) {
        one(engine, task, task.leaseOwner(), result);
    }

    /** {@code result} is the step's complete next context, or the branch when the node is a predicate. */
    public static void one(WorkflowEngine engine, TaskActivation task, String leaseOwner, Object result) {
        boolean predicate = task.kind() == NodeKind.PREDICATE;
        WorkflowEngine.StepInput step = new WorkflowEngine.StepInput(task.nodeId(),
                predicate ? null : result, predicate ? (Boolean) result : null);
        engine.report(new WorkflowEngine.Run(task.taskId(), leaseOwner, List.of(step), true));
    }
}
