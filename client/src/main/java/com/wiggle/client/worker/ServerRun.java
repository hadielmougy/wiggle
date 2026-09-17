package com.wiggle.client.worker;

import com.wiggle.core.NodeKind;
import com.wiggle.core.TaskActivation;

import java.util.Map;

/** Server-driven execution: run one step and report via complete/fail; the server advances the token. */
final class ServerRun {

    private static final System.Logger LOG = System.getLogger(ServerRun.class.getName());

    private final Worker w;
    private final TaskActivation task;

    ServerRun(Worker w, TaskActivation task) {
        this.w = w;
        this.task = task;
    }

    void run() {
        ActivityHandler handler = w.registrations().handlerFor(task.activity());
        if (handler == null) {
            reportFailure("no handler registered for activity '" + task.activity() + "'", false);
            return;
        }
        Heartbeat lease = w.newHeartbeat(task.taskId(), task.leaseOwner());
        lease.start();
        Step.begin(new Step.Info(task.attempt(), task.stepName(), task.instanceId(),
                task.baseContext(), task.baseContext() != null, task.itemIndex(), task.itemMapKey()));
        try {
            Object result = handler.invoke(task.context());
            lease.stop();   // the handler is done: no extension may race or trail the settle below
            settle(result);
        } catch (PermanentActivityException e) {
            lease.stop();
            reportFailure(Worker.describe(e), false);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.DEBUG,
                    () -> "step " + task.stepName() + " of " + task.instanceId() + " failed: " + e);
            lease.stop();
            reportFailure(Worker.describe(e), true);
        } catch (Throwable t) {
            lease.stop();
            reportFailure(Worker.describe(t), false);
            throw t;
        } finally {
            lease.stop();
            Step.end();
        }
    }

    /** Reports a finished step: a predicate must have produced a boolean, a task merges its result. */
    private void settle(Object result) {
        if (task.kind() == NodeKind.PREDICATE && !(result instanceof Boolean)) {
            reportFailure("predicate '" + task.stepName() + "' returned " + Worker.typeName(result), false);
            return;
        }
        w.client().complete(task.taskId(), task.leaseOwner(),
                task.kind() == NodeKind.PREDICATE ? Map.of("value", result) : result);
    }

    private void reportFailure(String message, boolean retryable) {
        try {
            w.client().fail(task.taskId(), task.leaseOwner(), message, retryable);
        } catch (RuntimeException e) {
            // The lease will expire and the leader will reclaim the task; nothing else to do.
            LOG.log(System.Logger.Level.WARNING,
                    "could not report failure of task " + task.taskId() + ": " + e.getMessage());
        }
    }
}
