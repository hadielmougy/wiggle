package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient.StepReport;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TaskActivation;

import java.util.List;

/** Server-driven execution: run one step, report it, and let the server advance the token. */
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
        long startedAt = System.currentTimeMillis();
        long t0 = System.nanoTime();
        try {
            Object result = handler.invoke(task.context());
            long finishedAt = startedAt + (System.nanoTime() - t0) / 1_000_000;
            java.util.List<com.wiggle.core.EmittedEvent> emitted = Step.drainEmitted();
            lease.stop();   // the handler is done: no extension may race or trail the settle below
            settle(result, startedAt, finishedAt, emitted);
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

    /** Reports a finished step: a predicate must have produced a boolean, a task merges its result.
     *  The run is final by construction -- this worker holds no graph and takes no continuation. */
    private void settle(Object result, long startedAt, long finishedAt,
                        java.util.List<com.wiggle.core.EmittedEvent> emitted) {
        boolean predicate = task.kind() == NodeKind.PREDICATE;
        if (predicate && !(result instanceof Boolean)) {
            reportFailure("predicate '" + task.stepName() + "' returned " + Worker.typeName(result), false);
            return;
        }
        StepReport step = new StepReport(task.nodeId(), predicate ? null : result,
                predicate ? (Boolean) result : null, startedAt, finishedAt, emitted);
        w.client().reportSteps(task.taskId(), task.leaseOwner(), List.of(step), true);
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
