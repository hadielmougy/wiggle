package com.wiggle.server.engine;

import com.wiggle.core.Cron;
import com.wiggle.core.Doc;
import com.wiggle.core.Ids;
import com.wiggle.server.store.Rows;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Recurring starts. A schedule is a row with a next-fire time; the leader fires the ones that have
 * come due, each as an ordinary instance start correlated to its schedule.
 */
final class Schedules {

    private static final System.Logger LOG = System.getLogger(Schedules.class.getName());

    private final Transactions transactions;
    private final InstanceLifecycle instances;

    Schedules(Transactions transactions, InstanceLifecycle instances) {
        this.transactions = transactions;
        this.instances = instances;
    }

    /** Creates a recurring start: {@code workflow} fires every {@code every}, first fire after one interval. */
    String every(String workflow, Duration every, Object context) {
        if (every.toMillis() < 1) throw EngineException.badRequest("schedule interval must be positive");
        Rows.Schedule s = new Rows.Schedule();
        s.intervalMillis = every.toMillis();
        s.nextFireAt = System.currentTimeMillis() + s.intervalMillis;
        return put(workflow, context, s, "every " + s.intervalMillis + "ms");
    }

    /** Creates a recurring start on a five-field cron expression (evaluated in UTC). */
    String cron(String workflow, String cron, Object context) {
        Cron parsed;
        try {
            parsed = Cron.parse(cron);
        } catch (IllegalArgumentException e) {
            throw EngineException.badRequest(e.getMessage());
        }
        Rows.Schedule s = new Rows.Schedule();
        s.cron = parsed.expression();
        s.nextFireAt = parsed.next(System.currentTimeMillis());
        return put(workflow, context, s, "cron '" + s.cron + "'");
    }

    /**
     * Upserts by workflow: a workflow has at most one schedule, so re-creating one (e.g. from
     * several app instances doing "ensure my schedule exists" on startup) updates the existing
     * row's cadence/context in place instead of piling up duplicate firers.
     */
    private String put(String workflow, Object context, Rows.Schedule s, String cadence) {
        return transactions.read(tx -> {
            tx.latestVersion(workflow).orElseThrow(
                    () -> EngineException.notFound("workflow '" + workflow + "'"));
            Optional<Rows.Schedule> existing = tx.scheduleByWorkflow(workflow);
            s.id = existing.map(e -> e.id).orElseGet(() -> Ids.next("sched"));
            s.workflow = workflow;
            s.context = Doc.of(context);
            s.createdAt = existing.map(e -> e.createdAt).orElseGet(System::currentTimeMillis);
            tx.putSchedule(s);
            boolean replaced = existing.isPresent();
            LOG.log(System.Logger.Level.INFO, () -> "schedule " + s.id + ": " + workflow + " " + cadence
                    + (replaced ? " (replacing existing schedule for this workflow)" : ""));
            return s.id;
        });
    }

    void delete(String id) {
        transactions.readVoid(tx -> tx.deleteSchedule(id));
    }

    List<Rows.Schedule> all() {
        return transactions.read(tx -> tx.schedules());
    }

    /**
     * Leader duty: start instances for schedules whose fire time has passed. The compare-and-set
     * on the fire time makes each fire exactly-once even if two leaders briefly overlap; missed
     * fires do not burst -- the next fire is one interval from now.
     */
    int fireDue(int max) {
        long now = System.currentTimeMillis();
        List<Rows.Schedule> due = transactions.read(tx -> tx.dueSchedules(now, max));
        int fired = 0;
        for (Rows.Schedule sched : due) {
            try {
                if (fire(sched, now)) fired++;
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "schedule " + sched.id + " failed to fire: " + e);
            }
        }
        return fired;
    }

    private boolean fire(Rows.Schedule sched, long now) {
        return transactions.inTx(tx -> {
            if (!tx.claimSchedule(sched.id, sched.nextFireAt, nextFire(sched, now))) return false;
            String id = instances.start(tx, sched.workflow, null, sched.context.raw(),
                    "schedule:" + sched.id, null);
            LOG.log(System.Logger.Level.DEBUG, () -> "schedule " + sched.id + " fired -> instance " + id);
            return true;
        });
    }

    /** Next fire after {@code now}: one interval ahead, or the cron's next UTC match. */
    private static long nextFire(Rows.Schedule sched, long now) {
        if (sched.cron == null) return now + sched.intervalMillis;
        return Cron.parse(sched.cron).next(now);
    }
}
