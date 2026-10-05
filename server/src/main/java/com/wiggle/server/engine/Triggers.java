package com.wiggle.server.engine;

import com.wiggle.core.Ids;
import com.wiggle.core.Json;
import com.wiggle.core.ShardIds;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Tx;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Starts on other instances' events. A trigger is a home-shard row naming a target workflow, a
 * source workflow (or {@code *}) and the event types it fires on; the leader reads each instance
 * shard's event log behind a per-shard dispatch position and starts the target on that same shard.
 * Moving the position and starting the instances share one transaction, guarded by a
 * compare-and-set on the position, so each event fires each trigger exactly once.
 */
final class Triggers {

    private static final System.Logger LOG = System.getLogger(Triggers.class.getName());

    static final String ANY = "*";
    /** How many triggered starts may follow one another before a chain is refused. */
    static final int MAX_DEPTH = 16;
    private static final String CORRELATION_PREFIX = "trigger:";
    private static final int MAX_TYPE_LENGTH = 32;
    private static final Set<String> LIFECYCLE = Set.of(Events.STARTED, Events.COMPLETED, Events.FAILED,
            Events.CANCELLED, Events.COMPENSATING, Events.COMPENSATED, Events.COMPENSATION_FAILED);

    private final Transactions transactions;
    private final Instances instances;
    private final long visibilityMillis;

    Triggers(Transactions transactions, Instances instances, long visibilityMillis) {
        this.transactions = transactions;
        this.instances = instances;
        this.visibilityMillis = visibilityMillis;
    }

    /**
     * Upserts by (workflow, source). The trigger fires on events appended after it was first
     * created; every instance shard's dispatch position exists before the row does.
     */
    String put(String workflow, String source, List<String> eventTypes, boolean includeContext) {
        if (workflow == null || workflow.isBlank()) throw EngineException.badRequest("a trigger names the workflow it starts");
        if (source == null || source.isBlank()) throw EngineException.badRequest("a trigger names its source workflow, or '*'");
        if (source.equals(workflow)) throw EngineException.badRequest("a trigger cannot start the workflow it listens to");
        List<String> types = validTypes(eventTypes);
        transactions.readHome(tx -> tx.latestVersion(workflow).orElseThrow(
                () -> EngineException.notFound("workflow '" + workflow + "'")));
        long now = System.currentTimeMillis();
        for (int shard : transactions.instanceShards()) {
            transactions.readShard(shard, tx -> {
                tx.createTriggerCursorIfAbsent(tx.latestEventSeq(), now);
                return null;
            });
        }
        return transactions.readHome(tx -> {
            Optional<Rows.Trigger> existing = tx.triggers().stream()
                    .filter(t -> t.workflow.equals(workflow) && t.source.equals(source))
                    .findFirst();
            Rows.Trigger t = new Rows.Trigger();
            t.id = existing.map(e -> e.id).orElseGet(() -> Ids.next("trg"));
            t.workflow = workflow;
            t.source = source;
            t.eventTypes = types;
            t.includeContext = includeContext;
            t.createdAt = existing.map(e -> e.createdAt).orElse(now);
            tx.putTrigger(t);
            LOG.log(System.Logger.Level.INFO, () -> "trigger " + t.id + ": " + source + " " + types + " -> "
                    + workflow + (existing.isPresent() ? " (replacing existing trigger for this route)" : ""));
            return t.id;
        });
    }

    private static List<String> validTypes(List<String> eventTypes) {
        if (eventTypes == null || eventTypes.isEmpty()) {
            throw EngineException.badRequest("a trigger fires on at least one event type");
        }
        Set<String> out = new LinkedHashSet<>();
        for (String type : eventTypes) {
            if (type == null || type.isBlank()) throw EngineException.badRequest("an event type is not blank");
            if (type.startsWith("wf.") && !LIFECYCLE.contains(type)) {
                throw EngineException.badRequest("'" + type + "' is not a lifecycle event; the lifecycle types are "
                        + new java.util.TreeSet<>(LIFECYCLE));
            }
            if (type.indexOf(',') >= 0 || type.length() > MAX_TYPE_LENGTH) {
                throw EngineException.badRequest("event type '" + type + "' holds a ',' or is over "
                        + MAX_TYPE_LENGTH + " characters");
            }
            out.add(type);
        }
        return List.copyOf(out);
    }

    /** Deletes a trigger; with none left, the dispatch positions go too, so the next trigger starts at the tail. */
    boolean delete(String id) {
        boolean[] deleted = new boolean[1];
        boolean none = transactions.readHome(tx -> {
            deleted[0] = tx.deleteTrigger(id);
            return tx.triggers().isEmpty();
        });
        if (deleted[0] && none) {
            for (int shard : transactions.instanceShards()) {
                transactions.readShard(shard, tx -> {
                    tx.deleteTriggerCursor();
                    return null;
                });
            }
        }
        return deleted[0];
    }

    List<Rows.Trigger> all() {
        return transactions.readHome(Tx::triggers);
    }

    /**
     * Leader duty: dispatches up to {@code max} events past each instance shard's position. Returns
     * how many events were dispatched, so a full batch drains.
     */
    int dispatch(int max) {
        List<Rows.Trigger> triggers = all();
        if (triggers.isEmpty()) return 0;
        int dispatched = 0;
        for (int shard : transactions.instanceShards()) {
            try {
                dispatched += dispatchShard(shard, triggers, max);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "trigger dispatch on shard " + shard + " failed: " + e);
            }
        }
        return dispatched;
    }

    private record Batch(long cursor, List<Rows.Event> events) { }

    private int dispatchShard(int shard, List<Rows.Trigger> triggers, int max) {
        long now = System.currentTimeMillis();
        Batch batch = transactions.readShard(shard, tx -> {
            Long cursor = tx.triggerCursor();
            if (cursor == null) {
                tx.createTriggerCursorIfAbsent(tx.latestEventSeq(), now);
                return null;
            }
            return new Batch(cursor, tx.eventsAfter(cursor, now - visibilityMillis, max));
        });
        if (batch == null || batch.events().isEmpty()) return 0;
        long last = batch.events().getLast().seq();
        try {
            boolean won = transactions.inShard(shard, tx -> {
                if (!tx.moveTriggerCursor(batch.cursor(), last, now)) return false;
                for (Rows.Event e : batch.events()) fire(tx, shard, triggers, e);
                return true;
            });
            return won ? batch.events().size() : 0;
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "trigger batch on shard " + shard + " failed, dispatching "
                    + "its events one at a time: " + e);
            return oneAtATime(shard, triggers, batch, now);
        }
    }

    /** Dispatches each event in its own transaction; an event whose starts throw is skipped. */
    private int oneAtATime(int shard, List<Rows.Trigger> triggers, Batch batch, long now) {
        long at = batch.cursor();
        int done = 0;
        for (Rows.Event e : batch.events()) {
            long from = at;
            boolean moved;
            try {
                moved = transactions.inShard(shard, tx -> {
                    if (!tx.moveTriggerCursor(from, e.seq(), now)) return false;
                    fire(tx, shard, triggers, e);
                    return true;
                });
            } catch (RuntimeException failure) {
                LOG.log(System.Logger.Level.WARNING, "event " + e.seq() + " (" + e.type() + " of instance "
                        + e.instanceId() + ") on shard " + shard + " skipped: its triggered start failed: " + failure);
                moved = transactions.inShard(shard, tx -> tx.moveTriggerCursor(from, e.seq(), now));
            }
            if (!moved) return done;
            at = e.seq();
            done++;
        }
        return done;
    }

    private void fire(Tx tx, int shard, List<Rows.Trigger> triggers, Rows.Event e) {
        for (Rows.Trigger t : triggers) {
            if (!matches(t, e)) continue;
            if (tx.latestVersion(t.workflow).isEmpty()) {
                LOG.log(System.Logger.Level.WARNING, () -> "trigger " + t.id + " skipped event " + e.seq()
                        + ": workflow '" + t.workflow + "' is not registered");
                continue;
            }
            int depth = depthOf(tx, e.instanceId()) + 1;
            if (depth > MAX_DEPTH) {
                LOG.log(System.Logger.Level.WARNING, () -> "trigger " + t.id + " skipped event " + e.seq()
                        + " of instance " + e.instanceId() + ": " + MAX_DEPTH + " triggered starts already lead to it");
                continue;
            }
            String id = instances.start(tx, ShardIds.next("wfi", shard), t.workflow, null,
                    context(tx, t, e, depth), CORRELATION_PREFIX + t.id + ":" + e.instanceId(), null);
            LOG.log(System.Logger.Level.DEBUG, () -> "trigger " + t.id + " fired on " + e.type() + " of "
                    + e.instanceId() + " -> instance " + id);
        }
    }

    static boolean matches(Rows.Trigger t, Rows.Event e) {
        if (e.createdAt() < t.createdAt || !t.eventTypes.contains(e.type())) return false;
        if (ANY.equals(t.source)) return !t.workflow.equals(e.workflow());
        return t.source.equals(e.workflow());
    }

    /** How many triggered starts lead to {@code instanceId}, walking its correlation ids back. */
    private static int depthOf(Tx tx, String instanceId) {
        int depth = 0;
        String id = instanceId;
        while (depth <= MAX_DEPTH) {
            String correlation = tx.findInstance(id).map(i -> i.correlationId).orElse(null);
            if (correlation == null || !correlation.startsWith(CORRELATION_PREFIX)) break;
            int split = correlation.indexOf(':', CORRELATION_PREFIX.length());
            if (split < 0) break;
            id = correlation.substring(split + 1);
            depth++;
        }
        return depth;
    }

    /** The source's context when the trigger asks for it, with what fired it under {@code trigger}. */
    private static Map<String, Object> context(Tx tx, Rows.Trigger t, Rows.Event e, int depth) {
        Map<String, Object> ctx = new LinkedHashMap<>();
        if (t.includeContext) {
            tx.findInstance(e.instanceId())
                    .filter(i -> i.context.isObject())
                    .ifPresent(i -> ctx.putAll(Json.asObject(i.context.raw())));
        }
        Map<String, Object> fired = new LinkedHashMap<>();
        fired.put("triggerId", t.id);
        fired.put("event", e.type());
        fired.put("seq", e.seq());
        fired.put("instanceId", e.instanceId());
        fired.put("workflow", e.workflow());
        fired.put("version", e.version());
        if (e.correlationId() != null) fired.put("correlationId", e.correlationId());
        if (e.nodeId() != null) fired.put("nodeId", e.nodeId());
        if (e.payload() != null) fired.put("payload", Json.parse(e.payload()));
        fired.put("depth", depth);
        ctx.put("trigger", fired);
        return ctx;
    }
}
