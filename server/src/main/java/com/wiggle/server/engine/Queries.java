package com.wiggle.server.engine;

import com.wiggle.core.InstanceView;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.core.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Freshness;
import com.wiggle.server.store.ReadTx;
import com.wiggle.server.store.Storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Read-only views over the store and the poller roster. Nothing here moves a token. */
final class Queries {

    private final Storage storage;
    private final PollerRegistry pollers;

    Queries(Storage storage, PollerRegistry pollers) {
        this.storage = storage;
        this.pollers = pollers;
    }

    List<Rows.BacklogSlice> backlog(int max) {
        long now = System.currentTimeMillis();
        Map<String, Rows.BacklogSlice> merged = new LinkedHashMap<>();
        for (Rows.BacklogSlice s : readEach(tx -> tx.backlogByVersion(now, max))) {
            merged.merge(s.workflow() + ":" + s.version() + ":" + s.queue(), s, (a, b) ->
                    new Rows.BacklogSlice(a.workflow(), a.version(), a.queue(), a.readyCount() + b.readyCount(),
                            Math.min(a.oldestAvailableAt(), b.oldestAvailableAt())));
        }
        return merged.values().stream()
                .sorted(Comparator.comparingInt(Rows.BacklogSlice::readyCount).reversed()).limit(max).toList();
    }

    /** A conditional slice is counted only up to this many ready tasks, and only this many are counted. */
    static final int COUNT_SLICES_UP_TO = 100_000;
    static final int COUNT_CAP = 10_000;

    WorkflowEngine.SliceCoverage coverage(Rows.BacklogSlice s) {
        long now = System.currentTimeMillis();
        WorkflowVersion wv = new WorkflowVersion(s.workflow(), s.version());
        PollerRegistry.Coverage c = pollers.coverage(wv, s.queue(), now);
        List<String> criteria = c.conditions().stream().map(Criteria::describe).toList();
        Integer claimable = switch (c.state()) {
            case COVERED -> s.readyCount();
            case UNCOVERED -> 0;
            case CONDITIONAL -> {
                if (s.readyCount() > COUNT_SLICES_UP_TO) yield null;
                Predicate any = c.conditions().size() == 1 ? c.conditions().getFirst() : new Predicate.Or(c.conditions());
                int n = 0;
                for (int shard : storage.instanceShards()) {
                    n += storage.readShard(shard, Freshness.REPLICA_OK,
                            tx -> tx.countReady(s.workflow(), s.version(), s.queue(), any, now, COUNT_CAP));
                }
                yield n >= COUNT_CAP && s.readyCount() > COUNT_CAP ? null : n;
            }
        };
        return new WorkflowEngine.SliceCoverage(c.state(), claimable, criteria);
    }

    Set<PollerRegistry.Poller> livePollers() {
        return pollers.live(System.currentTimeMillis());
    }

    Optional<InstanceView> instance(String id) {
        return storage.readFor(id, Freshness.PRIMARY, tx -> tx.findInstance(id).map(Queries::view));
    }

    List<InstanceView> list(String workflow, String status, int limit) {
        InstanceStatus s = status == null ? null : InstanceStatus.valueOf(status.toUpperCase(Locale.ROOT));
        return newestFirst(readEach(tx -> tx.listInstances(workflow, s, limit)), limit);
    }

    List<InstanceView> findByCorrelation(String correlationId, int limit) {
        return newestFirst(readEach(tx -> tx.findByCorrelation(correlationId, limit)), limit);
    }

    List<Token> tokens(String instanceId) {
        return storage.readFor(instanceId, Freshness.PRIMARY, tx -> tx.tokensOf(instanceId));
    }

    Rows.QueueDepth queueDepth() {
        long now = System.currentTimeMillis();
        int count = 0;
        long oldest = 0;
        for (int shard : storage.instanceShards()) {
            Rows.QueueDepth d = storage.readShard(shard, Freshness.PRIMARY, tx -> tx.queueDepth(now));
            if (d.readyCount() == 0) continue;
            oldest = count == 0 ? d.oldestAvailableAt() : Math.min(oldest, d.oldestAvailableAt());
            count += d.readyCount();
        }
        return new Rows.QueueDepth(count, oldest);
    }

    List<Token> pendingSignals(int max) {
        return readEach(tx -> tx.pendingSignals(max)).stream()
                .sorted(Comparator.comparingLong((Token t) -> t.createdAt)).limit(max).toList();
    }

    /** A console read on every instance shard, a replica allowed, the lists concatenated. */
    private <T> List<T> readEach(Function<ReadTx, List<T>> read) {
        List<T> out = new ArrayList<>();
        for (int shard : storage.instanceShards()) out.addAll(storage.readShard(shard, Freshness.REPLICA_OK, read));
        return out;
    }

    private static List<InstanceView> newestFirst(List<Instance> found, int limit) {
        return found.stream().sorted(Comparator.comparingLong((Instance i) -> i.createdAt).reversed())
                .limit(limit).map(Queries::view).toList();
    }

    private static InstanceView view(Instance i) {
        return new InstanceView(i.id, i.workflow, i.version, i.status.name(), i.terminationReason,
                i.error, i.context.raw(), i.createdAt, i.updatedAt);
    }
}
