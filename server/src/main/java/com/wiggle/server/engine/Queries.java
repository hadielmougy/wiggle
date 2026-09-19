package com.wiggle.server.engine;

import com.wiggle.core.InstanceView;
import com.wiggle.placement.IdCodec;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Read-only views over the store and the poller roster. Nothing here moves a token. */
final class Queries {

    /** Cap on the live set scanned for the epoch census; a draining epoch shrinks, so this is ample. */
    private static final int LIVE_CENSUS_CAP = 100_000;

    private final Storage storage;
    private final PollerRegistry pollers;

    Queries(Storage storage, PollerRegistry pollers) {
        this.storage = storage;
        this.pollers = pollers;
    }

    List<Rows.BacklogSlice> backlog(int max) {
        return storage.inTx(tx -> tx.backlogByVersion(System.currentTimeMillis(), max));
    }

    boolean covered(String workflow, int version, String queue) {
        return pollers.covers(workflow, version, queue, System.currentTimeMillis());
    }

    Set<PollerRegistry.Poller> livePollers() {
        return pollers.live(System.currentTimeMillis());
    }

    Optional<InstanceView> instance(String id) {
        return storage.inTx(tx -> tx.findInstance(id).map(Queries::view));
    }

    List<InstanceView> list(String workflow, String status, int limit) {
        InstanceStatus s = status == null ? null : InstanceStatus.valueOf(status.toUpperCase(Locale.ROOT));
        return storage.inTx(tx -> tx.listInstances(workflow, s, limit).stream().map(Queries::view).toList());
    }

    List<InstanceView> findByCorrelation(String correlationId, int limit) {
        return storage.inTx(tx -> tx.findByCorrelation(correlationId, limit).stream().map(Queries::view).toList());
    }

    List<Token> tokens(String instanceId) {
        return storage.inTx(tx -> tx.tokensOf(instanceId));
    }

    Map<Long, Integer> liveCountByEpoch() {
        return storage.inTx(tx -> {
            Map<Long, Integer> out = new HashMap<>();
            for (Instance i : tx.listInstances(null, InstanceStatus.RUNNING, LIVE_CENSUS_CAP)) {
                long epoch = IdCodec.parse(i.id).map(IdCodec.Placement::epoch).orElse(0L);
                out.merge(epoch, 1, Integer::sum);
            }
            return out;
        });
    }

    Rows.QueueDepth queueDepth() {
        return storage.inTx(tx -> tx.queueDepth(System.currentTimeMillis()));
    }

    int tasksProcessedSince(long since) {
        return storage.inTx(tx -> tx.countProcessedSince(since));
    }

    List<Token> pendingSignals(int max) {
        return storage.inTx(tx -> tx.pendingSignals(max));
    }

    private static InstanceView view(Instance i) {
        return new InstanceView(i.id, i.workflow, i.version, i.status.name(), i.terminationReason,
                i.error, i.context.raw(), i.createdAt, i.updatedAt);
    }
}
