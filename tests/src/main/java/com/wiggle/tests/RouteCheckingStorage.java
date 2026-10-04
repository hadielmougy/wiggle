package com.wiggle.tests;

import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.List;
import java.util.function.Function;

/**
 * A {@link Storage} that refuses every transaction not opened through a routed entry point. Run the
 * engine over it and any call site that still opens a bare {@link #inTx} fails, rather than writing
 * to whichever database happens to be the only one.
 */
public final class RouteCheckingStorage implements Storage {

    private final Storage inner;

    public RouteCheckingStorage(Storage inner) {
        this.inner = inner;
    }

    @Override public void migrate() { inner.migrate(); }

    @Override public <R> R inTx(Function<Tx, R> work) {
        throw new IllegalStateException("unrouted transaction: open it with inTxFor, inShard, inHome, "
                + "readFor or readShard");
    }

    @Override public <R> R inShard(int shard, Function<Tx, R> work) {
        if (shard != home() && !instanceShards().contains(shard)) {
            throw new IllegalStateException("transaction routed to unknown shard " + shard);
        }
        return inner.inTx(work);
    }

    @Override public List<Integer> instanceShards() { return inner.instanceShards(); }

    @Override public int home() { return inner.home(); }

    @Override public String fingerprint() { return inner.fingerprint(); }

    @Override public void close() { inner.close(); }
}
