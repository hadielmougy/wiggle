package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

/**
 * A write-buffering view of a transaction. The three hot writes of the drive loop --
 * {@code insertToken}, {@code updateToken}, {@code updateInstance} -- accumulate here and reach
 * the delegate through its bulk methods on {@link #flush}; every other call flushes first and
 * then delegates, so a read can never observe state the buffer is still holding, and a method
 * added to {@link Tx} tomorrow is safe by default rather than wrong by omission.
 *
 * <p>Flush order is inserts, then token updates, then instance updates: a token inserted and then
 * settled inside one buffer must exist before its update runs. Within one shape, order is
 * preserved. The buffer holds references, not copies -- engine writes hand a row to the store and
 * never touch it again, which is what makes that safe.
 *
 * <p>{@link #flush()} must be called before the transaction body returns: the wrapper cannot know
 * when the underlying transaction is about to commit, and unflushed writes are simply lost.
 *
 * <p>Correctness picks the flush points, not throughput: a compensable step, for one, reads the
 * compensation log to number its entry, and that read flushes everything accumulated so far. A
 * saga-heavy batch therefore batches less -- by design, since the alternative is a stale sequence.
 *
 * <p>Wrap only a transaction that rolls back ({@link Tx#transactional()}): a buffer discarded by a
 * throw is only correct when the writes already issued are discarded with it.
 */
public interface BufferedTx extends Tx {

    void flush();

    static BufferedTx of(Tx delegate) {
        List<Token> inserts = new ArrayList<>();
        List<Token> tokenUpdates = new ArrayList<>();
        List<Instance> instanceUpdates = new ArrayList<>();
        Runnable flush = () -> {
            if (!inserts.isEmpty()) { delegate.insertTokens(List.copyOf(inserts)); inserts.clear(); }
            if (!tokenUpdates.isEmpty()) { delegate.updateTokens(List.copyOf(tokenUpdates)); tokenUpdates.clear(); }
            if (!instanceUpdates.isEmpty()) { delegate.updateInstances(List.copyOf(instanceUpdates)); instanceUpdates.clear(); }
        };
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "insertToken"    -> { inserts.add((Token) args[0]); yield null; }
            case "updateToken"    -> { tokenUpdates.add((Token) args[0]); yield null; }
            case "updateInstance" -> { instanceUpdates.add((Instance) args[0]); yield null; }
            case "flush"          -> { flush.run(); yield null; }
            case "toString"       -> "BufferedTx(" + delegate + ")";
            case "hashCode"       -> System.identityHashCode(proxy);
            case "equals"         -> proxy == args[0];
            default -> {
                flush.run();
                try {
                    yield method.invoke(delegate, args);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        };
        return (BufferedTx) Proxy.newProxyInstance(BufferedTx.class.getClassLoader(),
                new Class<?>[]{BufferedTx.class}, handler);
    }
}
