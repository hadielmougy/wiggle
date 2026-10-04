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
 * <p>A flush is one {@link Tx#writeAll}: inserts, then token updates, then instance updates. Within one shape, order is
 * preserved. The buffer holds references, not copies, and a token is written once with the state
 * it has at the flush: an update of a token the buffer already inserted adds nothing, since the
 * insert carries it, and an update of one it already holds as an update moves that update last. This is what makes the INSERT-then-UPDATE of a token
 * created and moved on in one transaction a single INSERT. An instance update is never folded,
 * because each one advances the row's revision, which counts its writes.
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
            if (inserts.isEmpty() && tokenUpdates.isEmpty() && instanceUpdates.isEmpty()) return;
            delegate.writeAll(List.copyOf(inserts), List.copyOf(tokenUpdates), List.copyOf(instanceUpdates));
            inserts.clear();
            tokenUpdates.clear();
            instanceUpdates.clear();
        };
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "insertToken"    -> { inserts.add((Token) args[0]); yield null; }
            case "updateToken"    -> {
                Token t = (Token) args[0];
                boolean wasHeld = tokenUpdates.removeIf(r -> r == t);
                // An inserted token's insert carries this update, unless another row of the same id
                // was updated since: then this one goes last, so the newest state is written last.
                boolean insertCarriesIt = holds(inserts, t) && tokenUpdates.stream().noneMatch(r -> r.id.equals(t.id));
                if (!insertCarriesIt || wasHeld) tokenUpdates.add(t);
                yield null;
            }
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

    /** Whether {@code rows} holds this very row: identity, since a row is the engine's own object. */
    private static boolean holds(List<Token> rows, Token t) {
        for (Token r : rows) if (r == t) return true;
        return false;
    }
}
