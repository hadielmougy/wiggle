package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;

import java.util.ArrayList;
import java.util.List;

/**
 * The held-back writes of one transaction. The three hot writes of the drive loop --
 * {@code insertToken}, {@code updateToken}, {@code updateInstance} -- accumulate here and reach the
 * store through its bulk methods on {@link #flushTo}. The store flushes before it runs any other
 * statement of the transaction, so a read never misses state the buffer is still holding, and
 * before the transaction commits.
 *
 * <p>Flush order is inserts, then token updates, then instance updates. Within one shape, order is
 * preserved. The buffer holds references, not copies, and a token is written once with the state
 * it has at the flush: an update of a token the buffer already inserted adds nothing, since the
 * insert carries it, and an update of one it already holds as an update moves that update last.
 * This is what makes the INSERT-then-UPDATE of a token created and moved on in one transaction a
 * single INSERT. An instance update is never folded, because each one advances the row's revision,
 * which counts its writes.
 *
 * <p>Correctness picks the flush points, not throughput: a compensable step, for one, reads the
 * compensation log to number its entry, and that read flushes everything accumulated so far. A
 * saga-heavy batch therefore batches less -- by design, since the alternative is a stale sequence.
 */
public final class WriteBuffer {

    private final List<Token> inserts = new ArrayList<>();
    private final List<Token> tokenUpdates = new ArrayList<>();
    private final List<Instance> instanceUpdates = new ArrayList<>();

    public void insertToken(Token t) {
        inserts.add(t);
    }

    public void updateToken(Token t) {
        boolean wasHeld = tokenUpdates.removeIf(r -> r == t);
        // An inserted token's insert carries this update, unless another row of the same id was
        // updated since: then this one goes last, so the newest state is written last.
        boolean insertCarriesIt = holds(inserts, t) && tokenUpdates.stream().noneMatch(r -> r.id.equals(t.id));
        if (!insertCarriesIt || wasHeld) tokenUpdates.add(t);
    }

    public void updateInstance(Instance i) {
        instanceUpdates.add(i);
    }

    public boolean isEmpty() {
        return inserts.isEmpty() && tokenUpdates.isEmpty() && instanceUpdates.isEmpty();
    }

    /** Writes what is held to {@code tokens} and {@code instances} through their bulk methods, and empties the buffer. */
    public void flushTo(TokenRepository tokens, InstanceRepository instances) {
        if (!inserts.isEmpty()) {
            List<Token> batch = List.copyOf(inserts);
            inserts.clear();
            tokens.insertTokens(batch);
        }
        if (!tokenUpdates.isEmpty()) {
            List<Token> batch = List.copyOf(tokenUpdates);
            tokenUpdates.clear();
            tokens.updateTokens(batch);
        }
        if (!instanceUpdates.isEmpty()) {
            List<Instance> batch = List.copyOf(instanceUpdates);
            instanceUpdates.clear();
            instances.updateInstances(batch);
        }
    }

    /** Whether {@code rows} holds this very row: identity, since a row is the engine's own object. */
    private static boolean holds(List<Token> rows, Token t) {
        for (Token r : rows) if (r == t) return true;
        return false;
    }
}
