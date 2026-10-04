package com.wiggle.server.store;

import com.wiggle.core.TokenStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** What reaches the store when the buffer flushes: each token once, in its final state, newest write last. */
class BufferedTxTest {

    /** Records the writes a flush sends, as "op id status". */
    private final List<String> writes = new ArrayList<>();

    private BufferedTx buffered() {
        Tx recorder = (Tx) Proxy.newProxyInstance(Tx.class.getClassLoader(), new Class<?>[] {Tx.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "insertTokens" -> ((List<?>) args[0]).forEach(t -> writes.add("insert " + line((Rows.Token) t)));
                        case "updateTokens" -> ((List<?>) args[0]).forEach(t -> writes.add("update " + line((Rows.Token) t)));
                        case "updateInstances" -> ((List<?>) args[0]).forEach(i -> writes.add("instance " + ((Rows.Instance) i).id));
                        default -> { }
                    }
                    return null;
                });
        return BufferedTx.of(recorder);
    }

    private static String line(Rows.Token t) {
        return t.id + " " + t.status;
    }

    private static Rows.Token token(String id, TokenStatus status) {
        Rows.Token t = new Rows.Token();
        t.id = id;
        t.status = status;
        return t;
    }

    @Test @DisplayName("a token inserted and then moved on in the same transaction is one insert, in its final state")
    void insertThenUpdate() {
        BufferedTx tx = buffered();
        Rows.Token next = token("tok-1", TokenStatus.WAITING);
        tx.insertToken(next);
        next.status = TokenStatus.READY;
        tx.updateToken(next);
        tx.flush();
        assertEquals(List.of("insert tok-1 READY"), writes);
    }

    @Test @DisplayName("a token updated twice is written once, with its last state")
    void updateTwice() {
        BufferedTx tx = buffered();
        Rows.Token t = token("tok-1", TokenStatus.RUNNING);
        tx.updateToken(t);
        t.status = TokenStatus.DONE;
        tx.updateToken(t);
        tx.flush();
        assertEquals(List.of("update tok-1 DONE"), writes);
    }

    @Test @DisplayName("two rows of one token: whichever was written last is written last")
    void twoRowsOfOneToken() {
        BufferedTx tx = buffered();
        Rows.Token a = token("tok-1", TokenStatus.RUNNING);
        Rows.Token b = token("tok-1", TokenStatus.WAITING);
        tx.updateToken(a);
        tx.updateToken(b);
        a.status = TokenStatus.DONE;
        tx.updateToken(a);
        tx.flush();
        assertEquals(List.of("update tok-1 WAITING", "update tok-1 DONE"), writes);

        writes.clear();
        Rows.Token inserted = token("tok-2", TokenStatus.READY);
        Rows.Token other = token("tok-2", TokenStatus.RUNNING);
        tx.insertToken(inserted);
        tx.updateToken(other);
        inserted.status = TokenStatus.DONE;
        tx.updateToken(inserted);
        tx.flush();
        assertEquals(List.of("insert tok-2 DONE", "update tok-2 RUNNING", "update tok-2 DONE"), writes,
                "the insert cannot carry an update made after another row of the same token was written");
    }

    @Test @DisplayName("instance updates are never folded: each advances the row's revision")
    void instanceUpdatesKept() {
        BufferedTx tx = buffered();
        Rows.Instance i = new Rows.Instance();
        i.id = "wfi-1";
        tx.updateInstance(i);
        tx.updateInstance(i);
        tx.flush();
        assertEquals(List.of("instance wfi-1", "instance wfi-1"), writes);
    }

    @Test @DisplayName("any other call flushes first, so it sees every write made before it")
    void readsFlush() {
        BufferedTx tx = buffered();
        tx.insertToken(token("tok-1", TokenStatus.READY));
        tx.findToken("tok-1");
        assertEquals(List.of("insert tok-1 READY"), writes);
    }
}
