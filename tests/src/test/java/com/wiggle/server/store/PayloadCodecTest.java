package com.wiggle.server.store;

import com.wiggle.core.Doc;
import com.wiggle.core.Json;
import com.wiggle.server.store.TokenPayload.Frame;
import com.wiggle.server.store.TokenPayload.FrameKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tokens are live runtime state, not versioned reference data: an instance mid-flight through an
 * upgrade has its payload read back by the new code. So the column format is fixed, and this pins
 * it — both that today's writer produces it and that yesterday's rows still decode.
 */
class PayloadCodecTest {

    @Test @DisplayName("a payload written before this refactor decodes into typed frames")
    void decodesTheStoredFormat() {
        String stored = """
                {"__arm__charge":{"paid":true},\
                "__scopes__":[{"kind":"arm","idx":1,"view":{"id":"A-1"}},\
                {"kind":"item","idx":2,"mapKey":"eu","view":7}],\
                "__loops__":{"n3":4}}""";

        TokenPayload p = PayloadCodec.decode(stored);

        assertEquals(2, p.scopes().size());
        Frame arm = p.scopes().getFirst();
        assertEquals(FrameKind.ARM, arm.kind());
        assertEquals(1, arm.idx());
        assertNull(arm.mapKey());
        assertEquals(Doc.of(Map.of("id", "A-1")), arm.view());

        Frame item = p.scopes().getLast();
        assertEquals(FrameKind.ITEM, item.kind());
        assertEquals("eu", item.mapKey());
        assertEquals(1, p.innermostItem(), "the item frame is the innermost one");

        assertEquals(4L, p.loopCount("n3"));
        assertEquals(Map.of("__arm__charge", Map.of("paid", true)), p.staged(),
                "everything the engine does not reserve is a staged combine input");
    }

    @Test @DisplayName("encode then decode is the identity, values and all")
    void roundTrips() {
        TokenPayload p = TokenPayload.EMPTY
                .push(FrameKind.ARM, 0, null, Doc.of(Map.of("a", 1L)))
                .push(FrameKind.ITEM, 3, "k", Doc.of(List.of("x", "y")))
                .withLoopCount("guard", 9)
                .withStaged(Map.of("__arm__left", Map.of("n", 2L)));

        assertEquals(p, PayloadCodec.decode(PayloadCodec.encode(p)));
    }

    @Test @DisplayName("an empty payload stores nothing, and nothing decodes back to empty")
    void emptyIsNull() {
        assertNull(PayloadCodec.encode(TokenPayload.EMPTY));
        assertNull(PayloadCodec.encode(null));
        assertSame(TokenPayload.EMPTY, PayloadCodec.decode(null));
        assertTrue(PayloadCodec.decode("  ").isEmpty());
        assertTrue(PayloadCodec.decode("{}").isEmpty());
    }

    @Test @DisplayName("the written format is still the one already in every database")
    void writesTheLegacyShape() {
        TokenPayload p = TokenPayload.EMPTY
                .push(FrameKind.ITEM, 2, "eu", Doc.of(Map.of("id", "A-1")))
                .withLoopCount("n3", 4);
        Map<String, Object> written = Json.parseObject(PayloadCodec.encode(p));

        assertTrue(written.containsKey("__scopes__"));
        assertEquals(Map.of("n3", 4L), written.get("__loops__"));
        Map<?, ?> frame = (Map<?, ?>) ((List<?>) written.get("__scopes__")).getFirst();
        assertEquals("item", frame.get("kind"));
        assertEquals(2L, frame.get("idx"));
        assertEquals("eu", frame.get("mapKey"));
    }

    @Test @DisplayName("a compensation token's snapshots ride as staged inputs")
    void compensationSnapshots() {
        TokenPayload p = TokenPayload.EMPTY.withStaged(
                Map.of("input", Map.of("id", "A-1"), "result", Map.of("id", "A-1", "paid", true)));
        TokenPayload back = PayloadCodec.decode(PayloadCodec.encode(p));
        assertEquals(p.staged(), back.staged());
        assertTrue(back.scopes().isEmpty());
    }

    @Test @DisplayName("every with/push returns a new value -- stored copies never alias")
    void isImmutable() {
        TokenPayload base = TokenPayload.EMPTY.push(FrameKind.ARM, 0, null, Doc.of(Map.of("v", 1L)));
        TokenPayload changed = base.withTopView(Doc.of(Map.of("v", 2L)));

        assertEquals(Doc.of(Map.of("v", 1L)), base.top().view(), "the original is untouched");
        assertEquals(Doc.of(Map.of("v", 2L)), changed.top().view());
        assertEquals(1, base.push(FrameKind.ITEM, 0, null, Doc.of("x")).scopes().size() - 1,
                "push does not grow the receiver");
    }
}
