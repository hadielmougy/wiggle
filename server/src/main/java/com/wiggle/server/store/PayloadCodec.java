package com.wiggle.server.store;

import com.wiggle.core.Doc;
import com.wiggle.core.Json;
import com.wiggle.server.store.TokenPayload.Frame;
import com.wiggle.server.store.TokenPayload.FrameKind;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The only place a token payload is JSON. Every store encodes on write and decodes on read, so the
 * engine above never sees the text and the column format stays one decision in one file.
 *
 * <p>The format is the one already in every database, unchanged: a single object carrying the scope
 * stack under {@code __scopes__}, the loop counts under {@code __loops__}, and the staged combine
 * inputs as ordinary keys beside them. Tokens are live runtime state, not versioned reference data,
 * so an in-flight instance has to keep decoding across an upgrade.
 */
public final class PayloadCodec {

    private static final String SCOPES = "__scopes__";
    private static final String LOOPS = "__loops__";
    private static final String KIND = "kind";
    private static final String IDX = "idx";
    private static final String MAP_KEY = "mapKey";
    private static final String VIEW = "view";

    private PayloadCodec() {}

    /** Null or blank text is an empty payload -- what a token outside every scope carries. */
    public static TokenPayload decode(String json) {
        if (json == null || json.isBlank()) return TokenPayload.EMPTY;
        Map<String, Object> m = Json.parseObject(json);
        List<Frame> scopes = frames(m.remove(SCOPES));
        Map<String, Long> loops = loops(m.remove(LOOPS));
        return new TokenPayload(scopes, loops, m);   // whatever is left is staged
    }

    /** Null for an empty payload, so a token that carries nothing stores nothing. */
    public static String encode(TokenPayload payload) {
        if (payload == null || payload.isEmpty()) return null;
        Map<String, Object> m = new LinkedHashMap<>(payload.staged());
        if (!payload.scopes().isEmpty()) {
            List<Object> frames = new ArrayList<>(payload.scopes().size());
            for (Frame f : payload.scopes()) frames.add(frameJson(f));
            m.put(SCOPES, frames);
        }
        if (!payload.loopCounts().isEmpty()) m.put(LOOPS, new LinkedHashMap<>(payload.loopCounts()));
        return Json.write(m);
    }

    private static Map<String, Object> frameJson(Frame f) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put(KIND, f.kind() == FrameKind.ARM ? "arm" : "item");
        out.put(IDX, f.idx());
        if (f.mapKey() != null) out.put(MAP_KEY, f.mapKey());
        out.put(VIEW, f.view().raw());
        return out;
    }

    @SuppressWarnings("unchecked")
    private static List<Frame> frames(Object raw) {
        if (raw == null) return List.of();
        List<Frame> out = new ArrayList<>();
        for (Object o : Json.asArray(raw)) {
            Map<String, Object> m = (Map<String, Object>) o;
            out.add(new Frame(
                    "item".equals(m.get(KIND)) ? FrameKind.ITEM : FrameKind.ARM,
                    ((Number) m.get(IDX)).longValue(),
                    m.get(MAP_KEY) == null ? null : String.valueOf(m.get(MAP_KEY)),
                    Doc.of(m.get(VIEW))));
        }
        return out;
    }

    private static Map<String, Long> loops(Object raw) {
        if (raw == null) return Map.of();
        Map<String, Long> out = new LinkedHashMap<>();
        Json.asObject(raw).forEach((k, v) -> out.put(k, ((Number) v).longValue()));
        return out;
    }
}
