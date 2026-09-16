package com.wiggle.server.coord;

import com.wiggle.placement.Ring;
import com.wiggle.core.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON codec for a policy's epoch ring history, shared by every {@link CoordinatorStore} that persists
 * the ring as a text blob (JDBC {@code coord_policy.epochs}, Cassandra {@code coord_policy.epochs}). One
 * encoding keeps the on-disk form identical across backends.
 */
public final class EpochCodec {

    private EpochCodec() {}

    public static String encode(Map<Long, Ring.Epoch> epochs) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<Long, Ring.Epoch> e : epochs.entrySet()) {
            Ring.Epoch er = e.getValue();
            List<Object> ring = new ArrayList<>();
            for (Ring.Slot s : er.ring()) {
                Map<String, Object> slot = new LinkedHashMap<>();
                slot.put("shard", s.shard());
                slot.put("cellId", s.cellId());
                slot.put("region", s.region());
                ring.add(slot);
            }
            Map<String, Object> ringObj = new LinkedHashMap<>();
            ringObj.put("status", er.status().name());
            ringObj.put("ring", ring);
            out.put(Long.toString(e.getKey()), ringObj);
        }
        return Json.write(out);
    }

    public static Map<Long, Ring.Epoch> decode(String json) {
        Map<Long, Ring.Epoch> out = new LinkedHashMap<>();
        if (json == null || json.isBlank()) return out;
        Map<String, Object> obj = Json.parseObject(json);
        for (Map.Entry<String, Object> e : obj.entrySet()) {
            Map<String, Object> er = Json.asObject(e.getValue());
            Ring.Status status = Ring.Status.valueOf(Json.reqStr(er, "status"));
            List<Ring.Slot> ring = new ArrayList<>();
            for (Object o : Json.asArray(er.get("ring"))) {
                Map<String, Object> sm = Json.asObject(o);
                ring.add(new Ring.Slot((int) Json.num(sm, "shard", 0), Json.reqStr(sm, "cellId"),
                        Json.str(sm, "region", null)));
            }
            out.put(Long.parseLong(e.getKey()), new Ring.Epoch(ring, status));
        }
        return out;
    }
}
