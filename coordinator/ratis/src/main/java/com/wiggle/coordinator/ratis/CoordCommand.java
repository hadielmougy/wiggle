package com.wiggle.coordinator.ratis;

import com.wiggle.core.Json;

import java.util.Map;

/**
 * DESIGN SKETCH — not wired into the build. The replicated command/query vocabulary for the Ratis
 * coordinator state machine. Each mutating {@link com.wiggle.server.coord.CoordinatorStore} op becomes
 * one {@code Op}; the args travel as a JSON map so apply is a pure function of {@code (RocksDB, args)}.
 *
 * <p>Every arg that could be non-deterministic (timestamps) is supplied by the caller, never read inside
 * {@link CoordStateMachine#applyTransaction}. See docs/coordinator-ratis-rocksdb.md §1.
 */
public record CoordCommand(Op op, Map<String, Object> args) {

    public enum Op {
        // writes (go through the Raft log)
        CAS_POLICY, UPSERT_NODE, TOUCH_NODE, REMOVE_NODE, EXPIRE_NODES,
        BIND_CELL, PRUNE_ORPHAN_BINDINGS, PUT_DEFINITION, REMOVE_DEFINITION,
        PUT_NAMESPACE, ACQUIRE_LEADERSHIP, RELEASE_LEADERSHIP,
        // reads (may be served by a linearizable query instead of the log)
        GET_POLICY, LIST_POLICIES, GET_NODE, LIST_NODES, GET_DEFINITION,
        LIST_DEFINITIONS, GET_NAMESPACE, LIST_NAMESPACES
    }

    public byte[] encode() {
        return Json.write(Map.of("op", op.name(), "args", args)).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    public static CoordCommand decode(byte[] bytes) {
        Map<String, Object> m = Json.parseObject(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        @SuppressWarnings("unchecked")
        Map<String, Object> args = (Map<String, Object>) m.getOrDefault("args", Map.of());
        return new CoordCommand(Op.valueOf((String) m.get("op")), args);
    }

    /** Result of an applied command / query, encoded back to the caller over Ratis. */
    public record Result(boolean ok, Object value) {
        public byte[] encode() {
            return Json.write(Map.of("ok", ok, "value", value == null ? "" : value))
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        public static Result decode(byte[] bytes) {
            Map<String, Object> m = Json.parseObject(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
            return new Result(Boolean.TRUE.equals(m.get("ok")), m.get("value"));
        }
    }
}
