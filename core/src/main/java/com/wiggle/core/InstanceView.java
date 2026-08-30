package com.wiggle.core;

import java.util.Map;

public record InstanceView(String id, String workflow, int version, String status,
                           String terminationReason, String error, Object context,
                           long createdAt, long updatedAt) {

    public boolean isTerminal() {
        return !"RUNNING".equals(status);
    }

    public static InstanceView fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        return new InstanceView(Json.reqStr(m, "id"), Json.reqStr(m, "workflow"),
                (int) Json.num(m, "version", 0), Json.reqStr(m, "status"),
                Json.str(m, "terminationReason", null), Json.str(m, "error", null),
                m.get("context"), Json.num(m, "createdAt", 0), Json.num(m, "updatedAt", 0));
    }
}
