package com.wiggle.order;

import com.wiggle.client.worker.Handlers;

import java.util.LinkedHashMap;
import java.util.Map;

/** Step logic for the {@code nightly-report} workflow seeded by {@link DashboardSeed}. */
@Handlers("nightly-report")
public final class NightlyReportHandlers {

    public Map<String, Object> gather(Map<String, Object> ctx) {
        return put(ctx, "rows", 128);
    }

    public Map<String, Object> render(Map<String, Object> ctx) {
        return put(ctx, "done", true);
    }

    private static Map<String, Object> put(Map<String, Object> c, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(c);
        n.put(k, v);
        return n;
    }
}
