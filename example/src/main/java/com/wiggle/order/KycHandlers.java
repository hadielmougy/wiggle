package com.wiggle.order;

import com.wiggle.client.worker.Handlers;

import java.util.LinkedHashMap;
import java.util.Map;

/** Step logic for the {@code kyc-checks} workflow seeded by {@link DashboardSeed}. */
@Handlers("kyc-checks")
public final class KycHandlers {

    public Map<String, Object> verifyId(Map<String, Object> ctx) {
        return put(ctx, "idOk", true);
    }

    public Map<String, Object> riskScore(Map<String, Object> ctx) {
        return put(ctx, "risk", 12);
    }

    private static Map<String, Object> put(Map<String, Object> c, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(c);
        n.put(k, v);
        return n;
    }
}
