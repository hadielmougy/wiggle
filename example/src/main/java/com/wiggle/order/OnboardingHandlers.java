package com.wiggle.order;

import com.wiggle.client.worker.Arm;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.Handlers;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Step logic for the {@code onboarding} workflow seeded by {@link DashboardSeed}. The {@code merge}
 * method is the combine for the {@code send-welcome} / {@code provision} fork — explicit, returning
 * the complete post-join context.
 */
@Handlers("onboarding")
public final class OnboardingHandlers {

    public Map<String, Object> createAccount(Map<String, Object> ctx) {
        return put(ctx, "accountId", "acc-42");
    }

    public Map<String, Object> welcome(Map<String, Object> ctx) {
        return put(ctx, "welcomed", true);
    }

    public Map<String, Object> provisionHw(Map<String, Object> ctx) {
        return put(ctx, "provisioned", true);
    }

    public Map<String, Object> merge(@Context Map<String, Object> base,
                                     @Arm("send-welcome") Map<String, Object> welcome,
                                     @Arm("provision") Map<String, Object> provision) {
        Map<String, Object> out = new LinkedHashMap<>(base);
        if (welcome != null) out.putAll(welcome);
        if (provision != null) out.putAll(provision);
        return out;
    }

    public Map<String, Object> autoEscalate(Map<String, Object> ctx) {
        return put(ctx, "escalated", true);
    }

    public Map<String, Object> activate(Map<String, Object> ctx) {
        return put(ctx, "active", true);
    }

    private static Map<String, Object> put(Map<String, Object> c, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(c);
        n.put(k, v);
        return n;
    }
}
