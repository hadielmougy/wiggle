package com.wiggle.order;

import com.wiggle.client.worker.Handlers;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Step logic for the {@code onboarding} workflow seeded by {@link DashboardSeed}. The {@code merge}
 * combine has no method here, so its two branches ({@code send-welcome}, {@code provision}) fold with
 * the default union.
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
