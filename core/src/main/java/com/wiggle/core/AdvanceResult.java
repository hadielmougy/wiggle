package com.wiggle.core;

/**
 * The server's reply to a locally-executed run: the instance's status (the worker stops if it is
 * not {@code RUNNING}), the renewed lease expiry, and the token to reference for the next local
 * step ({@code null} once control has been handed back).
 */
public record AdvanceResult(String instanceStatus, long leaseExpiresAt, String nextTaskId) {

    public boolean running() {
        return "RUNNING".equals(instanceStatus);
    }
}
