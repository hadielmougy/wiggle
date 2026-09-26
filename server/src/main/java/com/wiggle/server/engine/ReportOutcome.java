package com.wiggle.server.engine;

public record ReportOutcome(String instanceStatus, long leaseExpiresAt, String nextTaskId) {}
