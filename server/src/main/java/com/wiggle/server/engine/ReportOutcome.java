package com.wiggle.server.engine;

import com.wiggle.server.store.Rows.Instance;

public record ReportOutcome(String instanceStatus, long leaseExpiresAt, String nextTaskId) {

    /** The answer to a report against an instance that is no longer running: its status, so the
     *  worker stops, with nothing leased back and nothing written. */
    static ReportOutcome stopped(Instance inst) {
        return new ReportOutcome(inst.status.name(), 0, null);
    }
}
