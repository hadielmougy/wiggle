package com.wiggle.core;

/**
 * The server's reply to an observed run report: the instance the run maps to (minted on the
 * first report), its status after the steps were applied, and how many anomalies this report
 * added.
 */
public record ObserveResult(String instanceId, String instanceStatus, int anomalies) {

    public boolean running() {
        return InstanceStatus.runningByName(instanceStatus);
    }
}
