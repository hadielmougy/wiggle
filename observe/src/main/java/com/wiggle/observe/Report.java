package com.wiggle.observe;

/** One report: a step of a run, or the run's end. Built by {@link ObservedFlow}, sent by {@link Reporter}. */
record Report(String workflow, int version, String key, String nodeId, Boolean predicateValue, String error,
              long startedAt, long finishedAt, boolean fin) {

    static Report end(String workflow, int version, String key) {
        return new Report(workflow, version, key, null, null, null, 0, 0, true);
    }
}
