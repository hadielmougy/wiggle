package com.wiggle.observe;

/** One completed call on an observed step, as the proxy recorded it. */
record StepRecord(String nodeId, Object merge, Boolean predicateValue, String error,
                  long startedAt, long finishedAt) {
}
