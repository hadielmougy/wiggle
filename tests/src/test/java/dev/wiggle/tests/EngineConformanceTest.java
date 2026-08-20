package dev.wiggle.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * JUnit wrapper over {@link Scenarios}. The scenarios themselves are framework-free so
 * they can also be run with {@code ./gradlew :tests:run} on a machine with no
 * dependency resolution available; this class exists so they participate in
 * {@code ./gradlew check} like any other test suite.
 */
class EngineConformanceTest {

    @Test @DisplayName("records round-trip through JSON without losing types")
    void recordCodecRoundTrip() {
        Scenarios.recordCodecRoundTrip();
    }

    @Test @DisplayName("definition versions are content-addressed")
    void definitionVersionIsContentAddressed() {
        Scenarios.definitionVersionIsContentAddressed();
    }

    @Test @DisplayName("the DSL rejects graphs that could not run")
    void dslRejectsInvalidGraphs() {
        Scenarios.dslRejectsInvalidGraphs();
    }

    @Test @DisplayName("a linear pipeline runs its steps in order")
    void sequentialPipeline() throws Exception {
        Scenarios.sequentialPipeline();
    }

    @Test @DisplayName("a false filter short-circuits the pipeline")
    void filterShortCircuits() throws Exception {
        Scenarios.filterShortCircuits();
    }

    @Test @DisplayName("parallel branches merge disjoint writes")
    void forkMergesDisjointWrites() throws Exception {
        Scenarios.forkMergesDisjointWrites();
    }

    @Test @DisplayName("the step after a join runs exactly once")
    void joinRunsContinuationOnce() throws Exception {
        Scenarios.joinRunsContinuationOnce();
    }

    @Test @DisplayName("forks nest correctly")
    void nestedForks() throws Exception {
        Scenarios.nestedForks();
    }

    @Test @DisplayName("a filter inside a branch does not strand its siblings")
    void filterInsideBranchDoesNotStrandSiblings() throws Exception {
        Scenarios.filterInsideBranchDoesNotStrandSiblings();
    }

    @Test @DisplayName("transient failures are retried")
    void retriesTransientFailures() throws Exception {
        Scenarios.retriesTransientFailures();
    }

    @Test @DisplayName("exhausted retries fail the instance")
    void exhaustedRetriesFailInstance() throws Exception {
        Scenarios.exhaustedRetriesFailInstance();
    }

    @Test @DisplayName("a permanent failure skips retries")
    void permanentFailureSkipsRetries() throws Exception {
        Scenarios.permanentFailureSkipsRetries();
    }

    @Test @DisplayName("a sleep defers without holding a worker slot")
    void sleepDefersWithoutHoldingAWorker() throws Exception {
        Scenarios.sleepDefersWithoutHoldingAWorker();
    }

    @Test @DisplayName("an expired lease is reclaimed and redelivered")
    void expiredLeaseIsReclaimed() throws Exception {
        Scenarios.expiredLeaseIsReclaimed();
    }

    @Test @DisplayName("completing a task without its lease is rejected")
    void staleLeaseIsRejected() throws Exception {
        Scenarios.staleLeaseIsRejected();
    }

    @Test @DisplayName("cancelling stops an instance")
    void cancelStopsAnInstance() throws Exception {
        Scenarios.cancelStopsAnInstance();
    }

    @Test @DisplayName("a heartbeat keeps a long task's lease alive so it runs once")
    void heartbeatKeepsLongTaskAlive() throws Exception {
        Scenarios.heartbeatKeepsLongTaskAlive();
    }

    @Test @DisplayName("discovery returns every live node's dialable address")
    void discoveryReturnsLiveServers() throws Exception {
        Scenarios.discoveryReturnsLiveServers();
    }

    @Test @DisplayName("backpressure holds when polling several servers round-robin")
    void backpressureHoldsAcrossServers() throws Exception {
        Scenarios.backpressureHoldsAcrossServers();
    }

    @Test @DisplayName("a worker seeded with one node drains the whole cluster")
    void workerSeededWithOneNodeDrainsCluster() throws Exception {
        Scenarios.workerSeededWithOneNodeDrainsCluster();
    }

    @Test @DisplayName("an unreachable seed falls through to a reachable node")
    void seedFailoverBootstrapsFromReachableNode() throws Exception {
        Scenarios.seedFailoverBootstrapsFromReachableNode();
    }

    @Test @DisplayName("exactly one leader is elected, and failover works")
    void leaderElectionAndFailover() {
        Scenarios.leaderElectionAndFailover();
    }

    @Test @DisplayName("work is distributed across workers, never duplicated")
    void workDistributesAcrossWorkers() throws Exception {
        Scenarios.workDistributesAcrossWorkers();
    }
}
