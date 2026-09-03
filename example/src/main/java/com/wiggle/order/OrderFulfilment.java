package com.wiggle.order;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

/**
 * The workflow <em>topology</em>: named steps and how they fork and rejoin. It compiles to a graph
 * the server drives; the step logic lives in {@link OrderHandlers}, bound on the worker by name.
 */
public final class OrderFulfilment {

    private OrderFulfilment() {}

    /**
     * Execution mode for benchmarking, from {@code WIGGLE_EXECUTION_MODE} (default SERVER). Set it
     * identically on the worker and submitter JVMs so they compile the same version (the mode is
     * part of the content hash).
     */
    private static ExecutionMode mode() {
        String v = System.getenv("WIGGLE_EXECUTION_MODE");
        return v == null || v.isBlank() ? ExecutionMode.SERVER : ExecutionMode.valueOf(v.trim());
    }

    public static Blueprint blueprint() {
        return Workflow.define("order-fulfilment").execution(ExecutionMode.SERVER)

                .step("validate")

                // A false guard ends the instance successfully, like an empty stream.
                .gate("in-stock")

                .fork(
                        Branch.of("payment", s -> s
                                // A flaky downstream: the retry policy rides on the topology node.
                                .step("authorise", RetryPolicy.exponential(5, Duration.ofMillis(100)))
                                .step("capture")),

                        Branch.of("shipping", s -> s
                                .step("reserve-stock")
                                // A server-side timer: no worker is held while we wait.
                                .sleep("await-warehouse", Duration.ofMillis(100))
                                .step("print-label")))

                // The payment and shipping arms change disjoint fields, so the default union folds
                // them (no combine handler needed).
                .combine("merge")

                .step("notify")
                .effect("audit")

                .build();
    }
}
