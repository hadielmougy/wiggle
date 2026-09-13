package com.wiggle.account;

import com.wiggle.client.dsl.FlowSpec;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

public class TransactionWorkflow {

    public static FlowSpec flowSpec() {
        return Workflow.define("accounts-workflow", RetryPolicy.fixed(100, Duration.ofSeconds(1)))
                .step("make-withdraw")
                .step("make-deposit")
                .build();
    }
}
