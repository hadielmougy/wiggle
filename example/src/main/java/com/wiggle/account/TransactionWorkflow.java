package com.wiggle.account;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

public class TransactionWorkflow {

    public static FlowSpec flowSpec() {
        return Wiggle.graph("accounts-workflow", RetryPolicy.fixed(100, Duration.ofSeconds(1)))
                .step("make-withdraw")
                .step("make-deposit")
                .build();
    }
}
