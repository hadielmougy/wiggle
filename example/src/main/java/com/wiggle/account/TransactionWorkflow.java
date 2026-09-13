package com.wiggle.account;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.Map;

public class TransactionWorkflow {

    public static FlowSpec flowSpec() {
        return Wiggle.define("accounts-workflow", RetryPolicy.fixed(100, Duration.ofSeconds(1)),
                Map.class, f -> f
                        .thenApply("make-withdraw")
                        .thenApply("make-deposit"));
    }
}
