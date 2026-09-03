package com.wiggle.account;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

public class TransactionWorkflow {

    public static Blueprint blueprint() {
        return Workflow.define("accounts-workflow", RetryPolicy.fixed(100, Duration.ofSeconds(1)))
                .step("make-withdraw", Transaction.class, Transaction::withdraw)
                .step("make-deposit", Transaction.class, Transaction::deposit)
                .build();
    }
}
