package com.wiggle.account;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.ContextCodec;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

public class TransactionWorkflow {

    public static Blueprint<Transaction> blueprint() {
        return Workflow.define("accounts-workflow", ContextCodec.records(Transaction.class), RetryPolicy.fixed(100, Duration.ofSeconds(1)))
                .step("make-withdraw", Transaction::withdraw)
                .step("make-deposit", Transaction::deposit)
                .build();
    }
}
