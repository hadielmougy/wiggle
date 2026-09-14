package com.wiggle.account;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

public class TransactionWorkflow {

    interface AccountSteps {
        Transaction makeWithdraw(Transaction trx);
        Transaction makeDeposit(Transaction trx);
    }


    public static FlowSpec flowSpec() {
        return FlowSpec.define("accounts-workflow", RetryPolicy.fixed(100, Duration.ofSeconds(1)),
                Transaction.class, AccountSteps.class, (f, s) -> f
                        .thenApply(s::makeWithdraw)
                        .thenApply(s::makeDeposit));
    }
}
