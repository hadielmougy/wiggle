package com.wiggle.account;

import com.wiggle.client.worker.ForFlow;

/**
 * Step logic for {@link TransactionWorkflow}, bound on a worker by name. Each method's name matches a
 * step (case/style-insensitive, so {@code makeWithdraw} serves {@code make-withdraw}) and its
 * signature -- a {@link Transaction} in and out -- makes it a task.
 */
@ForFlow("accounts-workflow")
public final class AccountHandlers {

    public Transaction makeWithdraw(Transaction trx) {
        return trx.withdraw();
    }

    public Transaction makeDeposit(Transaction trx) {
        return trx.deposit();
    }
}
