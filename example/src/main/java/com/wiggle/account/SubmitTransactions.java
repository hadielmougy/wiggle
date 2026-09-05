package com.wiggle.account;

import com.wiggle.client.WiggleConnection;

public class SubmitTransactions {

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("WIGGLE_URL", "localhost:8080");
        try (WiggleConnection wiggle = WiggleConnection.direct(url)) {
            var client = wiggle.client();
            client.register(TransactionWorkflow.blueprint());

            Transaction trx = new Transaction("from", "to");
            client.start(TransactionWorkflow.blueprint(), trx);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
