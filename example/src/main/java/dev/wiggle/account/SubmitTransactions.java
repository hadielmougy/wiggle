package dev.wiggle.account;

import dev.wiggle.client.worker.WiggleClient;

public class SubmitTransactions {

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("WIGGLE_URL", "localhost:8080");
        try (WiggleClient client = new WiggleClient(url)) {
            client.register(TransactionWorkflow.blueprint());

            Transaction trx = new Transaction("from", "to");
            client.start(TransactionWorkflow.blueprint(), trx);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
