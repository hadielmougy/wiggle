package dev.wiggle.account;

import dev.wiggle.client.worker.WiggleClient;

public class SubmitTransactions {

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("WIGGLE_URL", "localhost:8080");
        try (WiggleClient client = new WiggleClient(url)) {
            client.register(TransactionWorkflow.blueprint());

            for (int i = 1; i <= 1000; i++) {
                Transaction trx = new Transaction("from", "to");
                client.start(TransactionWorkflow.blueprint(), trx);
                Thread.sleep(50);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
