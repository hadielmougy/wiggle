package dev.wiggle.account;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.client.worker.WorkerOptions;
import dev.wiggle.order.Order;
import dev.wiggle.order.OrderFulfilment;

import java.time.Duration;

public class TransactionWorker {

    public static void main(String[] args) throws InterruptedException {
        String url = env("WIGGLE_URL", "localhost:8080");
        String id = env("WIGGLE_WORKER_ID", "worker-" + ProcessHandle.current().pid());
        int concurrency = Integer.parseInt(env("WIGGLE_WORKER_CONCURRENCY", "8"));

        WiggleClient client = new WiggleClient(url);

        Blueprint<Transaction> blueprint = TransactionWorkflow.blueprint();

        Worker worker = new Worker(client, id, WorkerOptions.defaults()
                .withConcurrency(concurrency)
                .withLongPollWait(Duration.ofSeconds(10)))
                .register(blueprint);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.close();
            client.close();
        }));

        worker.start();
        System.out.println("worker " + id + " registered " + blueprint.name() + " v" + blueprint.version()
                + " against " + url + " (concurrency " + concurrency + ")");
        Thread.currentThread().join();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
