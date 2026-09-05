package com.wiggle.order;

import com.wiggle.client.WiggleConnection;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;

import java.time.Duration;

/**
 * A standalone worker process. Run as many as you like against the same server; each
 * one pulls only as much work as it has free capacity.
 *
 * <p>Connecting starts from {@link WiggleConnection} -- the single entry point: {@code direct(url)} for a
 * standalone server (here), {@code coordinator(...)} for a sharded namespace (see
 * {@link NamespaceWorkerMain}). Swapping the factory is the only change to go distributed.
 */
public final class WorkerMain {

    public static void main(String[] args) throws Exception {
        String url = env("WIGGLE_URL", "localhost:8080");
        String id = env("WIGGLE_WORKER_ID", "worker-" + ProcessHandle.current().pid());
        int concurrency = Integer.parseInt(env("WIGGLE_WORKER_CONCURRENCY", "8"));
        int localBatch = Integer.parseInt(env("WIGGLE_LOCAL_BATCH_SIZE", "64"));   // LOCAL_ASYNC batch size

        var wiggle = WiggleConnection.direct(url);
        Blueprint blueprint = OrderFulfilment.blueprint();

        Worker worker = new Worker(wiggle.client(), id, WorkerOptions.defaults()
                        .withConcurrency(concurrency)
                        .withLocalBatchSize(localBatch)
                        .withLongPollWait(Duration.ofSeconds(10)))
                .register(blueprint)
                .handlers(new OrderHandlers());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.close();
            wiggle.close();
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
