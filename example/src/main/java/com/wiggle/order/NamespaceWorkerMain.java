package com.wiggle.order;

import com.wiggle.client.CellResolver;
import com.wiggle.core.Tls;
import com.wiggle.client.worker.NamespaceWorker;

/**
 * A coordinator-routed worker. Unlike {@link WorkerMain}, which binds to a single server,
 * this resolves the active cells of a namespace through the coordinator and runs one worker
 * per cell, reconciling as cells come and go. The OrderFulfilment blueprint (its class
 * handlers) is bound to every per-cell worker via the configurator.
 */
public final class NamespaceWorkerMain {

    public static void main(String[] args) throws Exception {
        String coord = env("WIGGLE_COORDINATOR_URL", "127.0.0.1:18099");
        String ns = env("WIGGLE_NAMESPACE", "abc");
        String id = env("WIGGLE_WORKER_ID", "order-fulfilment");

        CellResolver resolver = CellResolver.coordinator(coord, Tls.Options.DISABLED, "eu");
        // Reach in-cluster cells from the host via WIGGLE_ENDPOINT_REWRITE (each cell's pod IP -> its
        // port-forward), which CellResolver reads from the env; the lab's Forwards tab generates it.
        NamespaceWorker worker = new NamespaceWorker(resolver, ns, id,
                w -> w.register(OrderFulfilment.blueprint()).handlers(new OrderHandlers())).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.close();
            resolver.close();
        }));

        System.out.println("namespace worker " + id + " bound " + OrderFulfilment.blueprint().name()
                + " for namespace " + ns + " via coordinator " + coord);
        Thread.currentThread().join();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
