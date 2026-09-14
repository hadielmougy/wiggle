package com.wiggle.greet;

import com.wiggle.client.WiggleConnection;
import com.wiggle.client.worker.NamespaceWorker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.Tls;

import java.time.Duration;

/**
 * A coordinator-aware worker for one namespace. It resolves the namespace's active cells through the
 * coordinator and runs a {@link com.wiggle.client.worker.Worker} against each -- adding cells as they
 * open and dropping them as they retire -- so it keeps serving across a reshard without reconfiguration.
 *
 * <pre>
 *   WIGGLE_COORDINATOR_URL=127.0.0.1:8099 WIGGLE_NAMESPACE=ns1 \
 *     java -cp 'example/build/install/example/lib/*' com.wiggle.greet.GreetWorker
 * </pre>
 */
public final class GreetWorker {

    public static void main(String[] args) throws Exception {
        String coord = env("WIGGLE_COORDINATOR_URL", "127.0.0.1:8099");
        String ns = env("WIGGLE_NAMESPACE", "ns1");
        String id = env("WIGGLE_WORKER_ID", "greet-worker-" + ProcessHandle.current().pid());

        var resolver = WiggleConnection.coordinator(coord, Tls.Options.DISABLED, null);
        // The author (GreetStart) registers the topology; this worker only implements the steps.
        // It may well start first, so give it a window to wait for that registration.
        NamespaceWorker worker = new NamespaceWorker(resolver, ns, id,
                WorkerOptions.defaults().withAwaitRegistration(Duration.ofMinutes(5)),
                w -> w.handlers(new GreetHandlers())).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> { worker.close(); resolver.close(); }));
        System.out.println("greet worker '" + id + "' serving namespace '" + ns + "' via coordinator " + coord);
        Thread.currentThread().join();
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
