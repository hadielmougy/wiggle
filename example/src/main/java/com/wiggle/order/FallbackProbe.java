package com.wiggle.order;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;

import java.util.Arrays;
import java.util.Map;

/**
 * Measures the CROSS-NODE dispatch latency of a multi-node cluster: instances are started via one
 * node ({@code WIGGLE_SUBMIT_URL}) while the only worker long-polls a different node
 * ({@code WIGGLE_WORKER_URL}) of the same cluster. Wake-on-produce is node-local, so the parked
 * poll discovers the work only via the fallback re-claim — the path the adaptive fallback ramp
 * ({@code WIGGLE_ADAPTIVE_FALLBACK_POLL=true} on the server) is meant to shorten.
 *
 * <p>Probes run sequentially (one in flight), each a single-step workflow; sojourn is measured
 * start() → COMPLETED with a tight 3ms status poll against the submit node. Reports p50/p90/p99.
 *
 * <pre>
 *   WIGGLE_SUBMIT_URL=127.0.0.1:18100 WIGGLE_WORKER_URL=127.0.0.1:18102 \
 *     ./gradlew :example:fallbackProbe
 * </pre>
 */
public final class FallbackProbe {

    public static void main(String[] args) throws Exception {
        String submitUrl = env("WIGGLE_SUBMIT_URL", "127.0.0.1:18100");
        String workerUrl = env("WIGGLE_WORKER_URL", "127.0.0.1:18102");
        int probes = Integer.parseInt(env("WIGGLE_BENCH_COUNT", "200"));
        int warmup = Integer.parseInt(env("WIGGLE_BENCH_WARMUP", "20"));

        Blueprint bp = Workflow.define("fallback-probe").step("ping").build();

        try (WiggleClient submit = new WiggleClient(submitUrl);
             WiggleClient workerClient = new WiggleClient(workerUrl)) {
            submit.register(bp);
            try (Worker worker = new Worker(workerClient, "probe-worker",
                    WorkerOptions.defaults().withConcurrency(4))
                    .register(bp).handlers(new ProbeHandlers())) {
                worker.start();
                Thread.sleep(1000);   // let the worker park its long-poll

                System.out.printf("fallback probe: submit=%s worker=%s probes=%d (+%d warmup)%n",
                        submitUrl, workerUrl, probes, warmup);
                for (int i = 0; i < warmup; i++) probe(submit, bp);

                long[] sojourn = new long[probes];
                for (int i = 0; i < probes; i++) sojourn[i] = probe(submit, bp);

                Arrays.sort(sojourn);
                double mean = Arrays.stream(sojourn).average().orElse(0);
                System.out.printf(
                        "fallback probe: p50=%dms p90=%dms p99=%dms mean=%.1fms (n=%d)%n",
                        sojourn[probes / 2], sojourn[(int) (probes * 0.90)],
                        sojourn[(int) (probes * 0.99)], mean, probes);
            }
        }
    }

    /** One sequential probe: start via the submit node, tight-poll it to a terminal state. */
    private static long probe(WiggleClient submit, Blueprint bp) throws InterruptedException {
        long t0 = System.nanoTime();
        String id = submit.start(bp, Map.of("t", t0));
        long deadline = t0 + 15_000_000_000L;
        while (true) {
            InstanceView v = submit.instance(id);
            if (v.isTerminal()) return (System.nanoTime() - t0) / 1_000_000;
            if (System.nanoTime() > deadline) throw new IllegalStateException("probe stuck: " + id);
            Thread.sleep(3);
        }
    }

    @Handlers("fallback-probe")
    public static final class ProbeHandlers {
        public Map<String, Object> ping(Map<String, Object> ctx) { return ctx; }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private FallbackProbe() {}
}
