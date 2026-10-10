package com.wiggle.relay;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Step;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One relay stage in its own process: {@code RelayWorker <stage> [host:port]}. It polls only its
 * stage's queue, rebuilds the input that stage must receive from the shipment id, compares it field
 * by field, and fails the instance permanently on any difference.
 */
public final class RelayWorker {

    private static final AtomicLong CHECKED = new AtomicLong();
    private static final AtomicLong FIELDS = new AtomicLong();
    private static final AtomicLong MISMATCHED = new AtomicLong();

    private RelayWorker() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || !Relay.STAGES.contains(args[0])) {
            System.err.println("usage: RelayWorker <" + String.join("|", Relay.STAGES) + "> [host:port]");
            System.exit(2);
        }
        String stage = args[0];
        String target = args.length > 1 ? args[1] : "127.0.0.1:18600";
        String workerId = stage + "-" + ManagementFactory.getRuntimeMXBean().getPid();

        WiggleClient client = new WiggleClient(target);
        Worker worker = new Worker(client, workerId, WorkerOptions.defaults()
                .withConcurrency(32)
                .withAwaitRegistration(Duration.ofMinutes(10)));
        worker.registerHandler(handlerFor(stage)).start();
        log(workerId, "serving stage " + (Relay.STAGES.indexOf(stage) + 1) + "/" + Relay.STAGES.size()
                + " '" + stage + "' on queue '" + stage + "' via " + target);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            worker.close();
            client.close();
            log(workerId, "stopped: " + summary());
        }));

        String last = "";
        while (true) {
            Thread.sleep(5_000);
            String now = summary();
            if (!now.equals(last)) log(workerId, now);
            last = now;
        }
    }

    private static Object handlerFor(String stage) {
        return switch (stage) {
            case "intake" -> new Intake();
            case "enrich" -> new Enrich();
            case "price" -> new Price();
            case "reserve" -> new Reserve();
            case "dispatch" -> new Dispatch();
            case "settle" -> new Settle();
            default -> throw new IllegalArgumentException(stage);
        };
    }

    /** Checks {@code in} against the shipment after the stages before this one, then returns this stage's output. */
    static Map<String, Object> run(String stage, Map<String, Object> in) {
        int index = Relay.STAGES.indexOf(stage);
        if (!(in.get("id") instanceof String id)) {
            MISMATCHED.incrementAndGet();
            throw new PermanentActivityException(stage + ": input has no string id: " + in.get("id"));
        }
        Relay.Counter fields = new Relay.Counter();
        List<String> diff = Relay.diff(Relay.json(Relay.after(id, index)), in, fields);
        CHECKED.incrementAndGet();
        FIELDS.addAndGet(fields.leaves);
        if (!diff.isEmpty()) {
            MISMATCHED.incrementAndGet();
            String report = stage + " input for " + id + " (instance " + Step.instanceId() + ", attempt "
                    + Step.attempt() + ") differs in " + diff.size() + " of " + fields.leaves + " fields:\n  "
                    + String.join("\n  ", diff);
            System.out.println("MISMATCH " + report);
            throw new PermanentActivityException(report);
        }
        return Relay.json(Relay.after(id, index + 1));
    }

    private static String summary() {
        return "checked=" + CHECKED.get() + " fields=" + FIELDS.get() + " mismatched=" + MISMATCHED.get();
    }

    private static void log(String workerId, String message) {
        System.out.println(LocalTime.now().truncatedTo(ChronoUnit.SECONDS) + " [" + workerId + "] " + message);
    }

    @ForFlow(Relay.NAME)
    public static final class Intake {
        public Map<String, Object> intake(Map<String, Object> in) { return run("intake", in); }
    }

    @ForFlow(Relay.NAME)
    public static final class Enrich {
        public Map<String, Object> enrich(Map<String, Object> in) { return run("enrich", in); }
    }

    @ForFlow(Relay.NAME)
    public static final class Price {
        public Map<String, Object> price(Map<String, Object> in) { return run("price", in); }
    }

    @ForFlow(Relay.NAME)
    public static final class Reserve {
        public Map<String, Object> reserve(Map<String, Object> in) { return run("reserve", in); }
    }

    @ForFlow(Relay.NAME)
    public static final class Dispatch {
        public Map<String, Object> dispatch(Map<String, Object> in) { return run("dispatch", in); }
    }

    @ForFlow(Relay.NAME)
    public static final class Settle {
        public Map<String, Object> settle(Map<String, Object> in) { return run("settle", in); }
    }
}
