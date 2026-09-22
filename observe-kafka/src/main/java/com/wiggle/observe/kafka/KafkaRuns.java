package com.wiggle.observe.kafka;

import com.wiggle.observe.Observation;
import com.wiggle.observe.Observed;
import com.wiggle.observe.Run;
import com.wiggle.observe.RunContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Carries an observed run across Kafka: the producing side stamps the run's context on the
 * record as headers, the consuming side joins it before running the handler. Framework-free --
 * a Spring Kafka or Quarkus listener calls the same two methods -- and broker-free to test, since
 * only the record types are touched.
 *
 * <pre>{@code
 * // producing, inside an observed step or any code with a run open on the thread
 * producer.send(KafkaRuns.inject(new ProducerRecord<>("orders", key, payload)));
 *
 * // consuming
 * for (ConsumerRecord<String, Order> r : consumer.poll(timeout)) {
 *     KafkaRuns.handle(checkout, r, rec -> steps.reserve(rec.value()));
 * }
 * }</pre>
 */
public final class KafkaRuns {

    private KafkaRuns() {}

    /** Stamps the current thread's run on the record. Throws when no run is open; see {@link #inject(ProducerRecord, Run)}. */
    public static <K, V> ProducerRecord<K, V> inject(ProducerRecord<K, V> record) {
        return inject(record, Observation.run());
    }

    /** Stamps {@code run} on the record, replacing any run headers it already carries. */
    public static <K, V> ProducerRecord<K, V> inject(ProducerRecord<K, V> record, Run run) {
        write(record.headers(), run.context());
        return record;
    }

    /** Writes a context onto any Kafka headers, replacing what was there. */
    public static void write(Headers headers, RunContext context) {
        context.toHeaders().forEach((k, v) -> {
            headers.remove(k);
            headers.add(k, v.getBytes(StandardCharsets.UTF_8));
        });
    }

    /** The run context a record carries, or null when it has none. */
    public static RunContext read(ConsumerRecord<?, ?> record) {
        return read(record.headers());
    }

    public static RunContext read(Headers headers) {
        Map<String, String> h = new LinkedHashMap<>();
        for (Header header : headers) {
            if (header.value() != null) h.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return RunContext.fromHeaders(h);
    }

    /**
     * Joins the run a record belongs to, on this thread, for the handler's scope. A record with no
     * run headers still gets a run: keyed by the record's key when it has one, so a platform that
     * never adopted correlation ids is observed per message; otherwise a minted key.
     */
    public static Run join(Observed<?> flow, ConsumerRecord<?, ?> record) {
        RunContext ctx = read(record);
        if (ctx != null) return flow.join(ctx);
        Object key = record.key();
        return flow.join(key == null ? "" + record.topic() + "/" + record.partition() + "/" + record.offset()
                : String.valueOf(key));
    }

    /** {@link #join} around {@code handler}: opens the run, runs the handler, closes the run. */
    public static <K, V> void handle(Observed<?> flow, ConsumerRecord<K, V> record, Consumer<ConsumerRecord<K, V>> handler) {
        try (Run run = join(flow, record)) {
            handler.accept(record);
        }
    }
}
