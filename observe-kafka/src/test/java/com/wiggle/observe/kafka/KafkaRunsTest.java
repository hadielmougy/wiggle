package com.wiggle.observe.kafka;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.InstanceView;
import com.wiggle.observe.Observation;
import com.wiggle.observe.Observed;
import com.wiggle.observe.Observer;
import com.wiggle.observe.ObserverOptions;
import com.wiggle.observe.Run;
import com.wiggle.observe.RunContext;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A run crossing Kafka: the producing service stamps its run on the record, the consuming service
 * joins it, and the server sees one instance with both reporters. The broker is a MockProducer and
 * hand-built records: what travels is the headers, and those are all this adapter touches.
 */
class KafkaRunsTest {

    static {
        System.setProperty("wiggle.observe.settleMillis", "100");
    }

    interface Steps {
        Map<String, Object> accept(Map<String, Object> order);
        Map<String, Object> reserve(Map<String, Object> order);
        Map<String, Object> ship(Map<String, Object> order);
    }

    static final class Impl implements Steps {
        public Map<String, Object> accept(Map<String, Object> o) { return o; }
        public Map<String, Object> reserve(Map<String, Object> o) { return o; }
        public Map<String, Object> ship(Map<String, Object> o) { return o; }
    }

    private static FlowSpec spec(String name) {
        return FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> f
                .thenApply(s::accept).thenApply(s::reserve).thenApply(s::ship));
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "kafka-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static <T> T await(Supplier<T> probe) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            T v = probe.get();
            if (v != null) return v;
            if (System.currentTimeMillis() > deadline) fail("nothing landed in time");
            Thread.sleep(20);
        }
    }

    /** What a consumer would poll: the produced record's headers on a consumer record. */
    private static ConsumerRecord<String, String> delivered(ProducerRecord<String, String> sent, long offset) {
        return new ConsumerRecord<>(sent.topic(), 0, offset, -1L, null, -1, -1, sent.key(), sent.value(),
                new RecordHeaders(sent.headers()), java.util.Optional.empty());
    }

    @Test @DisplayName("a run stamped on a produced record is joined by the consumer and completes as one instance")
    void runCrossesKafka() throws Exception {
        FlowSpec spec = spec("kafka-orders");
        MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer gateway = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("gateway"));
             Observer warehouse = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("warehouse"))) {
            Observed<Steps> front = gateway.observe(spec, Steps.class, new Impl());
            Observed<Steps> back = warehouse.observe(spec, Steps.class, new Impl());

            // gateway: accept the order, then publish it with the run on the record
            try (Run run = front.begin("order-11")) {
                front.steps().accept(Map.of("id", "order-11"));
                producer.send(KafkaRuns.inject(new ProducerRecord<>("orders", "order-11", "{}")));
            }
            ProducerRecord<String, String> sent = producer.history().getFirst();
            assertEquals("order-11", new String(sent.headers().lastHeader(RunContext.RUN_HEADER).value(), StandardCharsets.UTF_8));
            assertEquals(spec.name(), new String(sent.headers().lastHeader(RunContext.WORKFLOW_HEADER).value(), StandardCharsets.UTF_8));

            // warehouse: consume, join, run its steps inside the handler
            ConsumerRecord<String, String> record = delivered(sent, 0);
            RunContext carried = KafkaRuns.read(record);
            assertEquals(new RunContext(spec.name(), 1, "order-11", spec.definition().startNode()), carried,
                    "the record names accept, the sender's last completed step, as the cause of what follows");
            KafkaRuns.handle(back, record, r -> {
                assertEquals("order-11", Observation.correlationId(), "the handler runs inside the joined run");
                back.steps().reserve(Map.of());
                back.steps().ship(Map.of());
            });
            assertNull(back.current(), "handle closed the run");

            InstanceView v = await(() -> server.engine().findByCorrelation("order-11", 1).stream()
                    .filter(InstanceView::isTerminal).findFirst().orElse(null));
            assertEquals("COMPLETED", v.status());
            assertEquals(2, server.engine().tokens(v.id()).stream().map(t -> t.leaseOwner)
                    .filter(o -> o != null).distinct().count(), "both services on one instance");
            assertTrue(server.engine().anomalies(null, v.id(), 10).isEmpty());
        }
    }

    @Test @DisplayName("a record with no run headers is observed under its own key")
    void bareRecordGetsAKey() throws Exception {
        FlowSpec spec = spec("kafka-bare");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            Observed<Steps> flow = observer.observe(spec, Steps.class, new Impl());
            ConsumerRecord<String, String> keyed = new ConsumerRecord<>("orders", 0, 5L, "order-12", "{}");
            try (Run run = KafkaRuns.join(flow, keyed)) {
                assertEquals("order-12", run.correlationId(), "the record key stands in for a correlation id");
            }
            ConsumerRecord<String, String> bare = new ConsumerRecord<>("orders", 3, 7L, null, "{}");
            try (Run run = KafkaRuns.join(flow, bare)) {
                assertEquals("orders/3/7", run.correlationId(), "no key at all: the record's position");
            }
            assertNull(KafkaRuns.read(bare));
        }
    }

    @Test @DisplayName("inject without an open run is a caller error, not a silent header-less record")
    void injectNeedsARun() {
        assertThrows(IllegalStateException.class, () -> KafkaRuns.inject(new ProducerRecord<>("t", "k", "v")));
    }
}
