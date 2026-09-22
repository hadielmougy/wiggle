package com.wiggle.observe.kafka;

import com.wiggle.observe.Observed;
import com.wiggle.observe.Run;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

/** The code in {@code docs/observed-execution.md}'s Kafka section, kept compilable. */
final class KafkaDocSnippet {

    interface CheckoutSteps {
        Map<String, Object> accept(Map<String, Object> order);
        Map<String, Object> reserve(Map<String, Object> order);
    }

    static void produce(Observed<CheckoutSteps> checkout, Producer<String, String> producer, Map<String, Object> order) {
        // docs:begin produce
        try (Run run = checkout.begin((String) order.get("id"))) {
            checkout.steps().accept(order);
            producer.send(KafkaRuns.inject(new ProducerRecord<>("orders", (String) order.get("id"), "{...}")));
        }
        // docs:end produce
    }

    static void consume(Observed<CheckoutSteps> checkout, ConsumerRecord<String, String> record) {
        // docs:begin consume
        KafkaRuns.handle(checkout, record, r -> checkout.steps().reserve(Map.of("id", r.key())));
        // docs:end consume
    }

    private KafkaDocSnippet() {}
}
