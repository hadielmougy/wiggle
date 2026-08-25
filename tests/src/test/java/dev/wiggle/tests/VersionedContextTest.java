package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.ContextVersion;
import dev.wiggle.core.Ids;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.core.VersionedContextCodec;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The versioned context envelope: upcast-to-current on decode, an immutable origin version
 * exposed to handlers via {@link ContextVersion}, legacy (pre-envelope) contexts, and overlay-key
 * folding -- proven both as a pure codec round-trip and end to end through a live engine, where an
 * instance created under an older codec is upcast and finished by a newer worker.
 */
class VersionedContextTest {

    // v3 shape: 'total' (was 'amount' in v2) and 'currency' (added in v2), plus 'origin' which a
    // step stamps from ContextVersion so the test can observe the version a handler saw.
    public record Order(String id, int qty, java.math.BigDecimal total, String currency, int origin) {}

    /** currentVersion = 3; migrates v1 -> v2 (add currency) and v2 -> v3 (rename amount -> total). */
    private static VersionedContextCodec<Order> codecV3() {
        return VersionedContextCodec.builder(Order.class, 3)
                .schema("order")
                .upcast(1, m -> { m.put("currency", "USD"); return m; })
                .upcast(2, m -> { m.put("total", m.remove("amount")); return m; })
                .build();
    }

    @AfterEach
    void clearAmbient() { ContextVersion.clear(); }

    // ---- pure codec round-trip ----

    @Test @DisplayName("encode wraps the record in a versioned envelope stamped at the current version")
    void encodeWrapsInEnvelope() {
        Object env = codecV3().encode(new Order("A-1", 2, new java.math.BigDecimal("9.99"), "USD", 0));
        Map<String, Object> m = Json.asObject(env);
        assertEquals("order", m.get("_schema"));
        assertEquals(3L, ((Number) m.get("_v")).longValue());
        Map<String, Object> data = Json.asObject(m.get("data"));
        assertEquals("A-1", data.get("id"));
        assertNull(m.get("id"), "record fields live under data, not at the envelope top level");
    }

    @Test @DisplayName("decode of a v1 envelope upcasts through v2 and v3 to the current record")
    void decodeUpcastsFromV1() {
        // A context written by an old (v1) codec: only id/qty/amount, no currency, no total.
        Map<String, Object> data = new LinkedHashMap<>(Map.of("id", "A-2", "qty", 5, "amount", "20.00"));
        Object env = envelope(1, data);

        Order out = codecV3().decode(env);
        assertEquals("A-2", out.id());
        assertEquals(5, out.qty());
        assertEquals(new java.math.BigDecimal("20.00"), out.total(), "amount was renamed to total by the v2->v3 upcast");
        assertEquals("USD", out.currency(), "currency was defaulted by the v1->v2 upcast");
        assertEquals(1, ContextVersion.current(), "the origin version is surfaced to the handler");
    }

    @Test @DisplayName("a bare, pre-envelope context is read as version 1 and upcast")
    void decodeLegacyBareContext() {
        // No _v/_data markers at all -- an instance that predates the codec.
        Map<String, Object> bare = new LinkedHashMap<>(Map.of("id", "A-3", "qty", 1, "amount", "3.50"));

        Order out = codecV3().decode(bare);
        assertEquals("A-3", out.id());
        assertEquals(new java.math.BigDecimal("3.50"), out.total());
        assertEquals("USD", out.currency());
        assertEquals(1, ContextVersion.current(), "a legacy context is treated as born at version 1");
    }

    @Test @DisplayName("overlay keys left at the top level are folded into data; residual nulls are ignored")
    void decodeFoldsOverlayKeys() {
        Map<String, Object> data = new LinkedHashMap<>(Map.of("id", "A-4", "qty", 2, "total", "5.00", "currency", "EUR", "origin", 0));
        Map<String, Object> env = Json.asObject(envelope(3, data));
        // Simulate a forkEach payload overlay merged at the top level, plus a stale null left by a legacy upgrade.
        env.put("qty", 9);            // overlay overrides
        env.put("id", null);          // residual null must NOT clobber the real id

        Order out = codecV3().decode(env);
        assertEquals("A-4", out.id(), "a top-level null does not overwrite the real field");
        assertEquals(9, out.qty(), "a non-null overlay key is folded into data");
    }

    @Test @DisplayName("encode always stamps the current version, independent of any prior decode on the thread")
    void encodeIsPureAndStampsCurrent() {
        VersionedContextCodec<Order> codec = codecV3();
        codec.decode(envelope(1, new LinkedHashMap<>(Map.of("id", "A-5", "qty", 1, "amount", "1.00")))); // leaves ambient = 1

        Object reEncoded = codec.encode(new Order("A-5", 1, new java.math.BigDecimal("1.00"), "USD", 0));
        Map<String, Object> m = Json.asObject(reEncoded);
        assertEquals(3L, ((Number) m.get("_v")).longValue(),
                "a migrated instance is re-stored at the current version, regardless of ambient state");
    }

    @Test @DisplayName("building with a gap in the upcast chain fails fast")
    void builderRejectsMissingUpcast() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> VersionedContextCodec.builder(Order.class, 3).schema("order")
                        .upcast(1, m -> m)   // missing 2 -> 3
                        .build());
        assertTrue(e.getMessage().contains("2"));
    }

    // ---- end to end through a live engine ----

    @Test @DisplayName("a v1-created instance is upcast and finished by a v3 worker, keeping its origin")
    void crossVersionUpcastThroughEngine() throws Exception {
        // The old world: currentVersion = 1, an Order with just id/qty/amount.
        record OrderV1(String id, int qty, java.math.BigDecimal amount) {}
        VersionedContextCodec<OrderV1> codecV1 = VersionedContextCodec.builder(OrderV1.class, 1)
                .schema("order").build();
        Blueprint<OrderV1> bpV1 = Workflow.define("order-mig", codecV1)
                .step("price", o -> o)   // identity; only needs to exist so the topology matches
                .build();

        // The new world: same workflow + step name (same topology), but the v3 codec and a step
        // that stamps the version it observed into the context.
        Blueprint<Order> bpV3 = Workflow.define("order-mig", codecV3())
                .step("price", o -> new Order(o.id(), o.qty(), o.total(), o.currency(), ContextVersion.current()))
                .build();

        assertEquals(bpV1.version(), bpV3.version(),
                "same name + topology must hash to the same version, so a v3 worker handles a v1 instance");

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "mig-" + Ids.next("x")).register(bpV3)) {
            w.start();
            client.register(bpV1);   // register the (same-version) definition so start() accepts it

            // Start under the OLD codec -> stored as a v1 envelope with no currency and an 'amount'.
            String legacyId = client.start(bpV1, new OrderV1("A-9", 4, new java.math.BigDecimal("40.25")));
            InstanceView v = client.awaitCompletion(legacyId, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());

            Order out = codecV3().decode(v.context());
            assertEquals(new java.math.BigDecimal("40.25"), out.total(), "amount was upcast to total end to end");
            assertEquals("USD", out.currency(), "currency was defaulted by the upcast end to end");
            assertEquals(1, out.origin(), "the handler saw the instance's origin version (1), not the current version");

            // A brand-new instance under the current codec is born at version 3.
            String freshId = client.start(bpV3, new Order("A-10", 1, new java.math.BigDecimal("1.00"), "GBP", 0));
            Order fresh = codecV3().decode(client.awaitCompletion(freshId, Duration.ofSeconds(20)).context());
            assertEquals(3, fresh.origin(), "a fresh instance's handler sees the current version");
            assertEquals("GBP", fresh.currency(), "a fresh instance's own data is untouched by upcasts");
        }
    }

    private static Object envelope(int v, Map<String, Object> data) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("_schema", "order");
        env.put("_v", v);
        env.put("data", data);
        return env;
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "ver-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }
}
