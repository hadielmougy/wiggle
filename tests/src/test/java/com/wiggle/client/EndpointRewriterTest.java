package com.wiggle.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The CellResolver endpoint-rewrite seam: map an in-cluster address to a reachable one for testing. */
class EndpointRewriterTest {

    @Test @DisplayName("empty/blank spec is identity")
    void identity() {
        assertEquals("10.244.0.7:8080", EndpointRewriter.fromSpec("").rewrite("10.244.0.7:8080"));
        assertEquals("10.244.0.7:8080", EndpointRewriter.fromSpec(null).rewrite("10.244.0.7:8080"));
    }

    @Test @DisplayName("port-only wildcard maps any host on that port")
    void wildcardPort() {
        EndpointRewriter r = EndpointRewriter.fromSpec("*:8080=127.0.0.1:18100");
        assertEquals("127.0.0.1:18100", r.rewrite("10.244.0.7:8080"));
        assertEquals("127.0.0.1:18100", r.rewrite("10.244.0.9:8080"));   // pod IP changed, still mapped
        assertEquals("10.244.0.7:9090", r.rewrite("10.244.0.7:9090"));   // different port untouched
    }

    @Test @DisplayName("exact host:port rules, first match wins; a scheme is tolerated")
    void exactRules() {
        EndpointRewriter r = EndpointRewriter.fromSpec(
                "10.244.0.7:8080=127.0.0.1:18100, 10.244.0.8:8080=127.0.0.1:18101");
        assertEquals("127.0.0.1:18100", r.rewrite("10.244.0.7:8080"));
        assertEquals("127.0.0.1:18101", r.rewrite("10.244.0.8:8080"));
        assertEquals("127.0.0.1:18100", r.rewrite("grpc://10.244.0.7:8080"));   // scheme stripped before match
        assertEquals("10.244.0.9:8080", r.rewrite("10.244.0.9:8080"));          // unmatched host unchanged
    }
}
