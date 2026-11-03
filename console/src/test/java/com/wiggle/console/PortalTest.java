package com.wiggle.console;

import com.wiggle.server.engine.EngineException;
import com.wiggle.server.store.StorageException;
import com.wiggle.testfixtures.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The portal is started from the server's environment, on its own port, or not at all. */
class PortalTest {

    @Test @DisplayName("the portal is on by default, on 8070; WIGGLE_PORTAL_PORT=0 turns it off")
    void onByDefault() {
        assertEquals(8070, Portal.port(Map.of()));
        assertEquals(9000, Portal.port(Map.of(Portal.PORT_ENV, "9000")));
        assertEquals(0, Portal.port(Map.of(Portal.PORT_ENV, "0")));
        assertThrows(IllegalArgumentException.class, () -> Portal.port(Map.of(Portal.PORT_ENV, "web")));
    }

    /** A backend whose every call throws {@code failure}. */
    private static DashboardData failing(RuntimeException failure) {
        return (DashboardData) Proxy.newProxyInstance(DashboardData.class.getClassLoader(),
                new Class<?>[] {DashboardData.class}, (proxy, method, args) -> { throw failure; });
    }

    private static HttpResponse<String> cluster(DashboardData data) throws Exception {
        try (ConsoleServer http = new ConsoleServer(data, new ConsoleAuth("admin", null, false), TestPorts.free()).start()) {
            return HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                    URI.create("http://localhost:" + http.port() + "/api/cluster")).build(),
                    HttpResponse.BodyHandlers.ofString());
        }
    }

    @Test @DisplayName("a failed call answers with the engine's status, or 503/500 with a reference and no cause")
    void errorsAreMappedWithoutLeakingCauses() throws Exception {
        assertEquals(409, cluster(failing(EngineException.conflict("already finished"))).statusCode());

        HttpResponse<String> unavailable = cluster(failing(new StorageException("FATAL: host db-7.internal",
                null, StorageException.Classification.TRANSIENT)));
        assertEquals(503, unavailable.statusCode());
        assertFalse(unavailable.body().contains("db-7"), unavailable.body());
        assertTrue(unavailable.body().contains("ref "), unavailable.body());

        HttpResponse<String> internal = cluster(failing(new RuntimeException("relation wf_token does not exist")));
        assertEquals(500, internal.statusCode());
        assertFalse(internal.body().contains("wf_token"), internal.body());
    }
}
