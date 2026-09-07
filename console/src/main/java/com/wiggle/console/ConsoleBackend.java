package com.wiggle.console;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.core.Tls;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * How the console reaches the control plane, abstracted over the two connection modes so
 * {@link GrpcDashboardData} is mode-agnostic:
 * <ul>
 *   <li>{@link #reads()} — a client for general reads (workflows / schedules / cluster).</li>
 *   <li>{@link #cells()} — the client(s) to fan an instance listing across (one in direct mode, all
 *       active cells of the namespace under a coordinator).</li>
 *   <li>{@link #forInstance(String)} — the client that owns an instance, for operate-by-id.</li>
 * </ul>
 */
interface ConsoleBackend extends AutoCloseable {

    WiggleClient reads();

    List<WiggleClient> cells();

    WiggleClient forInstance(String id);

    @Override void close();

    /** One standalone cluster: every call goes to the single client (the shared DB is the whole view). */
    final class Direct implements ConsoleBackend {
        private final WiggleClient client;

        Direct(DirectConnection conn) {
            this.client = conn.client();
        }

        @Override public WiggleClient reads() { return client; }
        @Override public List<WiggleClient> cells() { return List.of(client); }
        @Override public WiggleClient forInstance(String id) { return client; }
        @Override public void close() { /* the DirectConnection owns the client */ }
    }

    /** A coordinator-sharded namespace: fan listings across active cells; route reads/by-id via the connection. */
    final class Coordinated implements ConsoleBackend {
        private final CoordinatedConnection conn;
        private final String namespace;
        private final Tls.Options tls;
        private final Map<String, WiggleClient> cellClients = new ConcurrentHashMap<>();

        Coordinated(CoordinatedConnection conn, String namespace, Tls.Options tls) {
            this.conn = conn;
            this.namespace = namespace;
            this.tls = tls;
        }

        @Override public WiggleClient reads() { return conn.clientForNamespace(namespace); }

        @Override public List<WiggleClient> cells() {
            List<WiggleClient> out = new ArrayList<>();
            for (String target : conn.activeCellTargets(namespace)) {
                out.add(cellClients.computeIfAbsent(target, t -> new WiggleClient(t, tls)));
            }
            return out;
        }

        @Override public WiggleClient forInstance(String id) { return conn.clientForInstance(id); }

        @Override public void close() {
            cellClients.values().forEach(WiggleClient::close);   // the connection owns its own clients; we own these
        }
    }
}
