package com.wiggle.coordinator.ratis;

import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.CoordinatorStoreProvider;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The {@link CoordinatorStoreProvider} seam for the embedded Ratis+RocksDB backend, resolved by
 * {@code dist} the same way JDBC/Cassandra/etcd are. It parses {@code WIGGLE_COORD_STORE=ratis://...},
 * boots (or joins) the coordinator's Ratis group with a RocksDB-backed {@link CoordStateMachine}, and
 * hands back a client-side {@link RatisCoordinatorStore}. A single-member group is the zero-dependency
 * dev mode.
 *
 * <h2>URI</h2>
 * {@code ratis://<host:port>/<data-dir>?peers=<p1>,<p2>,...&id=<self>} where each peer is
 * {@code <id>@<host:port>} (bare {@code <host:port>} auto-assigns ids {@code n0, n1, ...}).
 * <ul>
 *   <li>{@code /<data-dir>} — the local directory for the Raft log + RocksDB state (required).</li>
 *   <li>{@code peers} — the group membership; omitted means a single-member dev group at
 *       {@code 127.0.0.1:10000}.</li>
 *   <li>{@code id} — which peer <em>this</em> process is; defaults to the first peer (correct for the
 *       single-member case, required to be set explicitly on each node of a real multi-node group).</li>
 * </ul>
 * Example (dev): {@code ratis:///var/lib/wiggle/coord}. Example (3-node):
 * {@code ratis:///var/lib/wiggle/coord?peers=n0@10.0.0.1:10000,n1@10.0.0.2:10000,n2@10.0.0.3:10000&id=n0}
 */
public final class RatisCoordinatorStoreProvider implements CoordinatorStoreProvider {

    // A fixed group id so every member and client addresses the same coordinator Raft group.
    private static final RaftGroupId GROUP_ID =
            RaftGroupId.valueOf(UUID.fromString("d5b6f0a2-0000-4000-8000-c0ffeec0ffee"));

    private static final String DEFAULT_PEER_ADDRESS = "127.0.0.1:10000";

    private final RaftServer server;   // this node's group member (null if this process is only a client)
    private final RaftClient client;

    public RatisCoordinatorStoreProvider(String uri) {
        try {
            URI u = URI.create(uri);
            File dataDir = new File(u.getPath() == null || u.getPath().isBlank() ? "/var/lib/wiggle/coord" : u.getPath());
            Map<String, String> q = parseQuery(u.getRawQuery());

            List<RaftPeer> peers = parsePeers(q.get("peers"));
            RaftPeerId selfId = RaftPeerId.valueOf(q.getOrDefault("id", peers.get(0).getId().toString()));
            RaftGroup group = RaftGroup.valueOf(GROUP_ID, peers);

            RaftProperties props = new RaftProperties();
            org.apache.ratis.RaftConfigKeys.Rpc.setType(props, SupportedRpcType.GRPC);
            RaftServerConfigKeys.setStorageDir(props, List.of(new File(dataDir, "raft")));

            boolean isMember = peers.stream().anyMatch(p -> p.getId().equals(selfId));
            this.server = isMember ? startServer(selfId, group, props, peers) : null;
            this.client = RaftClient.newBuilder().setProperties(props).setRaftGroup(group).build();
        } catch (Exception e) {
            throw new IllegalStateException("failed to start ratis coordinator store from " + uri, e);
        }
    }

    private static RaftServer startServer(RaftPeerId selfId, RaftGroup group, RaftProperties props, List<RaftPeer> peers)
            throws Exception {
        // The GRPC server binds to this peer's declared port.
        int port = Integer.parseInt(peerAddress(peers, selfId).split(":", 2)[1]);
        GrpcConfigKeys.Server.setPort(props, port);
        RaftServer server = RaftServer.newBuilder()
                .setServerId(selfId)
                .setGroup(group)
                .setProperties(props)
                .setStateMachine(new CoordStateMachine())
                .build();
        server.start();
        return server;
    }

    @Override public CoordinatorStore coordinatorStore() {
        return new RatisCoordinatorStore(client, server);
    }

    // ---- uri parsing ----

    private static List<RaftPeer> parsePeers(String peersSpec) {
        List<RaftPeer> peers = new ArrayList<>();
        if (peersSpec == null || peersSpec.isBlank()) {
            peers.add(RaftPeer.newBuilder().setId("n0").setAddress(DEFAULT_PEER_ADDRESS).build());
            return peers;
        }
        String[] specs = peersSpec.split(",");
        for (int i = 0; i < specs.length; i++) {
            String s = specs[i].trim();
            String id, addr;
            int at = s.indexOf('@');
            if (at >= 0) { id = s.substring(0, at); addr = s.substring(at + 1); }
            else { id = "n" + i; addr = s; }
            peers.add(RaftPeer.newBuilder().setId(id).setAddress(addr).build());
        }
        return peers;
    }

    private static String peerAddress(List<RaftPeer> peers, RaftPeerId id) {
        return peers.stream().filter(p -> p.getId().equals(id)).findFirst().orElseThrow().getAddress();
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> q = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) return q;
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) q.put(pair, "");
            else q.put(pair.substring(0, eq), pair.substring(eq + 1));
        }
        return q;
    }
}