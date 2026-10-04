package com.wiggle.server.topology;

import com.wiggle.server.store.ReplicatedStorage;
import com.wiggle.server.store.ShardState;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The shards a sharded deployment runs on, what each holds, and how new instances are spread over
 * them. Built from the {@code WIGGLE_STORAGE_TOPOLOGY} document by {@link TopologyParser}, which has
 * already validated it.
 *
 * @param shards      every shard, in the order the document lists them
 * @param generations the placement history, oldest first
 */
public record Topology(List<Shard> shards, List<Generation> generations) {

    /** What a shard holds. */
    public enum Role { INSTANCES, HOME, AUTH, SEARCH }

    /** A database connection. {@code password} is already interpolated from the environment. */
    public record Connection(String url, String user, String password, int pool) {
        @Override public String toString() {
            return "Connection[url=" + url + ", user=" + user + ", pool=" + pool + "]";
        }
    }

    /**
     * One shard: a permanent id, its state, its roles, its primary and its read replicas.
     *
     * @param maxReplicaLagMillis a replica further behind than this serves no reads
     * @param replicaFallback     what a replica-allowed read does when no replica is within the lag
     */
    public record Shard(int id, ShardState state, Set<Role> roles, Connection primary, List<Connection> replicas,
                        long maxReplicaLagMillis, ReplicatedStorage.Fallback replicaFallback) {
        public Shard {
            replicas = List.copyOf(replicas);
        }

        public boolean has(Role role) { return roles.contains(role); }
    }

    /**
     * One version of the placement: from {@code activeFrom} (epoch millis), root instances are minted
     * on the shards in {@code weights} in proportion to their weight.
     */
    public record Generation(long id, long activeFrom, Map<Integer, Integer> weights) {}

    public Topology {
        shards = List.copyOf(shards);
        generations = List.copyOf(generations);
    }

    /** The shard with {@code role}; the validated document has exactly one home and one auth shard. */
    public Shard only(Role role) {
        return shards.stream().filter(s -> s.has(role)).findFirst().orElseThrow();
    }

    public int home() {
        return only(Role.HOME).id();
    }

    public Optional<Shard> shard(int id) {
        return shards.stream().filter(s -> s.id() == id).findFirst();
    }

    /** The generation that applies at {@code now}: the newest one already active, or the oldest one
     *  when none is active yet. */
    public Generation generationAt(long now) {
        Generation current = generations.getFirst();
        for (Generation g : generations) {
            if (g.activeFrom() <= now) current = g;
        }
        return current;
    }

    /** The newest generation this document holds, active or not. */
    public Generation newestGeneration() {
        return generations.getLast();
    }
}
