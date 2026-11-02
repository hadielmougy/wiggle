package com.wiggle.jdbc;

import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.TaskLocks;
import com.wiggle.server.store.WriteRepositories;

import java.util.Optional;

/** The repositories of one {@link JdbcStorage}, one per area; each runs on the connection of the transaction open on the calling thread. */
final class JdbcRepositories implements WriteRepositories {

    private final JdbcStorage owner;
    private final JdbcShardRepository shards;
    private final JdbcClusterRepository cluster;
    private final JdbcScheduleRepository schedules;
    private final JdbcAuthRepository auth;
    private final JdbcSearchRepository search;
    private final JdbcCompensationRepository compensation;
    private final JdbcDynamicNodeRepository dynamicNodes;
    private final JdbcEventRepository events;
    private final JdbcGraphRepository graph;
    private final JdbcInstanceRepository instances;
    private final JdbcTokenRepository tokens;
    private final TaskLocks locks;

    JdbcRepositories(Dialect dialect, JdbcStorage owner) {
        this.owner = owner;
        this.shards = new JdbcShardRepository(owner);
        this.cluster = new JdbcClusterRepository(owner);
        this.schedules = new JdbcScheduleRepository(owner);
        this.auth = new JdbcAuthRepository(owner);
        this.search = new JdbcSearchRepository(owner);
        this.compensation = new JdbcCompensationRepository(owner);
        this.dynamicNodes = new JdbcDynamicNodeRepository(owner);
        this.events = new JdbcEventRepository(owner);
        this.graph = new JdbcGraphRepository(owner);
        this.instances = new JdbcInstanceRepository(owner);
        this.tokens = new JdbcTokenRepository(owner);
        this.locks = new TaskLocks() {
            @Override public Optional<Instance> lockInstanceOf(String tokenId) {
                return instances.lockInstanceOf(tokenId);
            }

            /** The token and its instance in one statement where the dialect can; otherwise the instance, then the token under its lock. */
            @Override public Optional<Rows.LockedTask> lockTask(String tokenId) {
                if (dialect.supportsJoinedLock()) return instances.lockTask(tokenId);
                return lockInstanceOf(tokenId).flatMap(i -> tokens.findToken(tokenId).map(t -> new Rows.LockedTask(i, t)));
            }
        };
    }

    @Override public JdbcShardRepository shards() { return shards; }
    @Override public JdbcAuthRepository auth() { return auth; }
    @Override public JdbcSearchRepository search() { return search; }
    @Override public JdbcInstanceRepository instances() { return instances; }
    @Override public JdbcTokenRepository tokens() { return tokens; }
    @Override public JdbcScheduleRepository schedules() { return schedules; }
    @Override public JdbcClusterRepository cluster() { return cluster; }
    @Override public JdbcCompensationRepository compensation() { return compensation; }
    @Override public JdbcEventRepository events() { return events; }
    @Override public JdbcGraphRepository graph() { return graph; }
    @Override public JdbcDynamicNodeRepository dynamicNodes() { return dynamicNodes; }
    @Override public TaskLocks locks() { return locks; }
    @Override public void bufferWrites() { owner.bufferWrites(); }
}
