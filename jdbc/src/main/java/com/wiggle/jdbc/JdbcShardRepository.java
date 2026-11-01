package com.wiggle.jdbc;

import com.wiggle.server.store.Rows;
import com.wiggle.server.store.ShardRepository;
import com.wiggle.server.store.ShardState;

import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** {@link ShardRepository} on {@code wf_shard} and {@code wf_shard_registry}. */
final class JdbcShardRepository extends JdbcRepository implements ShardRepository {

    JdbcShardRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public void claimShardIdentity(int shardId) {
        jdbc().update(dialect.insertIgnore("INSERT INTO wf_shard (k,shard_id) VALUES ('self',:shard)"),
                params().add("shard", shardId));
    }

    @Override public void writeShardBeat(long now) {
        jdbc().update("UPDATE wf_shard SET beat_at=:now WHERE k='self'", params().add("now", now));
    }

    @Override public void putShardRecord(Rows.ShardRecord r) {
        String update = "UPDATE wf_shard_registry SET state=:state,first_seen=:first,retired_at=:retired "
                + "WHERE shard_id=:shard";
        if (jdbc().update(update, shard(r)) > 0) return;
        if (jdbc().update(dialect.insertIgnore("INSERT INTO wf_shard_registry "
                + "(shard_id,state,first_seen,retired_at) VALUES (:shard,:state,:first,:retired)"), shard(r)) > 0) return;
        jdbc().update(update, shard(r));   // another node inserted it first
    }

    private static Params shard(Rows.ShardRecord r) {
        return params()
                .add("shard", r.shardId())
                .add("state", r.state().name())
                .add("first", r.firstSeen())
                .number("retired", r.retiredAt());
    }

    @Override public OptionalInt shardIdentity() {
        List<Integer> ids = jdbc().queryForList("SELECT shard_id FROM wf_shard WHERE k='self'", params(), Integer.class);
        return ids.isEmpty() ? OptionalInt.empty() : OptionalInt.of(ids.getFirst());
    }

    @Override public OptionalLong shardBeat() {
        List<Long> beats = jdbc().queryForList("SELECT beat_at FROM wf_shard WHERE k='self' AND beat_at IS NOT NULL",
                params(), Long.class);
        return beats.isEmpty() ? OptionalLong.empty() : OptionalLong.of(beats.getFirst());
    }

    @Override public List<Rows.ShardRecord> shardRegistry() {
        return jdbc().query("SELECT shard_id,state,first_seen,retired_at FROM wf_shard_registry ORDER BY shard_id",
                (rs, row) -> {
                    long retired = rs.getLong("retired_at");
                    Long retiredAt = rs.wasNull() ? null : retired;
                    return new Rows.ShardRecord(rs.getInt("shard_id"),
                            ShardState.valueOf(rs.getString("state")), rs.getLong("first_seen"), retiredAt);
                });
    }
}
