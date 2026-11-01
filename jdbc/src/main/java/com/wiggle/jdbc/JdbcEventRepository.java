package com.wiggle.jdbc;

import com.wiggle.server.store.EventRepository;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.StorageException;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link EventRepository} on {@code wf_event}, {@code wf_event_cursor} and {@code wf_event_cursor_shard}. */
final class JdbcEventRepository extends JdbcRepository implements EventRepository {

    JdbcEventRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public void advanceEventPosition(String consumer, int shard, long ackedSeq) {
        String move = "UPDATE wf_event_cursor_shard SET acked_seq=CASE WHEN acked_seq<:acked THEN :acked "
                + "ELSE acked_seq END WHERE consumer=:consumer AND shard_id=:shard";
        Params p = params().add("acked", ackedSeq).text("consumer", consumer).add("shard", shard);
        if (jdbc().update(move, p) > 0) return;
        if (jdbc().update(dialect.insertIgnore("INSERT INTO wf_event_cursor_shard (consumer,shard_id,acked_seq) "
                + "VALUES (:consumer,:shard,:acked)"), p) > 0) return;
        jdbc().update(move, p);
    }

    @Override public long appendEvent(Rows.Event e) {
        KeyHolder seq = new GeneratedKeyHolder();
        jdbc().update("INSERT INTO wf_event (instance_id,workflow,version,correlation_id,type,"
                        + "node_id,payload_ver,payload,created_at) VALUES "
                        + "(:instanceId,:workflow,:version,:correlationId,:type,:nodeId,:payloadVer,"
                        + ":payload,:createdAt)",
                params()
                        .text("instanceId", e.instanceId())
                        .text("workflow", e.workflow())
                        .add("version", e.version())
                        .text("correlationId", e.correlationId())
                        .text("type", e.type())
                        .text("nodeId", e.nodeId())
                        .add("payloadVer", e.payloadVer())
                        .text("payload", e.payload())
                        .add("createdAt", e.createdAt()),
                seq, new String[]{"seq"});
        Number key = seq.getKey();
        if (key == null) throw new StorageException("wf_event insert returned no seq", null);
        return key.longValue();
    }

    @Override public void createEventCursorIfAbsent(Rows.EventCursor cursor) {
        jdbc().update(dialect.insertIgnore("INSERT INTO wf_event_cursor "
                        + "(consumer,acked_seq,last_seen,created_at) VALUES "
                        + "(:consumer,:acked,:lastSeen,:createdAt)"),
                params()
                        .text("consumer", cursor.consumer())
                        .add("acked", cursor.ackedSeq())
                        .add("lastSeen", cursor.lastSeen())
                        .add("createdAt", cursor.createdAt()));
    }

    @Override public void advanceEventCursor(String consumer, long ackedSeq, long now) {
        // Update-then-insert rather than an upsert: only PostgreSQL takes ON CONFLICT DO UPDATE,
        // and the two statements are portable. The trailing update covers the race where another
        // ack inserted the row between ours: it folds our seq into the row that won.
        if (moveCursor(consumer, ackedSeq, now) > 0) return;
        createEventCursorIfAbsent(new Rows.EventCursor(consumer, ackedSeq, now, now));
        moveCursor(consumer, ackedSeq, now);
    }

    private int moveCursor(String consumer, long ackedSeq, long now) {
        return jdbc().update("UPDATE wf_event_cursor SET acked_seq=CASE WHEN acked_seq<:acked "
                        + "THEN :acked ELSE acked_seq END, last_seen=:now WHERE consumer=:consumer",
                params().add("acked", ackedSeq).add("now", now).text("consumer", consumer));
    }

    @Override public int deleteEvents(long createdBefore, Long upToSeq, int max) {
        Params pick = params().add("before", createdBefore).add("max", max);
        if (upToSeq != null) pick.add("upTo", upToSeq);
        List<Long> seqs = jdbc().queryForList("SELECT seq FROM wf_event WHERE created_at<:before"
                + (upToSeq != null ? " AND seq<=:upTo" : "")
                + " ORDER BY seq LIMIT :max", pick, Long.class);
        if (seqs.isEmpty()) return 0;
        return jdbc().update("DELETE FROM wf_event WHERE seq<=:upTo AND created_at<:before",
                params().add("upTo", seqs.getLast()).add("before", createdBefore));
    }

    @Override public Map<Integer, Long> eventPositions(String consumer) {
        Map<Integer, Long> out = new HashMap<>();
        jdbc().query("SELECT shard_id, acked_seq FROM wf_event_cursor_shard WHERE consumer=:consumer",
                params().text("consumer", consumer),
                rs -> { out.put(rs.getInt("shard_id"), rs.getLong("acked_seq")); });
        return out;
    }

    @Override public Long oldestEventPosition(int shard) {
        // MIN over no cursors is NULL; a consumer with no row for the shard holds 0.
        return jdbc().queryForObject("SELECT MIN(COALESCE(p.acked_seq, 0)) FROM wf_event_cursor c "
                + "LEFT JOIN wf_event_cursor_shard p ON p.consumer=c.consumer AND p.shard_id=:shard",
                params().add("shard", shard), Long.class);
    }

    @Override public List<Rows.Event> eventsAfter(long afterSeq, long createdBefore, int max) {
        return jdbc().query("SELECT seq,instance_id,workflow,version,correlation_id,type,node_id,"
                        + "payload_ver,payload,created_at FROM wf_event "
                        + "WHERE seq>:after AND created_at<:before ORDER BY seq LIMIT :max",
                params().add("after", afterSeq).add("before", createdBefore).add("max", max),
                (rs, row) -> JdbcStorage.readEvent(rs));
    }

    @Override public long latestEventSeq() {
        return jdbc().queryForObject("SELECT COALESCE(MAX(seq),0) FROM wf_event", params(), Long.class);
    }

    @Override public Rows.EventCursor eventCursor(String consumer) {
        return first(jdbc().query("SELECT consumer,acked_seq,last_seen,created_at FROM wf_event_cursor "
                        + "WHERE consumer=:consumer", params().text("consumer", consumer),
                (rs, row) -> JdbcStorage.readCursor(rs))).orElse(null);
    }

    @Override public Long oldestAckedSeq() {
        // MIN over no cursors is a row holding NULL, so the absence comes from the value.
        return jdbc().queryForObject("SELECT MIN(acked_seq) FROM wf_event_cursor", params(), Long.class);
    }
}
