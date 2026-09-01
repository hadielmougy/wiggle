package com.wiggle.coordinator.ratis;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * DESIGN SKETCH — not wired into the build. A thin RocksDB KV wrapper + the coordinator key layout
 * (docs/coordinator-ratis-rocksdb.md §3). Owned by {@link CoordStateMachine}; all mutations happen inside
 * apply via {@link #batch()} so one command commits atomically.
 */
final class RocksKv implements AutoCloseable {

    // key prefixes
    static final String POLICY = "policy/";
    static final String NODE = "node/";
    static final String NODE_NS = "node-ns/";     // node-ns/<ns>/<id> -> "" (secondary index)
    static final String CELL = "cell/";           // cell/<ns>/<cellId> -> fingerprint
    static final String DEF = "def/";             // def/<ns>/<name>
    static final String NS = "ns/";
    static final String LEADER = "leader";

    private final RocksDB db;

    RocksKv(RocksDB db) { this.db = db; }

    // ---- point ops ----
    Optional<String> get(String key) {
        try {
            byte[] v = db.get(bytes(key));
            return v == null ? Optional.empty() : Optional.of(new String(v, StandardCharsets.UTF_8));
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    /** A batch applied atomically -- used for a command that touches a primary + its index. */
    Batch batch() { return new Batch(); }

    /** Keys under {@code prefix}, in order -- backs nodes(ns)/definitions(ns) prefix scans. */
    List<String> scan(String prefix) {
        List<String> out = new ArrayList<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seek(bytes(prefix)); it.isValid(); it.next()) {
                String k = new String(it.key(), StandardCharsets.UTF_8);
                if (!k.startsWith(prefix)) break;
                out.add(k);
            }
        }
        return out;
    }

    List<String> values(String prefix) {
        List<String> out = new ArrayList<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seek(bytes(prefix)); it.isValid(); it.next()) {
                if (!new String(it.key(), StandardCharsets.UTF_8).startsWith(prefix)) break;
                out.add(new String(it.value(), StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    /** RocksDB checkpoint at the current state -- the raw material for a Ratis snapshot (§5). */
    void checkpointTo(String dir) {
        try (Checkpoint cp = Checkpoint.create(db)) { cp.createCheckpoint(dir); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }

    @Override public void close() { db.close(); }

    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    /** A write batch; call {@link #commit()} once at the end of applyTransaction. */
    final class Batch {
        private final WriteBatch wb = new WriteBatch();
        Batch put(String k, String v) { try { wb.put(bytes(k), bytes(v)); } catch (Exception e) { throw new IllegalStateException(e); } return this; }
        Batch delete(String k) { try { wb.delete(bytes(k)); } catch (Exception e) { throw new IllegalStateException(e); } return this; }
        void commit() {
            try (WriteOptions wo = new WriteOptions().setSync(false)) { db.write(wo, wb); }
            catch (Exception e) { throw new IllegalStateException(e); }
            finally { wb.close(); }
        }
    }
}
