package com.wiggle.coordinator.ratis;

import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A thin RocksDB KV wrapper + the coordinator key layout (docs/coordinator-ratis-rocksdb.md §3). Owned by
 * {@link CoordStateMachine}; all mutations happen inside apply via {@link #batch()} so one command commits
 * atomically. Control-plane state is bounded, so by-namespace listings are plain prefix scans filtered in
 * memory (the same approach the etcd store takes) rather than a maintained secondary index.
 */
final class RocksKv implements AutoCloseable {

    // key prefixes
    static final String POLICY = "policy/";       // policy/<ns>            -> policy blob (self-contained)
    static final String NODE = "node/";           // node/<id>              -> node blob (carries namespace)
    static final String CELL = "cell/";           // cell/<ns>/<cellId>     -> fingerprint
    static final String DEF = "def/";             // def/<ns>/<name>        -> definition blob
    static final String NS = "ns/";               // ns/<namespace>         -> namespace blob
    static final String LEADER = "leader";        // leader                 -> {holder, expiresAt}

    private final RocksDB db;
    private final Options options;

    private RocksKv(RocksDB db, Options options) {
        this.db = db;
        this.options = options;
    }

    /** Open (creating if absent) a RocksDB at {@code dir}; loads the bundled native library once. */
    static RocksKv open(File dir) {
        try {
            RocksDB.loadLibrary();
            Files.createDirectories(dir.toPath());
            Options opts = new Options().setCreateIfMissing(true);
            RocksDB db = RocksDB.open(opts, dir.getAbsolutePath());
            return new RocksKv(db, opts);
        } catch (Exception e) {
            throw new IllegalStateException("open rocksdb at " + dir, e);
        }
    }

    // ---- point ops ----
    Optional<String> get(String key) {
        try {
            byte[] v = db.get(bytes(key));
            return v == null ? Optional.empty() : Optional.of(new String(v, StandardCharsets.UTF_8));
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    /** A batch applied atomically -- used for a command that touches a primary + its index. */
    Batch batch() { return new Batch(); }

    /** Keys under {@code prefix}, in order -- backs the by-namespace / expiry scans. */
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

    /** Key+value pairs under {@code prefix}, in order -- for scans that need the key (suffix) too. */
    List<Kv> entries(String prefix) {
        List<Kv> out = new ArrayList<>();
        try (RocksIterator it = db.newIterator()) {
            for (it.seek(bytes(prefix)); it.isValid(); it.next()) {
                String k = new String(it.key(), StandardCharsets.UTF_8);
                if (!k.startsWith(prefix)) break;
                out.add(new Kv(k, new String(it.value(), StandardCharsets.UTF_8)));
            }
        }
        return out;
    }

    /** RocksDB checkpoint at the current state -- the raw material for a Ratis snapshot (§5). */
    void checkpointTo(String dir) {
        try (Checkpoint cp = Checkpoint.create(db)) { cp.createCheckpoint(dir); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }

    @Override public void close() {
        db.close();
        options.close();
    }

    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    /** A prefix-scan entry. */
    record Kv(String key, String value) {}

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