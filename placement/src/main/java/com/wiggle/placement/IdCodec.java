package com.wiggle.placement;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The instance id: {@code {namespace}[.c{cell}].e{epoch}.s{shard}.{ulid}}. An instance's cell is a
 * pure function of its id plus the placement policy, so there is no per-instance directory.
 *
 * <p>The optional {@code .c{cell}} segment names the cell that minted the id. Epoch and shard say
 * where an instance belongs under the current ring; the cell says where it was actually written, so
 * a deployment that never reshards can route on the label alone.
 *
 * <p>That segment sits before the epoch, not after the shard: a ulid may contain dots, so a
 * trailing segment would be ambiguous -- {@code ns.e0.s0.cfoo.bar} would parse as cell {@code foo}
 * with ulid {@code bar}.
 *
 * <p>Legacy ids (a bare {@code wfi_...}) do not match and {@link #parse} returns empty; callers
 * route those to the genesis cell.
 */
public final class IdCodec {

    // namespace (no '.'), optional .c<cell> (no '.'), then .e<digits> .s<digits> . <ulid rest>
    private static final Pattern PATTERN =
            Pattern.compile("^([^.]+)(?:\\.c([^.]+))?\\.e(\\d+)\\.s(\\d+)\\.(.+)$");

    /**
     * Instance-id columns are {@code VARCHAR(128)} (schema v9); {@link #format} refuses anything
     * longer. A schema owned by a DBA ({@code WIGGLE_SCHEMA_MODE=verify}) must be migrated to v9
     * before a cell using long namespace or cell names starts.
     */
    public static final int MAX_LENGTH = 128;

    private IdCodec() {}

    /** A parsed id. A null {@code cellId} is not an error: it means "ask the placement policy". */
    public record Placement(String namespace, String cellId, long epoch, long shard, String ulid) {

        /** True when the id names the cell that minted it, so routing needs no ring. */
        public boolean hasCell() { return cellId != null; }
    }

    /** Builds an id with no cell label, for a cell that has no id configured. */
    public static String format(String namespace, long epoch, long shard, String ulid) {
        return format(namespace, null, epoch, shard, ulid);
    }

    /**
     * Builds an id. The namespace and cell must not contain {@code '.'} (they are id segments); a
     * null or blank cell omits the segment.
     */
    public static String format(String namespace, String cellId, long epoch, long shard, String ulid) {
        requireSegment(namespace, "namespace");
        String cell = cellId == null || cellId.isBlank() ? null : cellId;
        if (cell != null) requireSegment(cell, "cell id");

        String id = namespace + (cell == null ? "" : ".c" + cell)
                + ".e" + epoch + ".s" + shard + "." + ulid;
        if (id.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("instance id would be " + id.length()
                    + " characters, over the " + MAX_LENGTH + " an id column holds: '" + id
                    + "'. Shorten the namespace or the cell id.");
        }
        return id;
    }

    private static void requireSegment(String value, String what) {
        if (value == null || value.isEmpty() || value.indexOf('.') >= 0) {
            throw new IllegalArgumentException(
                    what + " must be non-empty and contain no '.': '" + value + "'");
        }
    }

    /** Parses an id, or empty for a legacy one. */
    public static Optional<Placement> parse(String id) {
        if (id == null) return Optional.empty();
        var m = PATTERN.matcher(id);
        if (!m.matches()) return Optional.empty();
        try {
            return Optional.of(new Placement(m.group(1), m.group(2), Long.parseLong(m.group(3)),
                    Long.parseLong(m.group(4)), m.group(5)));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /** True for a legacy (non-epoch-aware) id -- route these to the genesis cell. */
    public static boolean isLegacy(String id) {
        return parse(id).isEmpty();
    }

    /**
     * The shard a new id lands on: a hash of the ulid reduced to {@code [0, ringSize)}, 0 for a
     * single-cell ring. Deterministic across JVMs. Stamped into the id at mint time and read back
     * verbatim on resolve, never recomputed.
     */
    // docs:begin shard-for
    public static long shardFor(String ulid, int ringSize) {
        return ringSize <= 1 ? 0 : Math.floorMod(hash64(ulid), ringSize);
    }
    // docs:end shard-for

    /**
     * 64-bit FNV-1a over the ulid, finished with a murmur3 fmix64 avalanche. {@link String#hashCode()}
     * clusters on the shared timestamp prefix of ULIDs minted close together; the finalizer is what
     * spreads the random suffix evenly.
     */
    private static long hash64(String s) {
        long h = 0xcbf29ce484222325L;              // FNV-1a 64 offset basis
        for (int i = 0, n = s.length(); i < n; i++) {
            h ^= s.charAt(i);
            h *= 0x00000100000001b3L;               // FNV prime
        }
        h ^= h >>> 33;                             // fmix64 (MurmurHash3) -- avalanche
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }
}
