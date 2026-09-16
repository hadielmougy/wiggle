package com.wiggle.core;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The instance id: {@code {namespace}[.c{cell}].e{epoch}.s{shard}.{ulid}}. The id <em>is</em> the
 * routing record -- an instance's cell is a pure function of its id plus the (bounded) placement
 * policy, so the coordinator never stores a per-instance directory (R16). See the design reference §6.
 *
 * <p>The optional {@code .c{cell}} segment names the cell that <em>minted</em> the id. Epoch and
 * shard say where an instance belongs under the current ring, which is what makes resharding
 * possible; the cell says where it was actually written, which is what makes routing possible with
 * no ring at all. A deployment that never reshards can route on the label alone and ignore the
 * placement policy entirely.
 *
 * <p>It sits between the namespace and the epoch rather than after the shard, and that position is
 * load-bearing. A ulid may contain dots ({@link #format} only forbids them in the namespace and the
 * cell), so an optional trailing segment would be ambiguous: the legacy id
 * {@code ns.e0.s0.cfoo.bar} would parse as cell {@code foo} with ulid {@code bar}. Anchored between
 * two fixed markers it cannot be confused with anything.
 *
 * <p>Legacy ids (a bare {@code wfi_...} minted before a namespace was configured, or pre-adoption)
 * do not match and {@link #parse} returns empty; callers route those to the genesis cell (§7).
 */
public final class IdCodec {

    // namespace (no '.'), optional .c<cell> (no '.'), then .e<digits> .s<digits> . <ulid rest>
    private static final Pattern PATTERN =
            Pattern.compile("^([^.]+)(?:\\.c([^.]+))?\\.e(\\d+)\\.s(\\d+)\\.(.+)$");

    /** Instance-id columns are {@code VARCHAR(64)}; minting something longer fails at insert. */
    public static final int MAX_LENGTH = 64;

    private IdCodec() {}

    /**
     * A parsed id. {@code cellId} is null for an id minted before cells were stamped, which is not
     * an error -- it means "ask the placement policy", exactly as before.
     */
    public record Placement(String namespace, String cellId, long epoch, long shard, String ulid) {

        /** True when the id names the cell that minted it, so routing needs no ring. */
        public boolean hasCell() { return cellId != null; }
    }

    /** Builds an id with no cell label -- the pre-cell format, still minted by a cell that has no id. */
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
            // Fail here rather than at the insert, where the message is about a column
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
     * The shard a new id lands on: a well-mixed hash of the ulid reduced to {@code [0, ringSize)}
     * (0 for a single-cell ring). Deterministic across JVMs -- pure integer arithmetic on the ulid's
     * code units -- so the same ulid always maps to the same shard. The result is stamped into the id
     * at mint time and read back verbatim on resolve, never recomputed.
     */
    // docs:begin shard-for
    public static long shardFor(String ulid, int ringSize) {
        return ringSize <= 1 ? 0 : Math.floorMod(hash64(ulid), ringSize);
    }
    // docs:end shard-for

    /**
     * 64-bit FNV-1a over the ulid, finished with a murmur3 fmix64 avalanche so every input bit affects
     * every output bit. {@link String#hashCode()} barely mixes and clusters on the shared timestamp
     * prefix of ULIDs minted close together; the finalizer is what spreads the random suffix evenly.
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
