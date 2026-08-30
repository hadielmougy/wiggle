package com.wiggle.core;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The epoch-aware instance id: {@code {namespace}.e{epoch}.s{shard}.{ulid}}. The id <em>is</em> the
 * routing record -- an instance's cell is a pure function of its id plus the (bounded) placement
 * policy, so the coordinator never stores a per-instance directory (R16). See the design reference §6.
 *
 * <p>Legacy ids (a bare {@code wfi_...} minted before a namespace was configured, or pre-adoption)
 * do not match and {@link #parse} returns empty; callers route those to the genesis cell (§7).
 */
public final class IdCodec {

    // namespace has no '.', then .e<digits> .s<digits> . <ulid rest>
    private static final Pattern PATTERN = Pattern.compile("^([^.]+)\\.e(\\d+)\\.s(\\d+)\\.(.+)$");

    private IdCodec() {}

    /** A parsed epoch-aware id. */
    public record Placement(String namespace, long epoch, long shard, String ulid) {}

    /** Builds an id. The namespace must not contain '.' (it is the id's first segment). */
    public static String format(String namespace, long epoch, long shard, String ulid) {
        if (namespace == null || namespace.isEmpty() || namespace.indexOf('.') >= 0) {
            throw new IllegalArgumentException("namespace must be non-empty and contain no '.': '" + namespace + "'");
        }
        return namespace + ".e" + epoch + ".s" + shard + "." + ulid;
    }

    /** Parses an epoch-aware id, or empty for a legacy id. */
    public static Optional<Placement> parse(String id) {
        if (id == null) return Optional.empty();
        var m = PATTERN.matcher(id);
        if (!m.matches()) return Optional.empty();
        try {
            return Optional.of(new Placement(m.group(1), Long.parseLong(m.group(2)),
                    Long.parseLong(m.group(3)), m.group(4)));
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
    public static long shardFor(String ulid, int ringSize) {
        return ringSize <= 1 ? 0 : Math.floorMod(hash64(ulid), ringSize);
    }

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
