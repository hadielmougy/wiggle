package com.wiggle.core;

import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ids that carry the shard holding their instance: {@code {prefix}.s{shard}.{rest}}, e.g.
 * {@code wfi.s3.01k6...}. The shard is fixed when the id is minted and read back verbatim, never
 * recomputed. An id in any other shape -- a bare {@code wfi_...}, or one minted under the removed
 * cell coordinator -- carries no shard and belongs to the home shard.
 *
 * <p>An id derived from another (a token from its instance, a child from the token that starts it)
 * {@linkplain #inherit inherits} the owner's form: the owner's shard when it has one, the bare form
 * when it has none. Either way the derived id routes to wherever its owner lives.
 */
public final class ShardIds {

    /** Instance-id columns hold 128 characters. */
    public static final int MAX_LENGTH = 128;

    private static final Pattern SHARDED = Pattern.compile("^([a-z]+)\\.s(0|[1-9][0-9]{0,8})\\.([^.]+)$");
    private static final Pattern PREFIX = Pattern.compile("^[a-z]+$");

    private ShardIds() {}

    /** A new id on {@code shard}. */
    public static String next(String prefix, int shard) {
        return format(prefix, shard, Ids.token());
    }

    /**
     * Builds {@code {prefix}.s{shard}.{rest}}. The prefix is lowercase letters; {@code rest} is
     * non-empty and holds no {@code '.'}.
     */
    public static String format(String prefix, int shard, String rest) {
        if (prefix == null || !PREFIX.matcher(prefix).matches()) {
            throw new IllegalArgumentException("an id prefix is lowercase letters: '" + prefix + "'");
        }
        if (shard < 0) throw new IllegalArgumentException("a shard is not negative: " + shard);
        if (rest == null || rest.isEmpty() || rest.indexOf('.') >= 0) {
            throw new IllegalArgumentException("an id's last segment is non-empty and holds no '.': '"
                    + rest + "'");
        }
        String id = prefix + ".s" + shard + "." + rest;
        if (id.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("id would be " + id.length() + " characters, over the "
                    + MAX_LENGTH + " an id column holds");
        }
        return id;
    }

    /** The shard {@code id} carries, or empty when it carries none and so belongs to the home shard. */
    public static OptionalInt shardOf(String id) {
        if (id == null) return OptionalInt.empty();
        Matcher m = SHARDED.matcher(id);
        return m.matches() ? OptionalInt.of(Integer.parseInt(m.group(2))) : OptionalInt.empty();
    }

    /** A new id for something {@code ownerId} owns, on the owner's shard, in the owner's form. */
    public static String inherit(String prefix, String ownerId) {
        OptionalInt shard = shardOf(ownerId);
        return shard.isPresent() ? next(prefix, shard.getAsInt()) : Ids.next(prefix);
    }
}
