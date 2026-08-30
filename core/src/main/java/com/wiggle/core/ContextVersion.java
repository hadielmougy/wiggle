package com.wiggle.core;

/**
 * The schema version an activity's context was persisted at, published by a
 * {@link VersionedContextCodec} while it decodes. An activity can read {@link #current()} to
 * branch on where its data came from -- e.g. to treat an instance created under an older schema
 * specially -- without threading the version through its signature. The {@code data} itself is
 * always upcast to the current shape before the activity sees it; this value reports where it
 * came from.
 *
 * <p>The value is the version as stored at read time: a migrated instance reports its old version
 * on the first step that touches it, then -- once that step re-stores the context at the current
 * version -- reports the current version thereafter. For an origin marker that survives every step,
 * stamp it into the data from an upcast (see {@link VersionedContextCodec}).
 *
 * <p>Returns {@code 0} when the context is not a versioned envelope, or outside a
 * running activity.
 */
public final class ContextVersion {

    private static final ThreadLocal<Integer> CURRENT = new ThreadLocal<>();

    private ContextVersion() {}

    /** The origin schema version of the context on this thread, or {@code 0} if unversioned. */
    public static int current() {
        Integer v = CURRENT.get();
        return v == null ? 0 : v;
    }

    /** Published by {@link VersionedContextCodec} during {@code decode}. */
    static void set(int version) { CURRENT.set(version); }

    /** Cleared by the worker runtime once an activity returns; safe to call when unset. */
    public static void clear() { CURRENT.remove(); }
}
