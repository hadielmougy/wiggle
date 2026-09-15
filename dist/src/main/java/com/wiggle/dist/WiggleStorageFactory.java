package com.wiggle.dist;

import com.wiggle.postgres.PostgresStorageFactory;

/**
 * The standalone server's storage selection.
 *
 * <p>The mapping itself moved to {@link PostgresStorageFactory}, in the published {@code postgres}
 * module, because an application that <em>embeds</em> the engine needs the same switch and cannot
 * depend on this one: {@code dist} is the runnable distribution and is not published. The name stays
 * here so the image's entry point and anything referencing it are unaffected.
 */
public final class WiggleStorageFactory extends PostgresStorageFactory {
}
