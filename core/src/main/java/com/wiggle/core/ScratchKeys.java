package com.wiggle.core;

/**
 * The reserved keys a join's results are staged under on their way into a combine handler.
 *
 * <p>A combine's inputs travel in the same JSON object as the workflow's own data -- there is one
 * context, and the staged branch results ride in it from the join until the combine has run, at
 * which point they are stripped. So a staging key that looked like an ordinary key would collide
 * with one, and the collision is not hypothetical: the staged value overwrites the real one, and
 * the strip afterwards removes the key outright, taking the user's data with it. Silently.
 *
 * <p>Both shapes are derived from a name the author chose, which is exactly why they need the
 * prefix:
 * <ul>
 *   <li><b>forEach</b> stages under its fan-out node's name -- and that name <em>defaults to the
 *       collection key</em>, so the unprefixed form collided with the very collection it fanned
 *       over, in the most ordinary spelling of the construct;</li>
 *   <li><b>fork</b> stages each arm under the arm's name, which is its branch's last step name. A
 *       step called {@code payment} beside a context key called {@code payment} is unusual rather
 *       than inevitable, but it fails the same way when it happens.</li>
 * </ul>
 *
 * <p>The {@code __x__} convention is the one the engine's other internal keys already use
 * ({@code __loops__}, {@code __armIdx__}, {@code __item__}). These two are declared here, in core,
 * because the server writes them and the worker reads them: they are part of the contract between
 * the two, not an implementation detail of either.
 */
public final class ScratchKeys {

    private ScratchKeys() {}

    /** Where a fork arm's final view is staged for the combine that takes it. */
    public static String arm(String armName) {
        return "__arm__" + armName;
    }

    /** Where a forEach's collected item results are staged for its mandatory combine. */
    public static String forEach(String forEachNodeName) {
        return "__forEach__" + forEachNodeName;
    }
}
