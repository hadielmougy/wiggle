package com.wiggle.server.auth;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * What a role may do. A permission is an action, optionally scoped to one workflow or queue as
 * {@code action:scope}; {@value #ALL} grants every action. An unscoped action covers every scope.
 */
public final class Permissions {

    public static final String ALL = "*";
    /** Every read the portal serves. */
    public static final String PORTAL_READ = "portal.read";
    public static final String INSTANCE_CANCEL = "instance.cancel";
    public static final String INSTANCE_SIGNAL = "instance.signal";
    public static final String INSTANCE_START = "instance.start";
    public static final String SCHEDULE_WRITE = "schedule.write";
    public static final String USER_MANAGE = "user.manage";
    public static final String TASK_POLL = "task.poll";

    /** Every action a permission may name, and whether it takes a scope. */
    public static final List<String> ACTIONS = List.of(PORTAL_READ, INSTANCE_CANCEL, INSTANCE_SIGNAL,
            INSTANCE_START, SCHEDULE_WRITE, USER_MANAGE, TASK_POLL);
    private static final Set<String> SCOPED = Set.of(INSTANCE_CANCEL, INSTANCE_SIGNAL, INSTANCE_START,
            SCHEDULE_WRITE, TASK_POLL);
    private static final Pattern SCOPE = Pattern.compile("[A-Za-z0-9._:/-]{1,200}");

    public static final String ADMIN = "admin";
    public static final String VIEWER = "viewer";

    /** The roles every deployment has, which cannot be changed or deleted. */
    public static final Map<String, Set<String>> BUILTIN_ROLES = Map.of(
            ADMIN, Set.of(ALL),
            VIEWER, Set.of(PORTAL_READ));

    /** Whether {@code granted} allows {@code action}, on {@code scope} when it is non-null. */
    public static boolean allows(Set<String> granted, String action, String scope) {
        if (granted.contains(ALL) || granted.contains(action)) return true;
        return scope != null && granted.contains(action + ":" + scope);
    }

    /** {@code permissions} with each one checked; throws naming the first that is not a permission. */
    public static Set<String> validate(Iterable<String> permissions) {
        Set<String> out = new LinkedHashSet<>();
        for (String raw : permissions) {
            String p = raw == null ? "" : raw.trim();
            if (p.equals(ALL)) { out.add(p); continue; }
            int colon = p.indexOf(':');
            String action = colon < 0 ? p : p.substring(0, colon);
            if (!ACTIONS.contains(action)) {
                throw new IllegalArgumentException("'" + p + "' is not a permission; the actions are " + ACTIONS
                        + " and " + ALL);
            }
            if (colon >= 0) {
                if (!SCOPED.contains(action)) {
                    throw new IllegalArgumentException("'" + action + "' takes no scope: '" + p + "'");
                }
                if (!SCOPE.matcher(p.substring(colon + 1)).matches()) {
                    throw new IllegalArgumentException("'" + p + "' has an empty or malformed scope");
                }
            }
            out.add(p);
        }
        return out;
    }

    private Permissions() { }
}
