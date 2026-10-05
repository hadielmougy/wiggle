package com.wiggle.server.auth;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The names a caller may act on: every name, or some exact names plus some prefixes. A prefix is
 * kept with its trailing dot ({@code acme.}), so it matches {@code acme.orders} but neither
 * {@code acme} nor {@code acmex.orders}.
 */
public record Scope(boolean all, Set<String> names, Set<String> prefixes) {

    public static final Scope ALL = new Scope(true, Set.of(), Set.of());
    public static final Scope NONE = new Scope(false, Set.of(), Set.of());

    public Scope {
        names = all ? Set.of() : Set.copyOf(names);
        prefixes = all ? Set.of() : Set.copyOf(prefixes);
    }

    /** Exactly {@code names}. */
    public static Scope of(Collection<String> names) {
        return new Scope(false, Set.copyOf(names), Set.of());
    }

    /** Exactly {@code names}. */
    public static Scope of(String... names) {
        return of(Set.of(names));
    }

    /** The scopes of permissions as written: an exact name, or a prefix ending in {@code .*}. */
    public static Scope parse(Collection<String> scopes) {
        Set<String> names = new LinkedHashSet<>();
        Set<String> prefixes = new LinkedHashSet<>();
        for (String s : scopes) {
            if (s.endsWith(".*")) prefixes.add(s.substring(0, s.length() - 1));
            else names.add(s);
        }
        return new Scope(false, names, prefixes);
    }

    public boolean matches(String name) {
        if (all) return true;
        if (name == null) return false;
        if (names.contains(name)) return true;
        for (String p : prefixes) if (name.startsWith(p)) return true;
        return false;
    }

    /** Whether no name matches. */
    public boolean isEmpty() {
        return !all && names.isEmpty() && prefixes.isEmpty();
    }

    /** This scope narrowed to {@code name}, or itself when {@code name} is null. */
    public Scope narrow(String name) {
        if (name == null) return this;
        return matches(name) ? of(name) : NONE;
    }
}
