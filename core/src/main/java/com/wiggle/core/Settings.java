package com.wiggle.core;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * Where a setting is read from, by its two names: a property name ({@code wiggle.jdbc.url}) and an
 * environment name ({@code WIGGLE_JDBC_URL}). The property wins when both are set; a blank value
 * counts as unset.
 */
@FunctionalInterface
public interface Settings {

    /** The raw value of the setting, or null when neither name is set. */
    String raw(String property, String env);

    /** System properties, then the process environment. */
    static Settings system() {
        return (property, env) -> {
            String v = property == null ? null : System.getProperty(property);
            return v != null ? v : env == null ? null : System.getenv(env);
        };
    }

    /** The value of the setting, or null when it is unset or blank. */
    default String get(String property, String env) {
        String v = raw(property, env);
        return v == null || v.isBlank() ? null : v;
    }

    default String get(String property, String env, String def) {
        String v = get(property, env);
        return v == null ? def : v;
    }

    default int getInt(String property, String env, int def) {
        String v = get(property, env);
        return v == null ? def : Integer.parseInt(v.trim());
    }

    default long getLong(String property, String env, long def) {
        String v = get(property, env);
        return v == null ? def : Long.parseLong(v.trim());
    }

    default boolean getBoolean(String property, String env, boolean def) {
        String v = get(property, env);
        return v == null ? def : Boolean.parseBoolean(v.trim());
    }

    default double getDouble(String property, String env, double def) {
        String v = get(property, env);
        return v == null ? def : Double.parseDouble(v.trim());
    }

    /** The settings by environment name, as a map that answers {@code get} only. */
    default Map<String, String> byEnvName() {
        return new AbstractMap<>() {
            @Override public String get(Object key) {
                return key instanceof String name ? raw(null, name) : null;
            }

            @Override public Set<Entry<String, String>> entrySet() {
                throw new UnsupportedOperationException("settings are read by name");
            }
        };
    }
}
