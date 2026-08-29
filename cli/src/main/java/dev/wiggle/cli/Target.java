package dev.wiggle.cli;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

/**
 * The server the {@code wiggle} CLI talks to: a {@link Kind#COORDINATOR} or a single {@link Kind#CELL},
 * at an address. It is persisted (like {@code kubectl}/{@code docker context}) so a target set once with
 * {@code wiggle use} sticks across invocations.
 *
 * <p>Resolution precedence, per command: an explicit flag ({@code --coordinator}/{@code --server}) wins,
 * then the matching environment variable, then the saved target (if its kind matches what the command
 * needs), then a built-in default. A saved target of the wrong kind is reported, not silently misused.
 */
public record Target(Kind kind, String address) {

    public enum Kind {
        COORDINATOR("WIGGLE_COORDINATOR_URL", "127.0.0.1:8099"),
        CELL("WIGGLE_URL", "localhost:8080");

        final String env;
        final String fallback;
        Kind(String env, String fallback) { this.env = env; this.fallback = fallback; }
    }

    /** {@code ~/.wiggle/target.properties}; override the dir with {@code -Dwiggle.config.home} or
     *  {@code $WIGGLE_CONFIG_HOME} (the system property wins, which also makes this testable). */
    public static Path configFile() {
        String home = System.getProperty("wiggle.config.home");
        if (home == null || home.isBlank()) home = System.getenv("WIGGLE_CONFIG_HOME");
        Path dir = (home != null && !home.isBlank())
                ? Path.of(home)
                : Path.of(System.getProperty("user.home"), ".wiggle");
        return dir.resolve("target.properties");
    }

    public static Optional<Target> load() {
        Path f = configFile();
        if (!Files.exists(f)) return Optional.empty();
        try {
            Properties p = new Properties();
            try (var in = Files.newInputStream(f)) { p.load(in); }
            String kind = p.getProperty("kind");
            String address = p.getProperty("address");
            if (kind == null || address == null || address.isBlank()) return Optional.empty();
            return Optional.of(new Target(Kind.valueOf(kind.toUpperCase()), address.trim()));
        } catch (IOException | IllegalArgumentException e) {
            return Optional.empty();   // a corrupt file is treated as "no saved target"
        }
    }

    public void save() {
        Path f = configFile();
        try {
            Files.createDirectories(f.getParent());
            Properties p = new Properties();
            p.setProperty("kind", kind.name());
            p.setProperty("address", address);
            try (var out = Files.newOutputStream(f)) { p.store(out, "wiggle CLI target"); }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot save target to " + f, e);
        }
    }

    public static void clear() {
        try { Files.deleteIfExists(configFile()); }
        catch (IOException e) { throw new UncheckedIOException("cannot clear target", e); }
    }

    /**
     * Resolves the address for a command needing {@code want}: {@code flag} first, then the kind's
     * environment variable, then the saved target (if its kind matches), then the kind's default.
     *
     * @throws IllegalStateException if the saved target is of a different kind and no flag/env overrides it
     */
    public static String resolve(Kind want, String flag) {
        if (flag != null && !flag.isBlank()) return flag;
        String env = System.getenv(want.env);
        if (env != null && !env.isBlank()) return env;
        Optional<Target> saved = load();
        if (saved.isPresent()) {
            Target t = saved.get();
            if (t.kind() == want) return t.address();
            throw new IllegalStateException(
                    "current target is a " + t.kind() + " (" + t.address() + "), but this command needs a "
                    + want + ". Run `wiggle use " + want.name().toLowerCase() + " <addr>`, or pass "
                    + (want == Kind.COORDINATOR ? "--coordinator" : "--server") + ".");
        }
        return want.fallback;
    }

    @Override public String toString() {
        return kind.name().toLowerCase() + " @ " + address;
    }
}
