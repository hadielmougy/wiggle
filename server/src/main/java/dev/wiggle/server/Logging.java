package dev.wiggle.server;

import java.io.IOException;
import java.util.Locale;
import java.util.logging.*;

/**
 * Optional file logging for the standalone server, using the JDK's own {@code java.util.logging}
 * so there is no dependency to add. Wiggle logs through {@link System.Logger}, which routes to
 * java.util.logging by default; this simply attaches a rotating {@link FileHandler}.
 *
 * <p>Enable it with {@code WIGGLE_LOG_FILE} (and optionally {@code WIGGLE_LOG_LEVEL}). For full
 * control instead, set {@code -Djava.util.logging.config.file=…} and a {@code logging.properties};
 * when that system property is present this helper stands aside.
 */
public final class Logging {

    private static final long MAX_BYTES = 10L * 1024 * 1024;   // 10 MB per file
    private static final int FILES = 5;                        // rotate across 5 generations

    private Logging() {}

    /** Configures file logging from {@code WIGGLE_LOG_FILE} / {@code WIGGLE_LOG_LEVEL}. */
    public static void configureFromEnv() {
        configure(System.getenv("WIGGLE_LOG_FILE"), System.getenv("WIGGLE_LOG_LEVEL"));
    }

    /**
     * Attaches a rotating file handler for the given path. No-op when {@code file} is blank or an
     * explicit {@code java.util.logging.config.file} is in effect. {@code level} is a
     * {@link System.Logger.Level} name (INFO by default); Wiggle's own package is raised to it so
     * DEBUG lines are captured, while third-party loggers keep their defaults.
     *
     * @return the attached handler (so callers/tests can flush or close it), or {@code null}
     */
    public static Handler configure(String file, String level) {
        if (file == null || file.isBlank()) return null;
        if (System.getProperty("java.util.logging.config.file") != null) return null;

        Level julLevel = toJulLevel(level);
        try {
            FileHandler handler = new FileHandler(file, MAX_BYTES, FILES, true);
            handler.setFormatter(new SimpleFormatter());
            handler.setLevel(julLevel);
            Logger.getLogger("").addHandler(handler);           // root: capture every logger
            Logger.getLogger("dev.wiggle").setLevel(julLevel);  // but only raise our own verbosity
            return handler;
        } catch (IOException e) {
            System.err.println("wiggle: could not open log file '" + file + "': " + e.getMessage());
            return null;
        }
    }

    /** Maps a {@link System.Logger.Level} name to the java.util.logging level it routes to. */
    private static Level toJulLevel(String name) {
        if (name == null || name.isBlank()) return Level.INFO;
        return switch (name.trim().toUpperCase(Locale.ROOT)) {
            case "OFF" -> Level.OFF;
            case "ERROR" -> Level.SEVERE;
            case "WARNING" -> Level.WARNING;
            case "INFO" -> Level.INFO;
            case "DEBUG" -> Level.FINE;
            case "TRACE" -> Level.FINER;
            case "ALL" -> Level.ALL;
            default -> Level.INFO;
        };
    }
}
