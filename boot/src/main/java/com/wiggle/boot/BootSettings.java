package com.wiggle.boot;

import com.wiggle.core.Settings;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Boot's own settings from the {@code WIGGLE_*} ones, each read when it is asked for. Any of them set
 * anywhere else wins.
 * <ul>
 *   <li>{@code management.server.port}: Actuator on the probe port, {@code WIGGLE_DASHBOARD_PORT}, and
 *       nowhere when it is unset or 0;</li>
 *   <li>{@code logging.file.name}: {@code WIGGLE_LOG_FILE}, when set;</li>
 *   <li>{@code logging.level.com.wiggle}: {@code WIGGLE_LOG_LEVEL}, a {@link System.Logger.Level} name,
 *       when set.</li>
 * </ul>
 */
public final class BootSettings implements EnvironmentPostProcessor {

    private static final Map<String, Function<Settings, Object>> DERIVED = Map.of(
            "management.server.port", s -> {
                int port = s.getInt("wiggle.dashboard.port", "WIGGLE_DASHBOARD_PORT", 0);
                return port > 0 ? port : -1;
            },
            "logging.file.name", s -> s.get("wiggle.log.file", "WIGGLE_LOG_FILE"),
            "logging.level.com.wiggle", s -> logLevel(s.get("wiggle.log.level", "WIGGLE_LOG_LEVEL")));

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        environment.getPropertySources().addLast(new EnumerablePropertySource<>("wiggle-derived", DERIVED) {
            @Override public String[] getPropertyNames() {
                return DERIVED.keySet().toArray(String[]::new);
            }

            @Override public Object getProperty(String name) {
                Function<Settings, Object> derive = DERIVED.get(name);
                return derive == null ? null : derive.apply(NodeConfiguration.settings(environment));
            }
        });
    }

    /** {@code level}, a {@link System.Logger.Level} name, as Boot names it; null when unset. */
    static String logLevel(String level) {
        if (level == null) return null;
        return switch (level.trim().toUpperCase(Locale.ROOT)) {
            case "ALL" -> "TRACE";
            case "WARNING" -> "WARN";
            case "TRACE", "DEBUG", "INFO", "ERROR", "OFF" -> level.trim().toUpperCase(Locale.ROOT);
            default -> throw new IllegalArgumentException("WIGGLE_LOG_LEVEL='" + level
                    + "' is not one of ALL, TRACE, DEBUG, INFO, WARNING, ERROR, OFF");
        };
    }
}
