package com.wiggle.boot.test;

import com.wiggle.testfixtures.TestPorts;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.Profiles;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A test node's settings: fast timings, ports from {@link TestPorts}, and none of the process's own
 * {@code WIGGLE_*} variables but {@code WIGGLE_TEST_*}. Anything a test sets wins over these.
 */
public final class TestNodeInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    /** The settings every test node starts from. */
    static Map<String, Object> defaults(ConfigurableEnvironment environment) {
        Map<String, Object> d = new HashMap<>();
        d.put("WIGGLE_PORT", TestPorts.free());
        d.put("WIGGLE_PORTAL_PORT", environment.acceptsProfiles(Profiles.of("portal")) ? TestPorts.free() : 0);
        d.put("WIGGLE_DASHBOARD_PORT", environment.acceptsProfiles(Profiles.of("probe")) ? TestPorts.free() : 0);
        d.put("WIGGLE_NODE_NAME", "test-" + UUID.randomUUID().toString().substring(0, 8));
        d.put("WIGGLE_JDBC_POOL_SIZE", 4);
        d.put("WIGGLE_POLL_INTERVAL_MILLIS", 100);
        d.put("WIGGLE_HEARTBEAT_INTERVAL_MILLIS", 500);
        d.put("WIGGLE_MISSED_HEARTBEATS", 3);
        d.put("WIGGLE_LEASE_MILLIS", 20_000);
        d.put("WIGGLE_LONGPOLL_MAX_MILLIS", 500);
        d.put("WIGGLE_RETENTION_MILLIS", 3_600_000);
        return d;
    }

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        apply(context.getEnvironment(), Map.of());
    }

    /** Applies the test settings to {@code environment}, {@code overrides} on top of the defaults. */
    static void apply(ConfigurableEnvironment environment, Map<String, Object> overrides) {
        var sources = environment.getPropertySources();
        String env = StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME;
        if (sources.get(env) instanceof SystemEnvironmentPropertySource system) {
            Map<String, Object> kept = new HashMap<>();
            system.getSource().forEach((k, v) -> {
                if (!k.startsWith("WIGGLE_") || k.startsWith("WIGGLE_TEST_")) kept.put(k, v);
            });
            sources.replace(env, new SystemEnvironmentPropertySource(env, kept));
        }
        Map<String, Object> settings = defaults(environment);
        settings.putAll(overrides);
        PropertySource<?> test = new MapPropertySource("wiggle-test-node", settings);
        sources.addLast(test);
    }
}
