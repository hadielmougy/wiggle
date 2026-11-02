package com.wiggle.boot;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;

/**
 * Serves Actuator on the probe port, {@code WIGGLE_DASHBOARD_PORT}, and nowhere when it is unset or 0.
 * A {@code management.server.port} set anywhere else wins.
 */
public final class ProbePort implements EnvironmentPostProcessor {

    private static final String NAME = "management.server.port";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        environment.getPropertySources().addLast(new EnumerablePropertySource<>("wiggle-probe-port", NAME) {
            @Override public String[] getPropertyNames() {
                return new String[] {NAME};
            }

            @Override public Object getProperty(String name) {
                if (!NAME.equals(name)) return null;
                int port = NodeConfiguration.settings(environment)
                        .getInt("wiggle.dashboard.port", "WIGGLE_DASHBOARD_PORT", 0);
                return port > 0 ? port : -1;
            }
        });
    }
}
