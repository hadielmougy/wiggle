package com.wiggle.console;

import org.apache.catalina.Context;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;

/** A Tomcat of its own for the portal's servlet and sign-in filter, so a test can drive them over HTTP. */
final class ConsoleServer implements AutoCloseable {

    private final Tomcat tomcat;

    ConsoleServer(DashboardData data, ConsoleAuth auth, int port) {
        this(new DashboardServlet(data, auth), new AuthFilter(auth), port);
    }

    ConsoleServer(DashboardServlet servlet, AuthFilter filter, int port) {
        this.tomcat = new Tomcat();
        this.tomcat.setBaseDir(System.getProperty("java.io.tmpdir"));

        Connector connector = new Connector("HTTP/1.1");
        connector.setPort(port);
        tomcat.setConnector(connector);

        Context ctx = tomcat.addContext("", null);
        Tomcat.addServlet(ctx, "dashboard", servlet);
        ctx.addServletMappingDecoded("/*", "dashboard");

        FilterDef fd = new FilterDef();
        fd.setFilterName("auth");
        fd.setFilter(filter);
        ctx.addFilterDef(fd);
        FilterMap fm = new FilterMap();
        fm.setFilterName("auth");
        fm.addURLPattern("/*");
        ctx.addFilterMap(fm);
    }

    ConsoleServer start() {
        try {
            tomcat.start();
        } catch (org.apache.catalina.LifecycleException e) {
            throw new IllegalStateException("failed to start console on Tomcat: " + e.getMessage(), e);
        }
        return this;
    }

    int port() {
        return tomcat.getConnector().getLocalPort();
    }

    @Override public void close() {
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch (org.apache.catalina.LifecycleException ignored) {
            // best effort on shutdown
        }
    }
}
