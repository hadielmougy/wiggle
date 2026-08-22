dependencies {
    // The JDBC store implements the server's Storage SPI; it needs the server API to compile,
    // but never the other way round -- the server core has no knowledge of this module.
    api(project(":server"))

    // Drivers are loaded via java.sql.DriverManager, so they are only needed at runtime.
    // PostgreSQL for production; H2 (in PostgreSQL-compatibility mode) for dev and tests.
    runtimeOnly("org.postgresql:postgresql:${property("postgresVersion")}")
    runtimeOnly("com.h2database:h2:${property("h2Version")}")
}
