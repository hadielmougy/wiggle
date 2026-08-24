dependencies {
    // The PostgreSQL/H2 dialects build on the shared, dialect-aware JDBC store, which brings the
    // server API and HikariCP with it transitively.
    api(project(":jdbc"))

    // Drivers are loaded via the connection pool, so they are only needed at runtime.
    // PostgreSQL for production; H2 (in PostgreSQL-compatibility mode) for dev and tests.
    runtimeOnly("org.postgresql:postgresql:${property("postgresVersion")}")
    runtimeOnly("com.h2database:h2:${property("h2Version")}")
}
