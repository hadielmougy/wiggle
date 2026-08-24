dependencies {
    // The Oracle dialect builds on the shared, dialect-aware JDBC store, which brings the server
    // API and HikariCP with it transitively.
    api(project(":jdbc"))

    // The driver is loaded via the connection pool, so it is only needed at runtime.
    // ojdbc11 targets JDK 11+ (this project runs on 21).
    runtimeOnly("com.oracle.database.jdbc:ojdbc11:${property("oracleVersion")}")
}
