dependencies {
    // The SQL Server dialect builds on the shared, dialect-aware JDBC store, which brings the
    // server API and HikariCP with it transitively.
    api(project(":jdbc"))

    // The driver is loaded via the connection pool, so it is only needed at runtime.
    // The jre11 classifier is compatible with JDK 21.
    runtimeOnly("com.microsoft.sqlserver:mssql-jdbc:${property("sqlserverVersion")}")
}
