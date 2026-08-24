dependencies {
    // The MySQL dialect builds on the shared, dialect-aware JDBC store, which brings the server
    // API and HikariCP with it transitively.
    api(project(":jdbc"))

    // The driver is loaded via the connection pool, so it is only needed at runtime. The
    // connector also speaks MariaDB.
    runtimeOnly("com.mysql:mysql-connector-j:${property("mysqlVersion")}")
}
