dependencies {
    // The JDBC store implements the server's Storage SPI; it needs the server API to compile,
    // but never the other way round -- the server core has no knowledge of this module.
    api(project(":server"))

    // Connection pooling for every dialect. Exposed as `api` so the per-database modules
    // (postgres/mysql/oracle) inherit it without re-declaring it.
    api("com.zaxxer:HikariCP:${property("hikariVersion")}")
}
