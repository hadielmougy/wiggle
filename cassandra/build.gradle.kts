dependencies {
    // Cassandra is NOT a JDBC dialect: it implements the server's Storage SPI directly on the
    // DataStax driver (CQL, lightweight transactions, partition-aware data model). It needs only
    // the server API to compile.
    api(project(":server"))
    // Also implements the coordinator's CoordinatorStoreProvider seam (a CQL coord store, LWT policy CAS).
    api(project(":coordinator:spi"))

    // The CQL driver is used at both compile and runtime (unlike the JDBC drivers, which load via
    // DriverManager), because we call its typed API directly.
    implementation("org.apache.cassandra:java-driver-core:${property("cassandraDriverVersion")}")
}
