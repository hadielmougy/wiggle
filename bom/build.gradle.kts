// The Wiggle BOM: a version-alignment platform. A consumer imports it once
//
//     implementation(platform("sh.wiggle:wiggle-bom:<version>"))
//     implementation("sh.wiggle:wiggle-client")     // no version — the BOM supplies it
//
// and every wiggle module, the shared gRPC/protobuf stack, and the storage drivers resolve to one
// coherent set. This removes the per-artifact version pins (and the resolutionStrategy.force hacks)
// that otherwise force enterprise consumers to edit their build files.
plugins {
    `java-platform`
}

javaPlatform {
    // so the platform may import the gRPC BOM and re-export its aligned constraints
    allowDependencies()
}

dependencies {
    // Align the whole gRPC family (grpc-api/stub/protobuf/netty-shaded/...) from its own BOM.
    api(platform("io.grpc:grpc-bom:${property("grpcVersion")}"))

    constraints {
        // --- wiggle's own modules, at this build's version ---
        listOf(
            "wiggle-core", "wiggle-proto", "wiggle-client", "wiggle-server",
            "wiggle-jdbc", "wiggle-postgres", "wiggle-mysql", "wiggle-oracle",
            "wiggle-sqlserver", "wiggle-client-all",
        ).forEach { api("sh.wiggle:$it:$version") }

        // --- the control-plane stack wiggle-proto exposes transitively ---
        api("com.google.protobuf:protobuf-java:${property("protobufVersion")}")
        api("io.github.hadielmougy:shield:${property("shieldVersion")}")
        api("com.zaxxer:HikariCP:${property("hikariVersion")}")

        // --- JDBC drivers each wiggle-<db> module brings in at runtime ---
        api("org.postgresql:postgresql:${property("postgresVersion")}")
        api("com.h2database:h2:${property("h2Version")}")
        api("com.mysql:mysql-connector-j:${property("mysqlVersion")}")
        api("com.oracle.database.jdbc:ojdbc11:${property("oracleVersion")}")
        api("com.microsoft.sqlserver:mssql-jdbc:${property("sqlserverVersion")}")
    }
}

// The BOM has no code and is a java-platform, so tell the vanniktech plugin (applied by the root
// build) to publish the platform component rather than auto-detecting a java library.
mavenPublishing {
    configure(com.vanniktech.maven.publish.JavaPlatform())
}
