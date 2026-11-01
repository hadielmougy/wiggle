// The Wiggle BOM: a version-alignment platform. A consumer imports it once
//
//     implementation(platform("sh.wiggle:wiggle-bom:<version>"))
//     implementation("sh.wiggle:wiggle-client")     // no version — the BOM supplies it
//
// and every wiggle module, Jackson, the client's retry library, and the storage drivers resolve to one
// coherent set. This removes the per-artifact version pins (and the resolutionStrategy.force hacks)
// that otherwise force enterprise consumers to edit their build files.
plugins {
    `java-platform`
}

dependencies {
    constraints {
        // --- wiggle's own modules, at this build's version ---
        listOf(
            "wiggle-core", "wiggle-client", "wiggle-server",
            "wiggle-jdbc", "wiggle-postgres", "wiggle-client-all",
            // wiggle-server has an api dependency on it, so a consumer resolves it whether or
            // not they name it; the BOM should pin it like everything else it can drag in.
            "wiggle-election",
        ).forEach { api("sh.wiggle:$it:$version") }

        // --- what the core, the client and the JDBC store bring in ---
        api("com.fasterxml.jackson.core:jackson-core:${property("jacksonVersion")}")
        api("io.github.hadielmougy:shield:${property("shieldVersion")}")
        api("com.zaxxer:HikariCP:${property("hikariVersion")}")
        api("org.springframework:spring-jdbc:${property("springFrameworkVersion")}")
        api("org.springframework:spring-tx:${property("springFrameworkVersion")}")

        // --- JDBC drivers wiggle-postgres brings in at runtime (H2 is for tests/local runs) ---
        api("org.postgresql:postgresql:${property("postgresVersion")}")
        api("com.h2database:h2:${property("h2Version")}")
    }
}

// The BOM has no code and is a java-platform, so tell the vanniktech plugin (applied by the root
// build) to publish the platform component rather than auto-detecting a java library.
mavenPublishing {
    configure(com.vanniktech.maven.publish.JavaPlatform())
}
