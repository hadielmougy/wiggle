plugins {
    application
}

dependencies {
    api(project(":core"))
    api(project(":proto"))

    // Only needed when running with WIGGLE_JDBC_URL set. The in-memory store,
    // which is the default, needs nothing at all.
    runtimeOnly("org.postgresql:postgresql:${property("postgresVersion")}")
    runtimeOnly("com.h2database:h2:${property("h2Version")}")
}

application {
    mainClass.set("dev.wiggle.server.WiggleServer")
}
