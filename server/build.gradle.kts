plugins {
    application
}

dependencies {
    api(project(":core"))
    api(project(":proto"))

    // The standalone distribution ships with PostgreSQL support so it works out of the box
    // against a database; the server code itself never references this module (it resolves a
    // store via the StorageProvider SPI). With no JDBC URL set, the in-memory store is used
    // and this is inert. Drop it for a strictly in-memory build.
    runtimeOnly(project(":postgres"))
}

application {
    mainClass.set("dev.wiggle.server.WiggleServer")
    applicationName = "wiggle"
}
