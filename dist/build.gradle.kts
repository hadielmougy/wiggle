plugins {
    application
}

// The runnable, all-backends standalone server. This is the only module that depends on every
// storage module at once; bundling it into the Docker image ships every backend, and the URL
// scheme picks one at runtime. It is deliberately NOT published to Maven Central (see the root
// build's publishedModules) -- library users depend on wiggle-server plus whichever storage module
// they want. Each storage module contributes its JDBC driver transitively (runtimeOnly), so the
// assembled distribution carries them all.
dependencies {
    implementation(project(":server"))
    // The composition layer: it runs a cell (WiggleServer) OR a coordinator (CoordinatorServer), and
    // owns the one bridge that needs both -- EmbeddedCellDeployer (starts in-process cells).
    implementation(project(":coordinator:runtime"))
    implementation(project(":jdbc"))
    implementation(project(":postgres"))
    implementation(project(":mysql"))
    implementation(project(":oracle"))
    implementation(project(":sqlserver"))
    implementation(project(":cassandra"))
}

application {
    mainClass.set("dev.wiggle.dist.Main")
    applicationName = "wiggle"
}
