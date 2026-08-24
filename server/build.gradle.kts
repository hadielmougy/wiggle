dependencies {
    api(project(":core"))
    api(project(":proto"))
    // No storage dependency: the server core is storage-agnostic and builds its store from an
    // injected StorageFactory. The runnable, all-backends server lives in the :dist module.
}

// Compiles the ClojureScript dashboard SPA (dashboard-ui/) into src/main/resources/dashboard/js
// so the bundle ships inside the server jar. Best-effort: if Node is not installed the task is
// skipped and the server falls back to its built-in minimal HTML page.
val dashboardUi = file("${rootDir}/dashboard-ui")
val dashboardBundle = file("src/main/resources/dashboard/js/app.js")

val buildDashboard = tasks.register<Exec>("buildDashboard") {
    group = "build"
    description = "Compiles the ClojureScript dashboard into the server resources."
    workingDir = dashboardUi
    inputs.dir(dashboardUi.resolve("src"))
    inputs.file(dashboardUi.resolve("shadow-cljs.edn"))
    inputs.file(dashboardUi.resolve("package.json"))
    outputs.file(dashboardBundle)

    // Skip cleanly when the toolchain is unavailable, or when explicitly disabled with
    // -PskipDashboard, so a pure-JVM build never requires Node.
    val npx = listOf("/opt/homebrew/bin/npx", "/usr/local/bin/npx", "npx").firstOrNull { path ->
        path == "npx" || file(path).exists()
    }
    val enabled = project.findProperty("skipDashboard") == null && npx != null
    onlyIf { enabled }
    if (enabled) {
        commandLine("sh", "-c",
            "if [ ! -d node_modules ]; then npm install; fi && ${npx} shadow-cljs release app")
    }
}

// Everything that reads main resources (processResources, jar, sourcesJar) must run after the
// bundle is generated, so declare the dependency broadly.
tasks.named("processResources") { dependsOn(buildDashboard) }
tasks.withType<Jar>().configureEach { dependsOn(buildDashboard) }
