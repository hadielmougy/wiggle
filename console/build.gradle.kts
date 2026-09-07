plugins {
    application
}

// The standalone ops console: a read/ops web UI (embedded Tomcat + servlets) that is a pure gRPC client
// of the control plane. It owns the dashboard SPA + JSON API and works the same against a single cluster
// (direct) or a coordinator-sharded namespace (fan-out). No storage drivers, no coordinator, no engine
// -- a thin console. Not published to Maven Central.
dependencies {
    implementation(project(":client"))
    implementation("org.apache.tomcat.embed:tomcat-embed-core:${property("tomcatVersion")}")

    testImplementation(project(":server"))        // an embedded WiggleServer for direct-mode tests
    testImplementation(project(":coordinator"))   // an in-process coordinator for coordinator-mode tests
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.wiggle.console.ConsoleMain")
    applicationName = "wiggle-console"
}

// Compiles the ClojureScript dashboard SPA (dashboard-ui/) into src/main/resources/dashboard/js so the
// bundle ships inside the console jar. Best-effort: if Node is not installed the task is skipped (and the
// committed bundle is used as-is), so a pure-JVM build never requires Node.
val dashboardUi = file("${rootDir}/dashboard-ui")
val dashboardBundle = file("src/main/resources/dashboard/js/app.js")

val buildDashboard = tasks.register<Exec>("buildDashboard") {
    group = "build"
    description = "Compiles the ClojureScript dashboard into the console resources."
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
