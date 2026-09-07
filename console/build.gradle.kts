plugins {
    application
}

// The standalone ops console: a read/ops web UI that is a pure gRPC client of the control plane. It
// reuses the client SDK and the server's HTTP dashboard/auth (via the DashboardData seam), and works
// the same against a single cluster (direct) or a coordinator-sharded namespace (fan-out). It carries
// no storage drivers or coordinator code -- a thin console, not a server. Not published to Maven Central.
dependencies {
    implementation(project(":client"))
    implementation(project(":server"))

    testImplementation(project(":coordinator"))   // in-process coordinator for the coordinator-mode test
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.wiggle.console.ConsoleMain")
    applicationName = "wiggle-console"
}
