dependencies {
    // The flow DSL (a FlowSpec is what gets observed) and, through it, the wire stubs and the
    // shared model. Nothing in :client changes for this module: it owns its own channel and RPCs.
    api(project(":client"))

    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
