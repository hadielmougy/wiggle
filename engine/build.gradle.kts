dependencies {
    // The engine's whole world: the shared model (nodes, ids, JSON, activations) and the id codec
    // it reads epochs from. No transport, no storage backend, no external dependency at all --
    // that is what makes this artifact embeddable on its own.
    api(project(":core"))
    api(project(":placement"))

    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
