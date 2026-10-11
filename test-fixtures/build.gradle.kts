plugins {
    `java-library`
}

// Helpers shared by the test suites of several modules (:tests, :console). No dependencies, so any
// module can take it as a testImplementation without pulling the server or client in. Not published.
dependencies {
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
