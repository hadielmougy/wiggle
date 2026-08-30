plugins {
    application
}

dependencies {
    implementation(project(":client"))
    implementation(project(":server"))

    testImplementation(project(":client"))
    testImplementation(project(":server"))
    // The coordinator control plane (moved out of :server): its runtime + SPI (spi comes transitively).
    testImplementation(project(":coordinator:runtime"))
    // The JDBC-backed store lives in its own module now; the JDBC/migration tests need it
    // (and the postgres module brings the H2 driver transitively at runtime). The mysql/oracle
    // modules supply their dialects + providers for the dialect and opt-in integration tests.
    testImplementation(project(":jdbc"))
    testImplementation(project(":postgres"))
    testImplementation(project(":mysql"))
    testImplementation(project(":oracle"))
    testImplementation(project(":sqlserver"))
    testImplementation(project(":cassandra"))
    // The dist module supplies the explicit WiggleStorageFactory used to run a WiggleServer against
    // a real database in tests (the same one the standalone image uses).
    testImplementation(project(":dist"))
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    // The same scenarios can be run without JUnit, or any network access at all:
    //   ./gradlew :wf-tests:run
    mainClass.set("dev.wiggle.tests.Scenarios")
}
