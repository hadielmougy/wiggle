plugins {
    application
}

dependencies {
    implementation(project(":client"))
    implementation(project(":server"))

    testImplementation(project(":client"))
    testImplementation(project(":server"))
    // The coordinator control plane (moved out of :server): its runtime + SPI (spi comes transitively).
    testImplementation(project(":coordinator"))
    // The JDBC-backed store lives in its own module now; the JDBC/migration tests need it, and
    // :postgres supplies both dialects -- PostgreSQL, and H2 for the runs with nothing installed.
    testImplementation(project(":jdbc"))
    testImplementation(project(":postgres"))
    // The dist module supplies the explicit WiggleStorageFactory used to run a WiggleServer against
    // a real database in tests (the same one the standalone image uses).
    testImplementation(project(":dist"))
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    // The same scenarios can be run without JUnit, or any network access at all:
    //   ./gradlew :wf-tests:run
    mainClass.set("com.wiggle.tests.Scenarios")
}
