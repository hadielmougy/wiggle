plugins {
    application
}

dependencies {
    implementation(project(":client"))
    implementation(project(":server"))

    testImplementation(project(":client"))
    testImplementation(project(":server"))
    // The JDBC-backed store lives in its own module now; the JDBC/migration tests need it
    // (and it brings the H2 driver transitively at runtime).
    testImplementation(project(":postgres"))
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    // The same scenarios can be run without JUnit, or any network access at all:
    //   ./gradlew :wf-tests:run
    mainClass.set("dev.wiggle.tests.Scenarios")
}
