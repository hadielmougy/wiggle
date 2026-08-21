plugins {
    application
}

dependencies {
    implementation(project(":client"))
    implementation(project(":server"))

    testImplementation(project(":client"))
    testImplementation(project(":server"))
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("com.h2database:h2:${property("h2Version")}")
}

application {
    // The same scenarios can be run without JUnit, or any network access at all:
    //   ./gradlew :wf-tests:run
    mainClass.set("dev.wiggle.tests.Scenarios")
}
