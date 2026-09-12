dependencies {
    api(project(":core"))
    api(project(":proto"))

    // Client-side fault tolerance: retry transient (UNAVAILABLE) RPCs so client/worker calls ride out
    // a server rescheduling / active-passive failover instead of failing the operation.
    implementation("io.github.hadielmougy:shield:${property("shieldVersion")}")

    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
