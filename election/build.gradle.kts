// Leader election, shared by the cell engine (:server) and the control plane (:coordinator).
//
// It depends on nothing. That is the point: :server and :coordinator must never depend on each
// other in source -- their only link is the gRPC contract -- so the election they now share has to
// live below both, with its own persistence SPI that each implements over its own tables.
dependencies {
    testImplementation(platform("org.junit:junit-bom:${property("junitVersion")}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
