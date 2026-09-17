// The placement rules: where an instance lives, and which cell may mint ids for it.
//
// Pure -- no I/O, no gRPC, no storage. That is the point: these rules were spread across the
// server (minting), the coordinator (the ring) and the client (resolution), which is how two of
// them came to disagree. Here they can be stated once and attacked with generated input.
//
// It depends on :core for the id format and on nothing else, so :server and :coordinator can both
// use it without depending on each other.
dependencies {
    api(project(":core"))

    testImplementation(platform("org.junit:junit-bom:${property("junitVersion")}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
