// Leader election for the server's clock-driven duties. It depends on nothing and persists through
// its own SPI, which the server implements over its node table.
dependencies {
    testImplementation(platform("org.junit:junit-bom:${property("junitVersion")}"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
