plugins {
    application
}

// The `wiggle` command-line tool: compile a declarative workflow YAML file to a graph and register
// it with a server. A thin front-end over the client DSL (the loader drives the same builder the
// Java DSL does) plus WiggleClient -- deliberately client-only, so it stays small and carries no
// server/storage dependencies. Not published to Maven Central.
dependencies {
    implementation(project(":client"))
    implementation("org.yaml:snakeyaml:${property("snakeyamlVersion")}")
    implementation("info.picocli:picocli:${property("picocliVersion")}")

    testImplementation(project(":server"))   // an in-memory WiggleServer for the register test
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("dev.wiggle.cli.Wiggle")
    applicationName = "wiggle"
}
