plugins {
    application
}

dependencies {
    implementation(project(":example"))
    implementation(project(":client"))
    implementation(project(":server"))
    implementation("com.anthropic:anthropic-java:2.71.0")
}

application {
    mainClass.set("com.wiggle.agents.claude.ClaudeAgentsDemo")
}

tasks.register<JavaExec>("runAgents") {
    group = "application"
    description = "Runs the multi-agent research team against Claude. Needs ANTHROPIC_API_KEY " +
            "(or an `ant auth login` profile); pass a topic with --args."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.agents.claude.ClaudeAgentsDemo")
}
