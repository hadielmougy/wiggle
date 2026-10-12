plugins {
    application
}

dependencies {
    implementation(project(":example"))
    implementation(project(":client"))
    implementation(project(":server"))
    implementation("com.openai:openai-java:4.80.0")
}

application {
    mainClass.set("com.wiggle.agents.openai.OpenAiAgentsDemo")
}

tasks.register<JavaExec>("runAgents") {
    group = "application"
    description = "Runs the multi-agent research team against OpenAI. Needs OPENAI_API_KEY; " +
            "pass a topic with --args."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.wiggle.agents.openai.OpenAiAgentsDemo")
}
