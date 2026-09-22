dependencies {
    api(project(":observe"))
    // The application brings its own Kafka: only the record and header types are touched here.
    compileOnly("org.apache.kafka:kafka-clients:${property("kafkaVersion")}")

    testImplementation("org.apache.kafka:kafka-clients:${property("kafkaVersion")}")
    testImplementation(project(":server"))
    testImplementation("org.junit.jupiter:junit-jupiter:${property("junitVersion")}")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
