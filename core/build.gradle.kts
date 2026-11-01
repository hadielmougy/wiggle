// Shared model: JSON, the compiled workflow graph, retry policy, wire records.
// It is on both the server and the client classpath, so its one dependency is Jackson's streaming
// core, which has none of its own.

dependencies {
    api("com.fasterxml.jackson.core:jackson-core:${property("jacksonVersion")}")
}
