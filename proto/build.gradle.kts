// Generated protobuf messages for the control-plane wire protocol, the HTTP method table
// (`ControlPlane`), and the hand-written Json <-> protobuf Value/Struct converter that bridges to
// `core.Json`.
plugins {
    id("com.google.protobuf") version "0.9.4"
}

dependencies {
    api("com.google.protobuf:protobuf-java:${property("protobufVersion")}")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${property("protocVersion")}"
    }
}
