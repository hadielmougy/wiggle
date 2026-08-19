// Generated gRPC/protobuf stubs for the control-plane wire protocol, plus the
// hand-written Json <-> protobuf Value/Struct converter that bridges to `core.Json`.
plugins {
    id("com.google.protobuf") version "0.9.4"
}

dependencies {
    api("com.google.protobuf:protobuf-java:${property("protobufVersion")}")
    api("io.grpc:grpc-stub:${property("grpcVersion")}")
    api("io.grpc:grpc-protobuf:${property("grpcVersion")}")
    api("io.grpc:grpc-netty-shaded:${property("grpcVersion")}")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${property("protocVersion")}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${property("grpcVersion")}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}
