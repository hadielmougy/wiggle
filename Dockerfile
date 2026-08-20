# Server image for the kind cluster. Built from the application-plugin distribution,
# so run `./gradlew :server:installDist` first (scripts/kind-up.sh does this for you).
FROM eclipse-temurin:21-jre

WORKDIR /opt/wiggle
COPY server/build/install/wiggle/ ./

EXPOSE 8080
ENTRYPOINT ["/opt/wiggle/bin/wiggle"]
