# syntax=docker/dockerfile:1
#
# Wiggle standalone server image. Self-contained: the build stage compiles the Java modules
# and the ClojureScript dashboard (needs Node), then the runtime stage ships only a JRE plus
# the assembled distribution.
#
#   docker build -t hadielmougy/wiggle:0.0.1 .
#   docker run --rm -p 8080:8080 -p 8090:8090 hadielmougy/wiggle:0.0.1      # in-memory
#
# The image bundles every storage backend (PostgreSQL/H2, MySQL/MariaDB, Oracle, SQL Server,
# Cassandra); the engine is picked from the URL scheme, so pointing it at a database is just env:
#   docker run --rm -p 8080:8080 -p 8090:8090 \
#     -e WIGGLE_JDBC_URL=jdbc:postgresql://db:5432/wiggle \
#     -e WIGGLE_JDBC_USER=wiggle -e WIGGLE_JDBC_PASSWORD=wiggle \
#     -e WIGGLE_DASHBOARD_PASSWORD=change-me \
#     hadielmougy/wiggle:0.0.1

# ---- build stage: JDK 21 + Node (for the shadow-cljs dashboard bundle) ----
FROM eclipse-temurin:21-jdk-jammy AS build

# Node 20 for the ClojureScript dashboard (shadow-cljs). Without it the build still succeeds
# but the dashboard would be absent; the image is meant to ship the full UI, so install it.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
 && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Warm the npm cache with just the dashboard's descriptors first, for better layer caching.
COPY dashboard-ui/package.json dashboard-ui/package-lock.json ./dashboard-ui/
RUN cd dashboard-ui && (npm ci || npm install)

# Then the rest of the sources and the full build (compiles Java + dashboard, assembles the dist).
# The :dist module bundles every storage backend; the engine is picked from the URL at runtime.
COPY . .
RUN chmod +x gradlew \
 && ./gradlew --no-daemon --console=plain :dist:installDist
# -> /src/dist/build/install/wiggle-server/{bin,lib}

# ---- runtime stage: distroless (glibc, no shell, non-root) ----
# gcr.io/distroless/java21-debian12 ships only a JRE on a minimal glibc base — no shell, no package
# manager, a tiny CVE surface — and the :nonroot tag runs as an unprivileged NUMERIC uid (65532), so
# the image satisfies a `runAsNonRoot: true` admission policy with no extra config. glibc (not musl)
# keeps the coordinator's RocksDB native lib working. There is no shell, so we launch the JVM
# directly (not the generated bin/wiggle script) and copy only lib/; health is a Kubernetes httpGet
# probe on /healthz (see deploy/helm), not a shell HEALTHCHECK.
FROM gcr.io/distroless/java21-debian12:nonroot AS runtime

WORKDIR /opt/wiggle
COPY --from=build /src/dist/build/install/wiggle-server/lib ./lib

# gRPC control plane, and the port that serves the always-open /healthz probe. Storage defaults to
# in-memory; set WIGGLE_JDBC_URL to run against PostgreSQL. Secure the console with
# WIGGLE_DASHBOARD_PASSWORD and turn on TLS with WIGGLE_TLS_KEYSTORE (see the README).
ENV WIGGLE_PORT=8080 \
    WIGGLE_DASHBOARD_PORT=8090
EXPOSE 8080 8090

USER 65532:65532
# MaxRAMPercentage lets the JVM size its heap from the container memory limit (cgroup-aware).
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-cp", "/opt/wiggle/lib/*", "com.wiggle.dist.Main"]
