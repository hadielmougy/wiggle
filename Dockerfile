# syntax=docker/dockerfile:1
#
# Wiggle standalone server image. Self-contained: the build stage compiles the Java modules
# and the ClojureScript dashboard (needs Node), then the runtime stage ships only a JRE plus
# the assembled distribution.
#
#   docker build -t hadielmougy/wiggle:2.1.4 .
#   docker run --rm -p 8080:8080 -p 8090:8090 hadielmougy/wiggle:2.1.4      # in-memory
#
# The image bundles every storage backend (PostgreSQL/H2, MySQL/MariaDB, Oracle, SQL Server,
# Cassandra); the engine is picked from the URL scheme, so pointing it at a database is just env:
#   docker run --rm -p 8080:8080 -p 8090:8090 \
#     -e WIGGLE_JDBC_URL=jdbc:postgresql://db:5432/wiggle \
#     -e WIGGLE_JDBC_USER=wiggle -e WIGGLE_JDBC_PASSWORD=wiggle \
#     -e WIGGLE_DASHBOARD_PASSWORD=change-me \
#     hadielmougy/wiggle:2.1.4

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
# -> /src/dist/build/install/wiggle/{bin,lib}

# ---- runtime stage: slim JRE + the assembled distribution ----
FROM eclipse-temurin:21-jre-jammy AS runtime

# wget for the container HEALTHCHECK below.
RUN apt-get update && apt-get install -y --no-install-recommends wget \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r wiggle && useradd -r -g wiggle -d /opt/wiggle wiggle

WORKDIR /opt/wiggle
COPY --from=build /src/dist/build/install/wiggle/ ./
USER wiggle

# gRPC control plane and the (optional) HTTP dashboard. Storage defaults to in-memory; set
# WIGGLE_JDBC_URL to run against PostgreSQL. Secure the dashboard with WIGGLE_DASHBOARD_PASSWORD
# and turn on TLS with WIGGLE_TLS_KEYSTORE (see the README).
ENV WIGGLE_PORT=8080 \
    WIGGLE_DASHBOARD_PORT=8090
EXPOSE 8080 8090

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -qO- "http://localhost:${WIGGLE_DASHBOARD_PORT}/healthz" >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/opt/wiggle/bin/wiggle"]
