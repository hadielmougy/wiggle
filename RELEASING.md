# Releasing to Maven Central

Wiggle publishes its library modules to the [Central Portal](https://central.sonatype.com)
under the group `sh.wiggle`:

| Module       | Artifact             | Notes                                             |
|--------------|----------------------|---------------------------------------------------|
| `core`       | `wiggle-core`        |                                                   |
| `proto`      | `wiggle-proto`       |                                                   |
| `client`     | `wiggle-client`      | the usual dependency: DSL + worker + client       |
| `server`     | `wiggle-server`      |                                                   |
| `jdbc`       | `wiggle-jdbc`        |                                                   |
| `postgres`   | `wiggle-postgres`    |                                                   |
| `mysql`      | `wiggle-mysql`       |                                                   |
| `oracle`     | `wiggle-oracle`      |                                                   |
| `sqlserver`  | `wiggle-sqlserver`   |                                                   |
| `bom`        | `wiggle-bom`         | version-alignment platform (`java-platform`)      |
| `client-all` | `wiggle-client-all`  | shaded client — gRPC/protobuf/Guava relocated     |

`example`, `tests`, `dist`, `cli`, `console`, and `coordinator` are not published.

Publishing is wired up with the [Vanniktech Maven Publish plugin](https://vanniktech.github.io/gradle-maven-publish-plugin/),
which builds sources + javadoc jars, signs every artifact, and uploads a deployment
bundle to the Central Portal.

> **A release is permanent.** Once a version is published to Central it can never be
> deleted or overwritten. Bump the version for every release.

## One-time setup

1. **Central Portal account + namespace.** Sign in at https://central.sonatype.com and
   register the `sh.wiggle` namespace, verified with a DNS **TXT record on `wiggle.sh`**
   (Central shows the exact token to add). A domain-verified namespace reads as a real
   project rather than a personal `io.github.<user>` repo — which matters for the
   enterprise Artifactory allowlists that gate adoption. Then **Account → Generate User
   Token** to get a token username/password pair.

2. **GPG signing key.**
   ```sh
   gpg --gen-key                                   # if you don't have one
   gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>   # publish the public key
   gpg --armor --export-secret-keys <KEY_ID>       # copy this armored block
   ```

3. **Credentials** — put these in `~/.gradle/gradle.properties` (never in the repo):
   ```properties
   mavenCentralUsername=<token-username>
   mavenCentralPassword=<token-password>
   signingInMemoryKey=<armored-secret-key>
   signingInMemoryKeyPassword=<key-passphrase>
   ```
   Or supply them as `ORG_GRADLE_PROJECT_*` environment variables in CI.

## Cutting a release

1. Set the version in the root `build.gradle.kts` (`version = "x.y.z"`), no `-SNAPSHOT`.
2. Verify the build is green:
   ```sh
   ./gradlew clean build
   ```
3. Dry-run the artifacts locally (builds, signs, and stages to a local repo — no upload):
   ```sh
   ./gradlew publishToMavenLocal
   ```
4. Upload the deployment to the Central Portal:
   ```sh
   ./gradlew publishToMavenCentral
   ```
5. Go to https://central.sonatype.com → **Deployments**, review the validated bundle,
   and click **Publish**. (Set `automaticRelease = true` in the root build if you'd
   rather skip this manual click once you trust the pipeline.)
6. Tag the release: `git tag vx.y.z && git push --tags`. **Pushing the tag also fires the
   [`Release` workflow](.github/workflows/release.yml)** (see below), which creates the GitHub
   Release for `vx.y.z` and attaches the runnable server distribution.

Artifacts appear on Central within ~15–30 minutes and sync to search indexes over the
following hours.

## Server distribution (runnable tarball, automated)

The runnable, all-backends server ships as a self-contained archive on the **GitHub Release** — no
Maven, no container registry, just a JRE 21 on the host. This is the lowest-friction path for
airgapped / locked-down environments (one HTTPS download, verify, extract, run).

`.github/workflows/release.yml` does this automatically on a `v*` tag push: it builds
`wiggle-server-<version>.{tar,zip}` (`bin/wiggle` launcher + `lib/`), writes `SHA-256SUMS`,
signs the checksums **keyless** (Sigstore/cosign — no stored key; uses the workflow's OIDC token),
and creates the Release with all of it attached. It first checks whether the Release already exists
and uploads with `--clobber` if so, so it composes with the CLI step below (which attaches to the
same `vx.y.z` Release). The tarball's version comes from the Gradle project version; the workflow
fails fast if the tag doesn't match. Run it by hand from the Actions tab (`workflow_dispatch`) to
build + download the archives as a run artifact without cutting a Release.

A downloader verifies the release like this:

```sh
sha256sum -c SHA-256SUMS                                   # archives match the manifest
cosign verify-blob --certificate SHA-256SUMS.pem \
  --signature SHA-256SUMS.sig \
  --certificate-identity-regexp '^https://github.com/hadielmougy/wiggle' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  SHA-256SUMS                                              # …and the manifest is authentic
```

## Container image (multi-arch, signed, automated)

The same [`Release` workflow](.github/workflows/release.yml) also publishes the container image on a
`v*` tag push — no stored secret required (GHCR uses the run's `GITHUB_TOKEN`, signing uses its OIDC
identity):

- **Multi-arch** `linux/amd64` + `linux/arm64`. The Dockerfile's build stage is pinned to the
  builder arch (`--platform=$BUILDPLATFORM`), and the JARs are portable, so the Java/dashboard
  compile runs **once** natively and only the per-arch runtime layer is rebuilt — no emulated
  recompilation.
- Pushed to **`ghcr.io/<owner>/wiggle`**, tagged `X.Y.Z` and `latest`, with an **SBOM** and **SLSA
  provenance** attestation.
- **Signed keyless** with cosign (Sigstore) — the image index digest.

First release only: make the GHCR package public (Packages → wiggle → Package settings → change
visibility) if you want anonymous pulls; otherwise consumers configure an image pull secret.
To publish to Docker Hub as well, add a second `docker/login-action` + registry to the `image` job
(needs `DOCKERHUB_USERNAME`/`DOCKERHUB_TOKEN` secrets).

Verify a pulled image:

```sh
cosign verify ghcr.io/hadielmougy/wiggle:0.0.1 \
  --certificate-identity-regexp '^https://github.com/hadielmougy/wiggle' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Releasing the `wiggle` CLI

The CLI (the coordinator namespace/epoch tool — see the README's "Command-line tool" section) ships
as a self-contained archive attached to the GitHub Release (not to Maven Central). It's a JVM app, so
users need Java 21 on their machine.

1. After tagging `vx.y.z` and creating the GitHub Release, build the archives and print their
   checksums:
   ```sh
   scripts/cli-release.sh                 # -> cli/build/distributions/wiggle-x.y.z.{zip,tar} + SHA-256
   UPLOAD=true scripts/cli-release.sh     # also attaches them to release vx.y.z (needs the gh CLI)
   ```
2. Update the Homebrew formula in [`HomebrewFormula/wiggle.rb`](HomebrewFormula/wiggle.rb): bump the
   `url` version and paste the **`.tar` SHA-256** printed above. Commit it (and mirror it into the
   `homebrew-tap` repo if you keep one).

Users then install per the "Command-line tool (`wiggle`)" section of the
[README](README.md#command-line-tool-wiggle).
