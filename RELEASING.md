# Releasing to Maven Central

Wiggle publishes four modules to the [Central Portal](https://central.sonatype.com)
under the group `io.github.hadielmougy`:

| Module   | Artifact          |
|----------|-------------------|
| `core`   | `wiggle-core`     |
| `proto`  | `wiggle-proto`    |
| `client` | `wiggle-client`   |
| `server` | `wiggle-server`   |

`example` and `tests` are not published.

Publishing is wired up with the [Vanniktech Maven Publish plugin](https://vanniktech.github.io/gradle-maven-publish-plugin/),
which builds sources + javadoc jars, signs every artifact, and uploads a deployment
bundle to the Central Portal.

> **A release is permanent.** Once a version is published to Central it can never be
> deleted or overwritten. Bump the version for every release.

## One-time setup

1. **Central Portal account + namespace.** Sign in at https://central.sonatype.com and
   register the `io.github.hadielmougy` namespace (verified by creating the GitHub repo
   it tells you to, or a TXT record). Then **Account → Generate User Token** to get a
   token username/password pair.

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
6. Tag the release: `git tag vx.y.z && git push --tags`.

Artifacts appear on Central within ~15–30 minutes and sync to search indexes over the
following hours.

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
