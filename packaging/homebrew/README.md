# Homebrew distribution

Wiggle's server is distributed through a **Homebrew tap** (your own repo), not
homebrew-core — a brand-new project doesn't meet homebrew-core's notability bar.

Users install with:

```sh
brew install hadielmougy/wiggle/wiggle
```

## Cutting a brew release

1. **Build the distribution tarball** (bundles launcher scripts + all jars):
   ```sh
   ./gradlew :server:distTar
   # -> server/build/distributions/wiggle-<version>.tar
   ```

2. **Attach it to the matching GitHub release.** Create/upload against the version tag:
   ```sh
   gh release create v1.0.0 server/build/distributions/wiggle-1.0.0.tar \
       --title "v1.0.0" --notes "First release"
   # or, if the release already exists:
   gh release upload v1.0.0 server/build/distributions/wiggle-1.0.0.tar
   ```

3. **Get the checksum of the uploaded asset** (must match the exact file you attached —
   rebuilding can change the tar):
   ```sh
   shasum -a 256 server/build/distributions/wiggle-1.0.0.tar
   ```

4. **Publish the formula to the tap.** Create a repo named `homebrew-wiggle`
   (the `homebrew-` prefix is required), then commit `wiggle.rb` from this directory
   into its `Formula/` folder, updating `url` and `sha256` for the new version:
   ```sh
   git clone https://github.com/hadielmougy/homebrew-wiggle
   mkdir -p homebrew-wiggle/Formula
   cp packaging/homebrew/wiggle.rb homebrew-wiggle/Formula/wiggle.rb
   # edit url + sha256 if the version changed, then:
   cd homebrew-wiggle && git add Formula/wiggle.rb && git commit -m "wiggle 1.0.0" && git push
   ```

5. **Verify locally** before announcing:
   ```sh
   brew install --build-from-source hadielmougy/wiggle/wiggle
   brew test wiggle
   brew audit --strict --online wiggle
   ```

## Later: homebrew-core

Once the project is notable (stars, a track record, stable releases), submit the same
formula as a PR to https://github.com/Homebrew/homebrew-core so users can
`brew install wiggle` without the tap.
