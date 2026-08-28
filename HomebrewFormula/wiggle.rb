# Homebrew formula for the `wiggle` CLI (author + register Wiggle workflow definitions).
#
# The CLI is a JVM application, so this depends on a JDK and wraps the launcher with JAVA_HOME set.
# Per release, update `url`'s version and paste the .tar SHA-256 printed by scripts/cli-release.sh.
#
# Tap it straight from this repo:
#   brew tap hadielmougy/wiggle https://github.com/hadielmougy/wiggle
#   brew install hadielmougy/wiggle/wiggle
# (or copy this file into a dedicated `homebrew-tap` repo for `brew install hadielmougy/tap/wiggle`.)
class Wiggle < Formula
  desc "CLI to author and register Wiggle workflow definitions from YAML"
  homepage "https://github.com/hadielmougy/wiggle"
  url "https://github.com/hadielmougy/wiggle/releases/download/v2.1.5/wiggle-2.1.5.tar"
  sha256 "REPLACE_WITH_TAR_SHA256"  # from scripts/cli-release.sh
  license "Apache-2.0"

  depends_on "openjdk@21"

  def install
    # The archive extracts to bin/ + lib/; keep them together under libexec and wrap the launcher
    # so it always runs against the formula's JDK regardless of the user's JAVA_HOME.
    libexec.install Dir["*"]
    (bin/"wiggle").write_env_script libexec/"bin/wiggle",
      JAVA_HOME: Formula["openjdk@21"].opt_prefix
  end

  test do
    assert_match "wiggle", shell_output("#{bin}/wiggle --version")
  end
end
