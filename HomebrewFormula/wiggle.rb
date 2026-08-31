# typed: strict
# frozen_string_literal: true
class Wiggle < Formula
  desc "CLI to author and register Wiggle workflow definitions from YAML"
  homepage "https://github.com/hadielmougy/wiggle"
  url "https://github.com/hadielmougy/wiggle/releases/download/v2.1.6/wiggle-2.1.6.tar"
  sha256 "64db78c24ffafbfe3a67e9c77acb909384b6bd3790ba41f6b0f0d084d670c54e"
  license "Apache-2.0"

  depends_on "openjdk@21"

  def install
    libexec.install Dir["*"]
    (bin/"wiggle").write_env_script libexec/"bin/wiggle",
                                    JAVA_HOME: formula_opt_prefix("openjdk@21")
  end

  test do
    assert_match "wiggle", shell_output("#{bin}/wiggle --version")
  end
end
