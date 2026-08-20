class Wiggle < Formula
  desc "Durable workflow engine server (control plane)"
  homepage "https://github.com/hadielmougy/wiggle"
  url "https://github.com/hadielmougy/wiggle/releases/download/v1.0.0/wiggle-1.0.0.tar"
  sha256 "749cc78f492fa378652435a33b6c7e550759cdc06197dd61f344ceb633eb2436"
  license "Apache-2.0"

  depends_on "openjdk@21"

  def install
    # The application-plugin distribution: bin/ launcher scripts + lib/ jars.
    libexec.install Dir["*"]
    # Wrap the launcher so it always runs against the Java this formula depends on.
    (bin/"wiggle").write_env_script libexec/"bin/wiggle",
      JAVA_HOME: Formula["openjdk@21"].opt_prefix
  end

  test do
    # The server takes no CLI flags: it reads env and runs until killed. Start it on an
    # ephemeral port (WIGGLE_PORT=0), confirm it announces itself, then shut it down.
    log = testpath/"wiggle.log"
    pid = spawn({ "WIGGLE_PORT" => "0" }, "#{bin}/wiggle", out: log.to_s, err: log.to_s)
    begin
      sleep 8
      assert_match "Wiggle server", log.read
    ensure
      Process.kill("TERM", pid)
      Process.wait(pid)
    end
  end
end
