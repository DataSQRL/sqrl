class Sqrl < Formula
  desc "A SQRL compiler"
  homepage "https://datasqrl.com"
  url "https://datasqrl-public.s3.amazonaws.com/sqrl-cli.jar"
  version "1"
  sha256 "93e03e83999c50bf6fa283d64c49057e2b5b780927fe7174cd5edc2f2ba5e3ec"
  license ""

  bottle :unneeded

  depends_on "openjdk"

  def install
    libexec.install "sqrl-cli.jar"
    (bin/"sqrl").write <<~EOS
      #!/bin/bash
      exec "#{Formula["openjdk"].opt_bin}/java" -jar "#{libexec}/sqrl-cli.jar" "$@"
    EOS
    (bin/"sqrl").chmod 0755
  end

  test do
    system "#{bin}/sqrl", "--version"
  end
end
