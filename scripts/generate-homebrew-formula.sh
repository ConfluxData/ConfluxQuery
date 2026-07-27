#!/usr/bin/env bash
set -euo pipefail

tag="${1:?tag is required}"
repository="${2:?owner/repository is required}"
version="${tag#v}"
checksums="$(gh release download "$tag" --repo "$repository" --pattern SHA256SUMS --output -)"

checksum() {
  local target="$1"
  awk -v target="$target" '$2 ~ target { print $1 }' <<<"$checksums"
}

linux_x64="$(checksum 'x86_64-unknown-linux-gnu.tar.gz$')"
linux_arm64="$(checksum 'aarch64-unknown-linux-gnu.tar.gz$')"
macos_x64="$(checksum 'x86_64-apple-darwin.tar.gz$')"
macos_arm64="$(checksum 'aarch64-apple-darwin.tar.gz$')"

for value in "$linux_x64" "$linux_arm64" "$macos_x64" "$macos_arm64"; do
  [[ -n "$value" ]] || { echo "missing release checksum" >&2; exit 1; }
done

cat <<RUBY
class Qcli < Formula
  desc "One query shell for cloud data platforms"
  homepage "https://github.com/${repository}"
  version "${version}"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/${repository}/releases/download/${tag}/qcli-${version}-aarch64-apple-darwin.tar.gz"
      sha256 "${macos_arm64}"
    else
      url "https://github.com/${repository}/releases/download/${tag}/qcli-${version}-x86_64-apple-darwin.tar.gz"
      sha256 "${macos_x64}"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/${repository}/releases/download/${tag}/qcli-${version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "${linux_arm64}"
    else
      url "https://github.com/${repository}/releases/download/${tag}/qcli-${version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "${linux_x64}"
    end
  end

  def install
    bin.install "qcli"
    man1.install "qcli.1"
    bash_completion.install "completions/qcli.bash" => "qcli"
    zsh_completion.install "completions/_qcli"
    fish_completion.install "completions/qcli.fish"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/qcli --version")
  end
end
RUBY
