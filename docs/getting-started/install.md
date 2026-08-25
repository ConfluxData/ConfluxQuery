# Install ConfluxQuery

ConfluxQuery CLI and ConfluxQuery Gateway are distributed through the same
`qcli` executable.

## Release archive

Download the archive for your supported platform from the GitHub release,
verify `SHA256SUMS` and the Sigstore signature/provenance, then place `qcli` on
`PATH`. Archives also contain shell completions, `qcli.1`, the license,
deployment manifests, and core operations documentation.

```bash
sha256sum --check SHA256SUMS
cosign verify-blob \
  --bundle qcli-VERSION-PLATFORM.tar.gz.bundle \
  qcli-VERSION-PLATFORM.tar.gz
tar -xzf qcli-VERSION-PLATFORM.tar.gz
install -m 0755 qcli-VERSION-PLATFORM/qcli /usr/local/bin/qcli
qcli --version
```

Use `shasum -a 256` on macOS and `Get-FileHash` on Windows when `sha256sum`
is unavailable. Verification details and provenance expectations are in the
[release guide](../releasing.md).

## Build from source

The repository pins its Rust toolchain:

```bash
git clone https://github.com/deepakdixit/qcli.git
cd qcli
cargo build --release --locked -p qcli
./target/release/qcli --version
```

## Container

The OCI image runs as UID/GID 10001 and contains the same server-capable
binary:

```bash
docker build -t qcli:local .
docker run --rm qcli:local --version
```

Mount configuration and authentication files read-only when serving. Never
bake credentials into an image layer.

## Shell completions and manual

Release archives contain Bash, Zsh, Fish, and PowerShell completion files.
Install the file appropriate for the shell using that shell's standard
completion directory. Install `qcli.1` under a manpath such as
`/usr/local/share/man/man1/`.

## Validate the installation

```bash
qcli --help
qcli config path
qcli --config examples/milestone-2.env config check
qcli --config examples/milestone-2.env target list
```

See [supported platforms](../supported-platforms.md) before promoting a build
to production.
