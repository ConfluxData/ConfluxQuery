# Release and Branching Guide

## Branch model

qcli uses short-lived branches and a protected `main` branch:

```text
feature/*, fix/*, docs/*
          |
          v
      pull request -- required CI --> main
                                       |
                                       v
                              explicit signed v* tag
                                       |
                                       v
                                  publication
```

Pull requests and merges to `main` never change the version and never publish.
They run validation and produce only workflow-local build artifacts.

Recommended branch protection for `main`:

- require pull requests and one approval;
- require the CI `quality`, all `test` matrix jobs, `msrv`, and `security`;
- require the branch to be current before merge;
- disallow force pushes and deletion;
- restrict direct pushes.

## CI

`.github/workflows/ci.yml` runs for pull requests, pushes to `main`, and manual
dispatches. It verifies:

- formatting, Clippy with warnings denied, and rustdoc warnings;
- the complete workspace test suite on Linux, macOS ARM64, and Windows;
- release-mode binary build and installed-binary smoke commands;
- the declared Rust 1.86 minimum version;
- RustSec dependency advisories.

Live engine conformance remains a separately approved pre-release operation
because it requires Trino, Databricks, and Snowflake credentials.

## Version and tag policy

The workspace version in `Cargo.toml` is authoritative. A release PR updates the
version, lockfile, changelog, and compatibility documentation. After merge,
create a signed tag:

```bash
git tag -s v0.1.0
git push origin v0.1.0
```

Supported tags:

```text
v0.1.0-rc.1  GitHub prerelease only
v0.1.0       Stable GitHub release and guarded package publication
```

The release workflow rejects a tag whose base version differs from
`Cargo.toml`.

## GitHub release pipeline

`.github/workflows/release.yml`:

1. repeats the full formatting, lint, and test gate;
2. builds Linux x86-64/ARM64, macOS x86-64/ARM64, and Windows x86-64;
3. packages the binary, license, changelog, deployment assets, operational
   guides, man page, and shell completions;
4. creates SHA-256 checksums and an SPDX JSON SBOM;
5. signs release files with keyless Sigstore;
6. creates GitHub artifact provenance attestations;
7. builds and smoke-tests a non-root Linux AMD64 OCI server archive;
8. runs packaged HTTP, ADBC, JDBC, HA, and bounded-load profiles;
9. publishes a GitHub prerelease or stable release.

The `github-release` environment should require approval for initial releases.

## crates.io

Stable releases dispatch `.github/workflows/publish-packages.yml`. The
`crates-io` job runs only when this repository variable is set:

```text
ENABLE_CRATES_IO_PUBLISH=true
```

Configure:

- a protected `crates-io` GitHub environment;
- `CARGO_REGISTRY_TOKEN` in that environment, unless migrated to crates.io
  trusted publishing;
- ownership of every published `qcli-*` crate name.

The script publishes internal crates in dependency order, followed by `qcli`.
Versioned path dependencies allow local workspace builds and registry
publication from the same manifests.

Before enabling publication, run:

```bash
cargo publish --dry-run -p qcli-auth
# repeat in scripts/publish-crates.sh order after preceding crates exist
```

For the first release, Cargo cannot dry-run the final `qcli` package before its
versioned internal dependencies exist in the crates.io index. The guarded script
therefore publishes in dependency order and retries while the registry index
propagates. Subsequent releases can dry-run every crate before publication.

## Homebrew

Configure:

```text
Repository variable: HOMEBREW_TAP=OWNER/homebrew-tap
Environment secret:  HOMEBREW_TAP_TOKEN=<fine-grained token>
```

The token needs contents write permission only for the tap repository. The
guarded job downloads release checksums, generates `Formula/qcli.rb`, commits
it to the tap, and installs precompiled GitHub Release archives.

Users then run:

```bash
brew tap OWNER/tap
brew install qcli
```

## Required GitHub setup

This local checkout currently has no Git remote. After creating the repository:

1. add and push the GitHub remote;
2. configure branch protection and the three approval environments:
   `github-release`, `crates-io`, and `homebrew`;
3. configure the repository variables and secrets described above;
4. confirm all desired crates.io names and tap ownership;
5. run `workflow_dispatch` CI;
6. create `v0.1.0-rc.1` before the first stable tag.

No workflow publishes on an ordinary merge to `main`.
