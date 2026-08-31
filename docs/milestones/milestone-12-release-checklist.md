# Milestone 12 release closure checklist

Status: In progress

Purpose: close M12 with a real public, signed, cross-platform ConfluxQuery
release and verified clean-machine installation. Work through this checklist in
order. Record decisions and evidence as each item completes; do not mark M12
complete from workflow code alone.

## Decisions

| Decision | Status | Selected value |
|---|---|---|
| Branching and release model | Approved | Protected `main`, short-lived branches, signed tags publish |
| Canonical GitHub repository | Approved | `ConfluxData/ConfluxQuery` |
| Repository visibility and license | Pending | Recommended: public, Apache-2.0 |
| First release candidate | Pending | Recommended: `v0.1.0-rc.1` |
| Stable release | Pending | Recommended: `v0.1.0` |
| Documentation URL | Approved | `https://confluxdata.github.io/ConfluxQuery/` |
| Homebrew tap | Pending | Recommended: `ConfluxData/homebrew-tap` |
| crates.io required for v0.1.0 | Pending | — |
| Maven Central included in release campaign | Pending | — |
| Container registry | Pending | Recommended: `ghcr.io/confluxdata/qcli` |
| Git tag signing method | Pending | GPG or SSH signing key |

## Ordered closure flow

- [x] 1. Approve the branch, pull-request, and tag publication model.
- [ ] 2. Confirm canonical repository, package, documentation, Homebrew, Maven,
      and container coordinates.
- [ ] 3. Audit and update version, repository/homepage/documentation URLs,
      license, publisher, changelog, OCI, JDBC, and formula metadata.
- [ ] 4. Create/connect `ConfluxData/ConfluxQuery` with only a bootstrap commit
      on `main`; push the existing history to `initial-import`; merge it through
      a pull request using a merge commit; then enable squash-only merging,
      branch protection, and the required Actions checks for subsequent work.
- [ ] 5. Create protected GitHub environments for `github-release`, `homebrew`,
      and any enabled `crates-io` and `maven-central` publication.
- [ ] 6. Create `ConfluxData/homebrew-tap` and configure its narrowly scoped
      publication token and repository variable.
- [ ] 7. Run local release dry runs: Rust/JDBC/docs tests, archives, formula,
      workflow and script validation, SBOMs, reproducibility, and secret scan.
- [ ] 8. Run the complete GitHub CI workflow on `main` by manual dispatch.
- [ ] 9. Create and push the signed `v0.1.0-rc.1` tag from a verified `main`
      commit.
- [ ] 10. Verify the RC GitHub release contains five native archives, JDBC
      artifacts, OCI archive, checksums, SBOMs, Sigstore bundles, and provenance.
- [ ] 11. Verify checksum, signature, provenance, extraction, CLI, HTTP, Flight,
      ADBC, and JDBC behavior from downloaded RC artifacts.
- [ ] 12. Pass clean-machine installation on Linux x86-64/ARM64, macOS
      Intel/Apple Silicon, and Windows x86-64.
- [ ] 13. Publish and verify the Homebrew formula with
      `brew tap confluxdata/tap && brew install qcli`.
- [ ] 14. If approved, publish crates.io packages in dependency order and
      verify `cargo install qcli`.
- [ ] 15. Complete the M25 Maven Central publication alongside the campaign if
      approved, then resolve the driver from a clean Maven project.
- [ ] 16. Validate RC-to-stable configuration/state compatibility and rollback
      behavior that can be proven for the first release.
- [ ] 17. Fix RC findings, rerun all gates, create signed `v0.1.0`, and verify
      stable GitHub/Homebrew plus any enabled registry publications.
- [ ] 18. Update installation and website claims, write final M12 evidence,
      mark M12 complete, run documentation checks, and commit the closure.

## Mandatory M12 evidence

- Public release URL and immutable tag/commit.
- Artifact names and SHA-256 hashes for every supported platform.
- SBOM, Sigstore, and provenance verification output.
- Clean-machine command output for `qcli --version`, configuration validation,
  target discovery, interactive startup, and deterministic query execution.
- Working Homebrew installation on advertised platforms.
- Packaged Gateway HTTP/Flight/JDBC smoke evidence.
- RC-to-stable compatibility evidence and accepted rollback limitations.
- Exact supported platform, engine, client, and installation matrix.

## Publication rule

Merges to `main` validate and build workflow-local artifacts only. They do not
change versions or publish. A reviewed release commit on `main` becomes public
only through an explicit signed `v*` tag and protected publication environments.
