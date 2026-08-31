# Milestone 1 Notes: Configuration and Target Discovery

Status: Complete

Completed: 2026-07-21

## Demonstrated outcome

qcli is now a working Rust executable that discovers, validates, resolves, and safely displays targets from the sectioned qcli configuration format.

The milestone demo uses `examples/milestone-1.env`:

```text
$ qcli --config /tmp/qcli-milestone-1.env config check
Configuration is valid: 3 target(s)

$ qcli --config /tmp/qcli-milestone-1.env target list
databricks-dev           databricks
snowflake-prod           snowflake
trino-dev                trino

$ qcli --config /tmp/qcli-milestone-1.env target show trino-dev
target = trino-dev
engine = trino
catalog = hive
decimal_places = 10
output_format = table
schema = analytics
string_truncate = 80
timing = true
token = <redacted>
url = <redacted>
user = analyst
```

This demonstrates that target sections are discovered without `QCLI_TARGETS`, target properties override `[default]`, inherited properties remain available, and secrets are not printed.

## Delivered

- Cargo workspace with separate `qcli-config` library and `qcli` executable crates.
- Pinned toolchain policy and workspace lint policy.
- Custom sectioned `.env` parser.
- `[default]` inheritance and target-section discovery.
- Trino, Databricks SQL, Snowflake, and internal demo engine validation.
- Quoted values and comments outside quoted text.
- `${ENVIRONMENT_VARIABLE}` substitution without recursive or command expansion.
- Typed validation for booleans, non-negative integers, and durations.
- Unknown-property rejection with close-name suggestions.
- Duplicate section and property rejection.
- Source path and line numbers in parsing and validation errors.
- Unix credential-file permission validation with corrective guidance.
- Redacted secret value type with safe debug formatting.
- Redaction of passwords, tokens, private keys, secrets, and connection URLs.
- `qcli config path`.
- `qcli config check`.
- Redacted `qcli config show`.
- `qcli target list`.
- Redacted, resolved `qcli target show TARGET`.
- Configuration error exit code `3`.
- Example configuration and root README demo instructions.

## Automated evidence

Validation completed with the installed Rust 1.96.1 toolchain:

```text
cargo test --workspace
```

Result: eight tests passed—six configuration unit tests and two executable integration tests.

```text
cargo clippy --workspace --all-targets -- -D warnings
```

Result: passed with no warnings.

```text
cargo fmt --all -- --check
```

Result: passed.

Test coverage includes:

- Target discovery and inheritance.
- Target-specific override resolution.
- Type validation and source locations.
- Unknown-property suggestions.
- Quoted comment characters.
- Secret debug/display redaction.
- Broad Unix permission rejection.
- CLI validation, listing, resolved display, environment substitution, URL redaction, and exit codes.

## Architecture decisions exercised

- Configuration behavior lives in a reusable library rather than the binary.
- The CLI only handles process arguments and presentation.
- Engine property validation is selected during target resolution.
- Resolved target properties are deterministic through ordered maps.
- Secret values carry redaction metadata instead of relying on output-time name matching alone.
- No database driver or frontend framework dependency is introduced in M1.

## Known limitations

- CLI argument parsing is intentionally minimal and will be revisited as the command surface expands.
- Windows ACL validation is not implemented yet; Unix mode validation is active.
- Escape-sequence interpretation inside quoted configuration values is not implemented.
- Environment substitution supports `${NAME}` only; default-value expressions are intentionally unsupported.
- All connection URLs are currently fully redacted instead of showing a sanitized host-only form.
- Property schemas cover documented initial properties but will evolve through adapter feasibility work.
- Configuration mutation/edit commands are not provided; qcli reads but does not rewrite credential files.
- No engine connection or query execution is part of this milestone.

## Prerequisites established for Milestone 2

- A resolved target model is available to session creation.
- Secret-bearing values remain protected at the configuration boundary.
- The internal `demo` engine identifier is reserved for deterministic end-to-end testing.
- Display properties such as `decimal_places` and `string_truncate` are resolved for use by the future result renderer.

Milestone 2 can now introduce the frontend-neutral session manager, immutable query snapshots, query lifecycle, demo adapter, and common result stream.
