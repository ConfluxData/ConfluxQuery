# Milestone 3 Notes: Batch Output and Automation

Status: Complete

Completed: 2026-07-21

## Demonstrated outcome

qcli can now act as both a human-readable query command and a composable batch tool:

```text
$ qcli --config examples/milestone-2.env \
    --target demo --command "select * from sample" --format csv
id,name,amount
1,alpha,123.456789
2,beta-name-that-can-be-truncated,NULL
```

The summary and query identifiers are written to stderr, so redirected stdout contains only result data.

SQL can be supplied directly, from a file, or from stdin:

```text
qcli --config examples/milestone-2.env --target demo --file report.sql --format jsonl
printf 'generate 10' | qcli --config examples/milestone-2.env --target demo --file - --format tsv
```

Supported formats are `table`, `vertical`, `csv`, `tsv`, `json`, and `jsonl` (`ndjson` is accepted as an alias).

## Delivered

- One engine-independent `StreamOutput` pipeline shared by all frontends.
- Incremental batch consumption; previously written batches are not retained.
- Human table and vertical output with display-only truncation and decimal rounding.
- CSV and TSV with a single header across multiple batches.
- JSON arrays streamed without retaining the complete result.
- JSONL with one object per line.
- Exact decimals in machine formats; JSON encodes decimals as strings to avoid consumer precision loss.
- Explicit `null` values in JSON and `NULL` in delimited formats.
- Stable schema field ordering in JSON.
- Unicode-preserving machine output.
- Nested Arrow values in JSON and JSONL.
- Direct SQL through `--command`.
- SQL files through `--file PATH`.
- SQL from stdin through `--file -`.
- Target-level `output_format` defaults with command-line override.
- Result-only stdout and diagnostics/query metadata on stderr.
- Stable initial exit codes: usage/input `2`, configuration `3`, query `5`, output `7`.
- Broken pipes are treated as successful downstream termination without noisy diagnostics.
- Deterministic `generate N` support in the demo adapter.
- Generated results are emitted in batches of at most 1,024 rows through bounded channels.

## Exact-value contract

Human output may shorten strings and round decimals according to the resolved target settings. Those transformations never alter the Arrow batch or machine output.

For the sample decimal `123.456789` with `decimal_places=3`:

- Table and vertical display `123.457`.
- CSV and TSV emit `123.456789`.
- JSON and JSONL emit `"123.456789"`.

JSON decimal strings are intentional: they preserve Decimal128 precision for clients whose native JSON number type is an IEEE-754 double.

## Streaming and backpressure evidence

The demo adapter recognizes `generate N` and constructs at most 1,024 rows at a time. The core delivers those batches through a bounded Tokio channel, and the output pipeline serializes one batch before requesting the next.

The normal test suite streams 10,000 generated rows through both CSV and JSONL and asserts the maximum observed batch size. The explicit release gate streams one million rows through both formats into an I/O sink:

```text
cargo test --release -p qcli-core \
  tests::million_rows_stream_in_bounded_batches_to_csv_and_jsonl \
  -- --ignored --exact
```

The gate passed in 0.83 seconds after the release build was available. It is ignored in ordinary debug test runs so routine development checks stay fast.

## Automated evidence

`cargo test --workspace` passes 23 tests; the explicit million-row release gate is the one intentionally ignored test in that command.

`cargo clippy --workspace --all-targets -- -D warnings` passes with no warnings.

`cargo fmt --all -- --check` passes.

Golden and integration tests cover:

- Exact decimals independent of human display settings.
- NULL handling and Unicode.
- Nested JSON values.
- All six formats.
- File and stdin SQL sources.
- Machine-output stdout isolation.
- Usage, configuration, and query exit codes.
- Output errors and broken-pipe classification.
- Multi-batch and million-row bounded streaming.

## Reusability boundaries exercised

- Adapters produce Arrow batches and do not know the selected output format.
- `qcli-output` accepts Arrow batches and does not know the source engine.
- The CLI owns argument, stdin/file, stdout/stderr, and process-exit policy.
- Core query orchestration remains independent of CLI and serialization concerns.
- The same output pipeline can be reused by the future REPL and HTTP export paths.
- The generated-load path uses the normal adapter, core, channel, and output contracts.

## Known limitations

- Only the deterministic demo adapter executes queries; Trino begins in Milestone 4.
- Table and vertical output currently support the scalar types exercised by the demo. Machine JSON supports nested Arrow values.
- CSV and TSV inherit Apache Arrow's restriction against nested values; JSON or JSONL should be used for nested results.
- Tables are rendered once per incoming batch, so a multi-batch human result repeats its header and border.
- Query summaries do not yet include timing or engine metrics.
- Exit code `4` is reserved for future connection/authentication failures but is not emitted yet.
- SQL files are read as one query; statement splitting belongs to a later milestone.
- JSON precision normalization currently special-cases top-level Decimal128 columns. Nested decimals will require recursive schema-aware normalization.

## Prerequisites established for Milestone 4

- A production adapter only needs to stream exact Arrow batches through the existing driver contract.
- Remote results immediately gain all batch formats and input modes.
- Large paginated Trino results can map pages to bounded Arrow batches without a new output architecture.
- Trino errors can use the stable query/connection process contracts rather than leaking protocol details into the CLI.
