# CLI output and automation

## Machine formats

Use CSV, TSV, JSON, or JSONL for programs. These formats preserve complete
values, nulls, Unicode, decimals, nested values, and binary representation
without human truncation.

```bash
qcli --target trino-prod --format csv --file report.sql > report.csv
qcli --target trino-prod --format jsonl --command 'select * from events' \
  | jq -c 'select(.severity == "ERROR")'
```

`table` and `vertical` are for people. They honor decimal shortening, string
truncation, widths, null display, and other presentation properties without
mutating source Arrow values.

## stdin

```bash
printf '%s\n' 'select current_date' \
  | qcli --target snowflake-prod --file - --format json
```

## Exit-code handling

```bash
if ! qcli --target trino-prod --file job.sql --format jsonl > result.jsonl; then
  code=$?
  echo "qcli failed with class $code" >&2
  exit "$code"
fi
```

See the [exit code table](../reference/cli.md#exit-codes). Errors and timing go
to stderr so stdout remains parseable. Broken downstream pipes are success.

## Avoid secret leakage

- Put credentials in environment variables or a secret manager.
- Quote SQL to prevent shell expansion.
- Prefer `--file` when SQL contains shell-sensitive text.
- Do not pass tokens as command-line arguments.
- Disable history or use non-interactive mode for sensitive SQL.
- Capture stderr in an access-controlled log because it contains target and
  query diagnostics, though ConfluxQuery redacts configured secrets.

## Stable jobs

Pin a ConfluxQuery release and target configuration, specify an explicit machine
format, set query/connect timeouts, and test `target capabilities` during
deployment. Do not parse table borders or prompts.
