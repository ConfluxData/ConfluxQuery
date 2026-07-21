# Milestone 5 Notes: Interactive Terminal

Status: Complete

Completed: 2026-07-21

## Demonstrated outcome

qcli now starts an interactive shell when invoked without a batch command. A
target can be selected from the startup picker or supplied directly:

```text
qcli
qcli --target trino-local
```

The live Trino demonstration on `localhost:8080` executed a multiline query,
rendered its result, updated a session option, and reported the retained query
status without leaving the shell:

```text
Connected to 'trino-local' (trino). Type \help for help.
trino-local> select
   -> current_catalog, current_schema;
Query ID: qcli_0000000000000001
┌───────┬───────┐
│ _col0 │ _col1 │
├───────┼───────┤
│ tpch  │ tiny  │
└───────┴───────┘
1 rows
Engine query ID: 20260721_131013_00011_j9cnm
Time: 0.059s
trino-local> \set decimal_places 8
decimal_places = 8
trino-local> \status
target=trino-local engine=trino session=qcli_20260721_1310_01 version=2 status=completed: 1 rows, ...
```

## Architecture

The new `qcli-repl` crate owns terminal behavior and depends only on reusable
configuration, core, driver API, and output contracts. It receives registered
`EngineAdapter` values from the executable and submits every statement through
`SessionManager` and `QueryService`; it contains no Trino-specific branch.

`QueryHandle::next_item` was added to let interactive and future HTTP frontends
drain lifecycle events and bounded Arrow batches concurrently. This prevents
either channel from blocking the other while retaining the existing batch APIs.

Rustyline 18.0.1 supplies line editing, persistent file history, bracketed
paste, prompt signal handling, and the highlighting hook. It is MIT licensed
and compiled successfully under qcli's Rust 1.86 MSRV.

## Delivered

- Interactive startup and optional `--target TARGET` selection.
- Readable session IDs in `username_YYYYMMDD_HHMM_XX` form, using the target
  user with operating-system username and `qcli` fallbacks.
- Numbered/name-based target picker for configurations with multiple targets.
- Primary and continuation prompts containing the active target.
- Multiline SQL collection terminated by an unquoted final semicolon.
- Removal of the local delimiter before native SQL is sent to the engine.
- Lightweight SQL keyword highlighting without parsing or rewriting SQL.
- Persistent history beside the qcli configuration file.
- `history` and `history_limit` configuration controls.
- Sensitive-query filtering for password, secret, token, credential, and user
  management statements.
- History files forced to permission mode `0600` on Unix.
- Concurrent event and result-batch draining with bounded memory.
- Immediate qcli query ID, remote engine query ID, row count, and optional time.
- SIGINT cancellation that returns to the existing interactive session.
- Ctrl-C buffer clearing at the prompt and Ctrl-D shell exit.
- Versioned session changes through `SessionManager::set_option`.
- Runtime human-output format and timing controls.
- Redacted connection-property inspection.
- Query buffer printing and clearing.

## Meta-commands

```text
\help
\status
\set NAME VALUE
\format table|vertical|csv|tsv|json|jsonl
\timing [on|off]
\properties
\p
\r
\q
```

`\set` creates a new versioned session snapshot. A query already submitted from
an earlier snapshot is unaffected. `\properties` renders configuration values
through the redacted configuration type and overlays only explicit interactive
changes.

## Cancellation behavior

While a query is running, SIGINT calls `QueryHandle::cancel`, continues draining
the engine response, reports the structured cancellation outcome, and returns
to the same target prompt. A query-scoped Unix signal registration is installed
after line editing releases the terminal, avoiding interference between the
line editor's prompt handler and asynchronous cancellation.

The deterministic PTY gate starts `wait-for-cancel;`, waits until qcli reports
its query ID, sends SIGINT to the qcli process, observes cancellation, verifies
that the prompt returns, and then exits with Ctrl-D.

## Session ID convention

Interactive sessions use a short, traceable identifier such as:

```text
deepak_20260721_1605_01
```

The timestamp uses local time through the current minute. A process-wide
two-digit counter distinguishes sessions created for the same minute, and
characters outside ASCII letters, digits, and underscore in the configured
username are replaced with `_`. This identifier is diagnostic context, not a
secret or authorization token.

## Automated evidence

Pseudo-terminal tests cover:

- Interactive target picker by exact target name.
- Primary and continuation prompts.
- Multiline statement execution and result rendering.
- Session option mutation and version increment.
- `\status` and redacted `\properties` behavior.
- SIGINT cancellation without exiting the shell.
- Ctrl-D exit after cancellation.

Unit tests cover statement boundaries with quoted semicolons and conservative
sensitive-history detection. Existing batch, configuration, core, output, and
Trino tests remain unchanged and passing.

The live PTY gate used the existing Milestone 4 Trino target and passed against
the coordinator on `localhost:8080`.

## Known limitations

- SQL boundary detection currently handles quoted delimiters but does not yet
  model semicolons inside SQL comments or dollar-quoted strings.
- Highlighting intentionally covers common SQL keywords rather than performing
  dialect-specific parsing.
- Completion and metadata-aware suggestions begin with Milestone 6.
- The terminal currently renders each arriving Arrow batch as its own table;
  pager-aware continuous table layout remains future output work.
- History filtering is conservative string detection, not a SQL parser. Users
  can disable persistent history with `history=false` for stricter environments.
- Target switching, catalog/schema navigation, and metadata commands belong to
  Milestone 6.

## Prerequisites established for Milestone 6

- A reusable REPL boundary exists independently of concrete engine adapters.
- The prompt is driven by logical session state.
- Session overrides are versioned and queries use immutable snapshots.
- Meta-command routing and query-buffer handling have stable extension points.
- Interactive cancellation, status, history, and result rendering are working.
