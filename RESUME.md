# Resume the ConfluxQuery project

This file is the durable handoff for continuing the ConfluxQuery development
conversation after moving the repository or opening a new Codex session. The
repository and Git history are authoritative; this is a navigation summary,
not a replacement for the detailed documents.

## Resume prompt

After opening the repository at its new location, send this prompt:

```text
Resume the ConfluxQuery project from RESUME.md and the repository history.
Read the referenced authoritative documents before making changes. Preserve
the established branding, milestone gates, documentation standards, and the
practice of committing completed milestone work. First verify pwd, git status,
and the latest commit, then continue from the current decision point.
```

If continuing the existing chat, also state the new absolute path:

```text
The repository moved to <new-absolute-path>. Continue this conversation using
that workspace and follow RESUME.md.
```

## Product identity

- Umbrella product: **ConfluxQuery** by **ConfluxData**.
- Offerings: **ConfluxQuery CLI** and **ConfluxQuery Gateway**.
- Stable executable and technical identifier: `qcli`.
- Headline: **Query anywhere. Govern access once.**
- Current category: open-source multi-engine query access layer.
- Engines: Trino, Databricks SQL, and Snowflake.
- Protocols: terminal/batch, HTTP/OpenAPI, Arrow Flight SQL, ADBC, and the
  branded ConfluxQuery JDBC Driver.
- One query executes natively on one selected configured target. ConfluxQuery
  is not a database, federated query engine, semantic layer, or transaction
  coordinator.

The complete naming and claims contract is in
[`docs/brand-directives.md`](docs/brand-directives.md).

## Authoritative documents

Read these before product, architecture, milestone, or launch work:

1. [`docs/execution-plan.md`](docs/execution-plan.md) — authoritative milestone
   sequence, detailed gates, work packages, and implementation order.
2. [`docs/features-roadmap.md`](docs/features-roadmap.md) — long-term product
   direction and provisional post-M26 candidates.
3. [`docs/product-design.md`](docs/product-design.md) — product and technical
   design.
4. [`docs/brand-directives.md`](docs/brand-directives.md) — mandatory naming,
   positioning, visual, and current-claim boundaries.
5. [`launch.md`](launch.md) — website, GitHub, community, social, demo, and
   design-partner launch copy.
6. [`docs/connectivity-compatibility.md`](docs/connectivity-compatibility.md) —
   exact supported client and engine boundaries.
7. [`docs/milestones/`](docs/milestones/) — completion evidence and accepted
   limitations for individual milestones.

The documentation portal is configured by [`mkdocs.yml`](mkdocs.yml). Validate
documentation with:

```text
PATH="$PWD/.venv-docs/bin:$PATH" bash scripts/check-docs.sh
```

If the local documentation environment is absent, install
`requirements-docs.txt` into a virtual environment first.

## Current milestone state

- M1–M11: complete.
- M12 packaged release: still marked in progress.
- M13–M19: complete.
- M20 ODBC/BI: still experimental and marked in progress.
- M21–M25: complete, including enterprise identity, optional HA, unified
  connectivity release, product documentation, and branded JDBC.
- M26 dialect-aware SQL transpilation: defined and pending.
- M27–M34: provisional candidates only; they are not yet in the authoritative
  implementation sequence.

The next accepted implementation milestone is M26. However, the most recent
conversation intentionally left room for one more product-roadmap review before
implementation begins.

## M26 decision

M26 is a production-quality, opt-in, read-only transpilation milestone rather
than a parser spike.

- Native pass-through remains `transpile=off` by default.
- Explicit source dialects: Trino, Databricks SQL, and Snowflake.
- `strict` may execute only the certified portable subset.
- `best_effort` is offline-only in M26.
- Writes, DDL, scripting, procedures, administrative SQL, and uncertain
  semantics fail closed before adapter submission.
- CLI, HTTP, Flight SQL, ADBC, and JDBC must produce the same translation
  identity and execute the same immutable generated SQL.
- The detailed stages M26.1–M26.8, demo, boundaries, and exit gates are in the
  M26 section of `docs/execution-plan.md`.

## Provisional post-M26 direction

- M27 — Query Passport and Unified Observability.
- M28 — Query Intelligence, Eligibility, and Plan Analysis.
- M29 — Intelligent Routing, Cost Governance, and Workload Management.
- M30 — Governed MCP and Agent Connectivity.
- M31 — Cross-Engine Migration Validation.
- M32 — Lakehouse Metadata Intelligence.
- M33 — Data Contracts and Result Quality.
- M34 — Public Extension and Policy SDK.

Important M28/M29 split:

- M28 understands query requirements, determines eligible engines, estimates
  resource/cost risk, and recommends a target with evidence. It never changes
  the target.
- M29 owns logical targets, dataset-equivalence evidence, policy evaluation,
  physical target selection, fallback, and the routing audit record.
- Routing considers authorization, equivalent data, semantic compatibility,
  engine capability, availability/resource fit, and SLA before cost.
- Rollout progresses through advisory, shadow, guarded automatic, adaptive,
  and bounded validated modes.

These candidates need another review and full demo/exit-gate definitions before
they become authoritative milestones.

## Commercial and launch decision

Keep the core free and open source during the current adoption phase. Do not
put the CLI or basic self-hosted Gateway behind a paywall.

Near-term commercial path:

- Public open-source release and community adoption.
- Three to five paid design partners.
- Paid migration/portability assessments.
- Paid deployment, integration, and priority-support engagements.
- Later, an enterprise or managed/BYOC control plane for central management,
  policy, cost intelligence, fleet operations, retention, and support.

Current launch claim:

> ConfluxQuery is an open-source CLI and governed query Gateway for Trino,
> Databricks SQL, and Snowflake. It gives engineers, applications, and data
> tools one consistent way to connect, execute, and manage queries—without
> replacing the platforms that already run them.

Future vision, which must remain clearly labelled as future:

> ConfluxQuery is becoming the governed query control plane for modern data
> platforms—connect, translate, inspect, optimize, and route analytical
> workloads across the best authorized engine.

Do not claim universal SQL, automatic routing, cost savings, cross-engine
joins, full JDBC compliance, production ODBC, or governed agent access before
their milestone evidence exists.

## Working conventions established with the user

- Every milestone must be independently demoable.
- The milestone definition and sequence live in `docs/execution-plan.md`.
- On completion, write
  `docs/milestones/milestone-<number>-notes.md` with behavior, evidence,
  prerequisites, and accepted limitations.
- Update architecture, user documentation, compatibility, feature status,
  changelog, CI, and release workflows when the milestone changes them.
- Run tests proportional to risk and the strict documentation gate.
- Always commit completed milestone work.
- Preserve unrelated user changes and inspect a dirty worktree before editing.
- Native SQL behavior and existing technical names remain backward compatible
  unless a milestone explicitly changes their contract.

## Recent checkpoint commits

At the time this handoff was written, the recent history was:

```text
c6c8921 docs: add ConfluxQuery launch messaging
79770a7 docs: define intelligent query routing roadmap
540444a docs: expand ConfluxQuery product roadmap
002d60e docs: define M26 SQL transpilation milestone
160c191 feat: complete M25 ConfluxQuery JDBC driver
bd65cb4 docs: render Material icons on homepage
0029ea0 docs: establish ConfluxQuery product branding
535df74 docs: publish world-class qcli product portal
da53f20 feat: complete unified connectivity release
760f5e8 feat: add optional high availability cluster mode
1599f50 feat: add enterprise identity and transport
```

After moving the repository, verify rather than assume this checkpoint:

```text
pwd
git status --short
git log -5 --oneline
```

## Current decision point

No implementation is active. The repository was clean before this handoff was
created. The next conversation can either:

1. Perform the requested second review of the provisional M27–M34 product
   roadmap; or
2. Start M26.1 with the dialect inventory, representative query corpus, Rust
   parser/transpiler evaluation, and ADR.

Do not begin M27 or later implementation before M26 is completed and the
provisional milestones are promoted into the authoritative execution plan.
