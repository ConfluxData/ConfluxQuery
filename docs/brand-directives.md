# ConfluxQuery Brand and Terminology Directives

This document is the authoritative naming and messaging reference for the
ConfluxQuery product, its documentation, website content, release copy, package
descriptions, diagrams, and future visual design. Product-facing changes must
follow this contract before they are published.

## Brand architecture

| Role | Authoritative name | Meaning |
|---|---|---|
| Umbrella product | **ConfluxQuery** | The complete open-source query toolset. |
| Terminal offering | **ConfluxQuery CLI** | Interactive terminal and batch automation. |
| Server offering | **ConfluxQuery Gateway** | Governed HTTP and Arrow Flight SQL service. |
| Publisher | **ConfluxData** | The organization publishing ConfluxQuery. |
| Executable | `qcli` | Stable command installed on a machine. |
| Server invocation | `qcli serve` | Command that starts ConfluxQuery Gateway. |

The preferred publisher lockup is:

> **ConfluxQuery** by **ConfluxData**

The relationship must be introduced on the documentation homepage, repository
README, product landing pages, release descriptions, and package metadata.

## Primary message

### Headline

> Query anywhere. Govern access once.

### Product description

> ConfluxQuery is an open-source query toolset by ConfluxData. Use
> ConfluxQuery CLI to query Trino, Databricks SQL, and Snowflake from the
> terminal, or deploy ConfluxQuery Gateway to connect applications through
> HTTP, Arrow Flight SQL, ADBC, and JDBC.

### Short description

> An open-source query CLI and gateway for modern cloud data platforms.

### Technical positioning

> A multi-engine query access platform with one shared query, identity,
> session, result, and audit model.

## Offering messages

### ConfluxQuery CLI

> One consistent interactive and automated query experience across Trino,
> Databricks SQL, and Snowflake.

Use technical commands immediately after the product name:

```text
Start ConfluxQuery CLI with `qcli --target trino-prod`.
```

### ConfluxQuery Gateway

> A governed, Arrow-native query access layer for applications and data tools.

Use technical commands immediately after the product name:

```text
Start ConfluxQuery Gateway with `qcli serve`.
```

## Naming rules

### Required forms

- Use **ConfluxQuery** for the umbrella product.
- Use **ConfluxQuery CLI** for terminal, interactive, and batch workflows.
- Use **ConfluxQuery Gateway** for server mode, HTTP, Flight SQL, shared
  service operation, and cluster deployment.
- Use **ConfluxData** when naming the publisher.
- Use `qcli` only for the executable, repository, packages, technical
  identifiers, configuration, protocol contracts, code symbols, and literal
  commands.
- On first mention in an introductory page, establish that ConfluxQuery is
  distributed as the `qcli` command.

### Prohibited or deprecated forms

Do not introduce these as product names:

- “Conflux Query” with a space.
- “Conflux QUery” or other inconsistent capitalization.
- “qcli Gateway.”
- “qcli CLI.”
- “QCLI” as a human-facing product name.
- “ConfluxQuery Server” when **ConfluxQuery Gateway** is intended.

Lowercase “query gateway” is acceptable only as a generic architectural
category, not as the offering name.

## Stable technical contracts

Branding must not rename or alias these interfaces:

| Contract | Stable form |
|---|---|
| Binary and shell command | `qcli` |
| Serve subcommand | `qcli serve` |
| Repository | `qcli` |
| Rust package/crate prefix | `qcli`, `qcli-*` |
| Configuration directory | `~/.qcli/` |
| Default configuration | `~/.qcli/.env` |
| Environment prefix | `QCLI_*` |
| Query/session identifier prefix | `qcli_` and existing formats |
| Audit marker | `qcli_audit` |
| HTTP routes | `/v1/...`, `/health/...` |
| Flight metadata/session contracts | Existing `qcli-*` names |
| Release archives and container coordinates | Existing `qcli` naming |
| Terminal prompt | Existing target/catalog/schema prompt |

Examples, code blocks, logs, filenames, and API payloads must preserve exact
technical spelling even when surrounding prose uses ConfluxQuery.

## Product boundaries

Messaging must describe current behavior accurately:

- ConfluxQuery routes native SQL to one selected configured target.
- ConfluxQuery does not currently provide cross-engine distributed joins.
- ConfluxQuery is not a database, query engine, semantic layer, or transaction
  coordinator.
- Do not claim “write once, run anywhere” until dialect transpilation is
  implemented and released.
- Do not call the product a “federated query engine.”
- ODBC/BI remains experimental until named M20 integrations are certified.
- The branded ConfluxQuery JDBC Driver owns the stable `jdbc:qcli://` contract
  and delegates its Flight SQL transport to Apache Arrow JDBC.

Prefer:

> One workflow across multiple native SQL engines.

Avoid:

> Universal SQL that runs unchanged everywhere.

## Voice and tone

ConfluxQuery communication is:

- Technically precise rather than promotional without evidence.
- Direct and task-oriented.
- Confident about released capabilities and explicit about limitations.
- Welcoming to individual developers while credible for platform operators.
- Consistent in calling engines, protocols, identity systems, and clients by
  their correct names.

Use short headlines, concrete examples, and active verbs. Explain what a
feature solves before listing its components.

## Visual direction

ConfluxQuery inherits the ConfluxData family identity. Documentation and web
design should use:

- ConfluxData's indigo-led palette with cyan/green connectivity accents.
- Orange for warnings, experimental capabilities, or operational attention.
- A product mark suggesting multiple paths converging into a query cursor,
  terminal, or Arrow stream.
- Monospace typography as a technical accent, not the entire visual system.
- The publisher relationship without allowing “ConfluxData” to overwhelm the
  product title.

Recommended text lockup:

```text
[mark] ConfluxQuery
       by ConfluxData
```

The terminal prompt remains compact and is not prefixed with the product name.

## Information responsibilities

### ConfluxData website

The publisher website owns discovery, problem framing, product value,
comparison, calls to action, and links to documentation, releases, and GitHub.

### ConfluxQuery documentation

The product documentation owns onboarding, concepts, complete reference,
integration, deployment, security, operations, upgrades, and troubleshooting.
It must use the same product and offering names as the publisher website.

### Repository and packages

The repository README and package descriptions introduce ConfluxQuery, then
use stable `qcli` technical names for installation and commands.

## SEO and metadata vocabulary

Use these phrases naturally where relevant:

- ConfluxQuery
- ConfluxQuery CLI
- ConfluxQuery Gateway
- query CLI for Trino, Databricks SQL, and Snowflake
- Arrow Flight SQL query gateway
- ADBC and JDBC query access
- multi-engine query access platform
- open-source query gateway

Do not keyword-stuff pages or imply unsupported engine/client coverage.

## Review checklist

Before publishing product-facing content, verify:

- [ ] The umbrella name is exactly **ConfluxQuery**.
- [ ] CLI and Gateway offerings use their authoritative names.
- [ ] The publisher is identified as **ConfluxData** where context requires it.
- [ ] Literal commands and machine contracts still use `qcli`.
- [ ] Supported, experimental, and planned claims remain distinct.
- [ ] No federated-engine, universal-SQL, or released-transpilation claim was
      introduced.
- [ ] The page explains the user outcome, not only implementation details.
- [ ] Links and calls to action point to the correct offering documentation.

## Change control

Changes to umbrella naming, offering names, primary headline, stable technical
contracts, or product boundaries must update this directive first. Product
documentation and website repositories should treat this file as the upstream
reference for their own branding changes.
