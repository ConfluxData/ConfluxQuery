# Milestone 24.5 Notes: Product Documentation Portal

## Outcome

M24.5 turns qcli's engineering records into a coherent product manual. A new
user can understand the problem, install and configure the CLI, run and
automate queries, deploy the gateway, integrate applications, secure and scale
the service, and troubleshoot failures without reconstructing behavior from
milestone notes or source code.

The portal is a branded MkDocs Material static site suitable for GitHub Pages
or any static web host.

## Information architecture

- **Product:** inspiration, problem statement, principles, evolution, and
  supported/experimental/planned policy.
- **Get started:** verified installation, CLI first query, authenticated HTTP,
  Swagger, and Flight SQL quickstarts.
- **Concepts:** layered architecture, target resolution, versioned sessions,
  query/result lifecycle, Arrow data model, security boundaries, ingestion,
  and high availability.
- **CLI:** complete commands/options, exit codes, interactive commands,
  configuration properties, machine output, scripting, and engine setup.
- **Gateway:** HTTP resource workflows, Flight SQL behavior, authentication,
  ownership, quotas, health, deployment modes, and operations.
- **Ecosystem:** Python, Go, Java ADBC, upstream Arrow JDBC, Rust, JavaScript,
  Python HTTP, curl, and honest C/C++/R/ODBC boundaries.
- **How-to:** common task recipes, production transitions, upgrades, rollback,
  and symptom-oriented troubleshooting.
- **Reference:** compatibility, platforms, feature status, release contract,
  roadmap, execution plan, and architecture decisions.

Legacy milestone notes remain buildable and linkable but are deliberately
excluded from primary navigation so they do not overwhelm the product journey.

## Delivered

- `mkdocs.yml` with product navigation, search, light/dark themes, Mermaid,
  code-copy controls, strict validation, and qcli branding.
- Repository-native SVG identity and small visual styling layer.
- Pinned documentation dependencies in `requirements-docs.txt`.
- Repository-local virtual environment workflow that does not modify a
  system-managed Python installation.
- `make docs-serve`, `make docs-build`, and `make docs-check` workflows.
- Single-command deployment through `make docs-deploy`; it bootstraps the
  local environment and invokes strict `mkdocs gh-deploy --force`.
- A pull-request/main CI documentation job.
- A main-branch/manual GitHub Pages workflow that rebuilds, validates, and
  deploys the exact site.
- Drift detection that executes qcli help and compares product references with
  CLI help, configuration source registries, and REPL commands.
- Python syntax checks for published conformance examples.
- Git ignores for generated site output and the local documentation venv.

## Reference coverage evidence

The automated reference check discovered and verified documentation for:

```text
10 public command families
20 CLI and server options
54 accepted configuration properties
18 interactive shell commands
```

The property and REPL checks derive their inventory from the implementation,
while CLI commands/options derive from the executable's help output. A future
feature addition therefore fails CI until the corresponding reference changes.

## Validation

The following completion gates passed:

```text
make docs-check
python -m mkdocs build --strict
cargo run --quiet --locked -- --help
python scripts/check-cli-docs.py ...
python -m py_compile conformance/m24/http_profile.py conformance/m19/python/profile.py
ruby YAML parse of GitHub workflows; MkDocs strict config load
bash -n scripts/check-docs.sh
git diff --check
```

The strict build reports no omitted-navigation, invalid-link, absolute-link, or
unrecognized-link warnings. `make -n docs-deploy` verifies the one-command
bootstrap and publication path without mutating the remote `gh-pages` branch
during milestone completion.

## Publication model

Every pull request and main push runs the documentation check as part of the
normal CI workflow. Documentation changes merged to `main` also trigger the
dedicated deployment workflow. Maintainers can reproduce the same publication
locally with:

```bash
make docs-deploy
```

This uses MkDocs' `gh-deploy` mechanism and requires authenticated Git write
access. The generated `site/` directory is ephemeral and is never committed to
the source branch.

## Content decisions

- The portal explains the current product; chronological milestone notes are
  evidence, not the primary user experience.
- Supported, experimental, and planned features are never mixed. ODBC remains
  experimental and the qcli-branded JDBC driver remains M25.
- Engine-specific namespace and authentication differences are documented
  instead of being hidden behind generic SQL language.
- Examples use environment-based secrets and protected-file patterns.
- Architecture diagrams are Mermaid source so reviews can inspect changes.
- Raw OpenAPI remains the endpoint authority; narrative pages teach resource
  lifecycle and production use.

## Accepted limitations

- The site has qcli branding but no commissioned design system or marketing
  illustration set.
- Rust native Flight SQL and Java ADBC full programs remain linked to pinned
  repository conformance sources where reproducing entire programs would make
  the guide harder to maintain.
- External-link reachability is not checked during every offline build; MkDocs
  validates internal structure and recognized links.
- Versioned historical sites can be added with `mike` after the first tagged
  documentation release; current publishing tracks `main`.
- Live cloud-engine tutorials still depend on credentials and infrastructure
  supplied by the reader's organization.

## Next milestone

M25 can publish a branded Type 4 JDBC driver and add its generated Javadoc,
Maven coordinates, connection property reference, and certified examples to
this portal.
