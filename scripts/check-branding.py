#!/usr/bin/env python3
"""Enforce the human-facing ConfluxQuery terminology contract."""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
DIRECTIVE = ROOT / "docs/brand-directives.md"

required_directives = [
    "**ConfluxQuery**",
    "**ConfluxQuery CLI**",
    "**ConfluxQuery Gateway**",
    "**ConfluxData**",
    "`qcli serve`",
    "Query anywhere. Govern access once.",
]
directive_text = DIRECTIVE.read_text()
missing_directives = [item for item in required_directives if item not in directive_text]
if missing_directives:
    raise SystemExit("brand directive is missing: " + ", ".join(missing_directives))

published = [
    ROOT / "README.md",
    ROOT / "docs/index.md",
    ROOT / "docs/product-design.md",
    ROOT / "docs/features-roadmap.md",
    ROOT / "docs/unified-connectivity-release.md",
]
for directory in ["product", "getting-started", "concepts", "reference", "server", "guides"]:
    published.extend(sorted((ROOT / "docs" / directory).glob("*.md")))

prohibited = {
    r"\bConflux Query\b": "use ConfluxQuery without a space",
    r"\bqcli Gateway\b": "use ConfluxQuery Gateway",
    r"\bqcli CLI\b": "use ConfluxQuery CLI",
    r"\bqcli (?:is|provides|offers|evolved|labels|standardizes)\b": (
        "use ConfluxQuery for the human-facing product"
    ),
}
violations = []
for path in published:
    text = path.read_text()
    for pattern, guidance in prohibited.items():
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            line = text.count("\n", 0, match.start()) + 1
            violations.append(f"{path.relative_to(ROOT)}:{line}: {guidance}")

if violations:
    raise SystemExit("branding violations:\n" + "\n".join(violations))

mkdocs = (ROOT / "mkdocs.yml").read_text()
homepage = (ROOT / "docs/index.md").read_text()
for expected in [
    "site_name: ConfluxQuery",
    "ConfluxQuery CLI",
    "ConfluxQuery Gateway",
    "ConfluxData",
]:
    if expected not in mkdocs and expected not in homepage:
        raise SystemExit(f"published site identity is missing {expected!r}")

print(f"branding contract passes across {len(published)} product-facing files")
