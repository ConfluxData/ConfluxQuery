#!/usr/bin/env python3
"""Fail when public CLI/config/REPL surfaces are absent from references."""

from pathlib import Path
import re
import sys

help_text = Path(sys.argv[1]).read_text()
cli_reference = Path(sys.argv[2]).read_text()
repl_reference = Path(sys.argv[3]).read_text()
config_reference = Path(sys.argv[4]).read_text()
config_source = Path(sys.argv[5]).read_text()
repl_source = Path(sys.argv[6]).read_text()

entries = []
for line in help_text.splitlines():
    match = re.match(r"\s{2}((?:--)?[a-z][a-z0-9-]*(?:\s+[a-z-]+)*)", line)
    if match:
        entries.append(match.group(1).strip())

options = sorted(set(re.findall(r"--[a-z][a-z-]+", help_text)))
missing = [entry for entry in entries if f"`{entry}`" not in cli_reference]
missing += [option for option in options if f"`{option}" not in cli_reference]
if missing:
    raise SystemExit("CLI reference is missing help entries: " + ", ".join(missing))

property_block = config_source.split("fn allowed_properties", 1)[1].split(
    "fn validate_typed_value", 1
)[0]
properties = sorted(set(re.findall(r'"([a-z][a-z0-9_]*)"', property_block)))
missing_properties = [
    prop for prop in properties if f"`{prop}`" not in config_reference
]
if missing_properties:
    raise SystemExit(
        "configuration reference is missing properties: "
        + ", ".join(missing_properties)
    )

repl_commands = sorted(
    set(re.findall(r'"(\\\\[a-z][a-z-]*)', repl_source))
)
repl_commands = [command.replace("\\\\", "\\", 1) for command in repl_commands]
missing_repl = [
    command for command in repl_commands if f"`{command}" not in repl_reference
]
if missing_repl:
    raise SystemExit(
        "interactive reference is missing commands: " + ", ".join(missing_repl)
    )

print(
    f"references cover {len(entries)} commands, {len(options)} options, "
    f"{len(properties)} properties, and {len(repl_commands)} REPL commands"
)
