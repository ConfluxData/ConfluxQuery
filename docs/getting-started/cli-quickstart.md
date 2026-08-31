# ConfluxQuery CLI quickstart

## 1. Create the configuration

The `qcli` command reads `~/.qcli/.env` by default. Despite the filename, this
is a sectioned ConfluxQuery configuration file. `[default]` contains shared behavior; every other
section is a selectable target.

```ini
[default]
output_format = table
decimal_places = 3
string_truncate = 80
timing = true

[trino-local]
engine = trino
url = http://127.0.0.1:8080
user = analyst
catalog = tpch
schema = tiny
```

```bash
mkdir -p ~/.qcli
chmod 700 ~/.qcli
chmod 600 ~/.qcli/.env
qcli config check
qcli target list
```

## 2. Test the target

```bash
qcli target show trino-local
qcli target capabilities trino-local
qcli target test trino-local
```

`target show` redacts secrets. `target list` only discovers section headers and
engines, so it does not require credential environment variables to be set.

## 3. Run one query

```bash
qcli --target trino-local --command 'select * from tpch.tiny.nation limit 5'
```

For machine pipelines:

```bash
qcli --target trino-local --format jsonl \
  --command 'select nationkey, name from tpch.tiny.nation' > nations.jsonl
```

## 4. Enter the interactive shell

```bash
qcli --target trino-local
```

```text
trino-local[tpch.tiny]> \status
trino-local[tpch.tiny]> \catalogs
trino-local[tpch.tiny]> \schemas
trino-local[tpch.tiny]> \tables nation*
trino-local[tpch.tiny]> select count(*) from nation;
trino-local[tpch.tiny]> \q
```

Without `--target`, ConfluxQuery CLI prompts when multiple targets exist.
Target switches are atomic: it validates the destination before replacing the
active session.

## Next

- [Complete CLI reference](../reference/cli.md)
- [Interactive shell](../reference/repl.md)
- [Engine setup](../guides/engines.md)
- [Automation and exact output](../guides/cli-automation.md)
