//! Interactive qcli terminal built on the frontend-neutral core services.

use qcli_config::{Config, ResolvedTarget};
use qcli_core::{CoreError, QueryItem, QueryService, SessionManager, SessionSnapshot};
use qcli_driver_api::{EngineAdapter, MetadataRequest, ObjectKind, QueryEvent};
use qcli_metadata::MetadataService;
use qcli_output::{DisplayOptions, OutputError, OutputFormat, StreamOutput};
use rustyline::completion::{Completer, Pair, extract_word};
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::{Config as LineConfig, Editor, Helper};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

#[derive(Debug)]
pub enum ReplError {
    Line(ReadlineError),
    Core(CoreError),
    Output(OutputError),
    Io(io::Error),
    NoTargets,
    UnknownTarget(String),
}

impl fmt::Display for ReplError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Line(error) => write!(f, "terminal input failed: {error}"),
            Self::Core(error) => error.fmt(f),
            Self::Output(error) => error.fmt(f),
            Self::Io(error) => error.fmt(f),
            Self::NoTargets => f.write_str("configuration has no targets"),
            Self::UnknownTarget(target) => write!(f, "target '{target}' does not exist"),
        }
    }
}

impl std::error::Error for ReplError {}

impl From<CoreError> for ReplError {
    fn from(value: CoreError) -> Self {
        Self::Core(value)
    }
}
impl From<OutputError> for ReplError {
    fn from(value: OutputError) -> Self {
        Self::Output(value)
    }
}
impl From<ReadlineError> for ReplError {
    fn from(value: ReadlineError) -> Self {
        Self::Line(value)
    }
}
impl From<io::Error> for ReplError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<qcli_driver_api::DriverError> for ReplError {
    fn from(value: qcli_driver_api::DriverError) -> Self {
        Self::Core(CoreError::Driver(value))
    }
}

struct SqlHelper {
    candidates: Arc<RwLock<Vec<String>>>,
}

impl Completer for SqlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        position: usize,
        _context: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let candidates = self
            .candidates
            .read()
            .expect("completion candidates lock poisoned");
        Ok(completion_pairs(&candidates, line, position))
    }
}

fn completion_pairs(candidates: &[String], line: &str, position: usize) -> (usize, Vec<Pair>) {
    let (start, word) = extract_word(line, position, None, |character| {
        character.is_whitespace() || matches!(character, ',' | '(' | ')')
    });
    let matches = candidates
        .iter()
        .filter(|candidate| {
            candidate
                .to_ascii_lowercase()
                .starts_with(&word.to_ascii_lowercase())
        })
        .map(|candidate| Pair {
            display: candidate.clone(),
            replacement: candidate.clone(),
        })
        .collect();
    (start, matches)
}
impl Hinter for SqlHelper {
    type Hint = String;
}
impl Validator for SqlHelper {}
impl Helper for SqlHelper {}

impl Highlighter for SqlHelper {
    fn highlight<'line>(&self, line: &'line str, _position: usize) -> Cow<'line, str> {
        let keywords = [
            "select", "from", "where", "group", "order", "limit", "as", "join", "on", "with",
            "use", "set",
        ];
        let mut rendered = String::with_capacity(line.len() + 16);
        let mut changed = false;
        for token in
            line.split_inclusive(|character: char| !character.is_alphanumeric() && character != '_')
        {
            let word = token.trim_end_matches(|character: char| {
                !character.is_alphanumeric() && character != '_'
            });
            if keywords
                .iter()
                .any(|keyword| word.eq_ignore_ascii_case(keyword))
            {
                rendered.push_str("\x1b[1;34m");
                rendered.push_str(word);
                rendered.push_str("\x1b[0m");
                rendered.push_str(&token[word.len()..]);
                changed = true;
            } else {
                rendered.push_str(token);
            }
        }
        if changed {
            Cow::Owned(rendered)
        } else {
            Cow::Borrowed(line)
        }
    }

    fn highlight_char(&self, _line: &str, _position: usize, _kind: CmdKind) -> bool {
        true
    }
}

/// Run an interactive session, selecting a target when none was supplied.
///
/// # Errors
///
/// Returns terminal, configuration-selection, query, output, or history I/O
/// failures encountered during the session.
#[allow(clippy::too_many_lines)]
pub async fn run(
    config: &Config,
    requested_target: Option<&str>,
    adapters: Vec<Arc<dyn EngineAdapter>>,
    history_path: &Path,
) -> Result<(), ReplError> {
    let line_config = LineConfig::builder()
        .auto_add_history(false)
        .max_history_size(10_000)?
        .build();
    let completion_candidates = Arc::new(RwLock::new(vec![
        "\\targets".into(),
        "\\use".into(),
        "\\catalogs".into(),
        "\\schemas".into(),
        "\\tables".into(),
        "\\describe".into(),
        "\\use-catalog".into(),
        "\\use-schema".into(),
        "\\status".into(),
        "\\properties".into(),
        "SELECT".into(),
        "FROM".into(),
        "WHERE".into(),
    ]));
    let mut editor = Editor::<SqlHelper, DefaultHistory>::with_config(line_config)?;
    editor.set_helper(Some(SqlHelper {
        candidates: Arc::clone(&completion_candidates),
    }));
    let (interrupt_tx, mut interrupts) = tokio::sync::mpsc::channel(8);
    #[cfg(unix)]
    {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        tokio::spawn(async move {
            while signal.recv().await.is_some() {
                if interrupt_tx.send(()).await.is_err() {
                    break;
                }
            }
        });
    }
    #[cfg(not(unix))]
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() || interrupt_tx.send(()).await.is_err() {
                break;
            }
        }
    });
    let target = choose_target(config, requested_target, &mut editor)?;
    extend_completions(
        &completion_candidates,
        &config
            .targets()
            .map(|target| target.name.clone())
            .collect::<Vec<_>>(),
    );
    let history_enabled = target
        .properties
        .get("history")
        .and_then(|value| value.expose().parse().ok())
        .unwrap_or(true);
    let history_limit = target
        .properties
        .get("history_limit")
        .and_then(|value| value.expose().parse().ok())
        .unwrap_or(10_000);
    editor.set_max_history_size(history_limit)?;
    if history_enabled {
        let _ = editor.load_history(history_path);
    }
    let mut display_properties = target.properties.clone();
    let sessions = SessionManager::default();
    let mut snapshot = sessions.create(target);
    let metadata = MetadataService::new(adapters.clone(), Duration::from_secs(30));
    let service = QueryService::new(adapters, 8);
    let mut buffer = String::new();
    let mut format = property_format(&snapshot);
    let mut timing = property_bool(&snapshot, "timing", true);
    let mut last_status = "no query submitted".to_owned();
    let mut overrides = BTreeMap::new();

    println!(
        "Connected to '{}' ({}). Type \\help for help.",
        snapshot.target, snapshot.engine
    );
    loop {
        let prompt = if buffer.is_empty() {
            context_prompt(&snapshot)
        } else {
            "   -> ".to_owned()
        };
        match editor.readline(&prompt) {
            Ok(line) => {
                let command = line.trim();
                if command.starts_with('\\')
                    && (buffer.is_empty() || matches!(command, "\\p" | "\\r"))
                {
                    if !meta_command(
                        &line,
                        config,
                        &sessions,
                        &metadata,
                        &mut snapshot,
                        &mut format,
                        &mut timing,
                        &mut buffer,
                        &mut last_status,
                        &mut display_properties,
                        &mut overrides,
                        &completion_candidates,
                    )
                    .await?
                    {
                        break;
                    }
                    continue;
                }
                if !buffer.is_empty() {
                    buffer.push('\n');
                }
                buffer.push_str(&line);
                if statement_complete(&buffer) {
                    let sql = std::mem::take(&mut buffer);
                    if history_enabled && safe_for_history(&sql) {
                        let _ = editor.add_history_entry(sql.trim());
                    }
                    let statement = sql
                        .trim_end()
                        .strip_suffix(';')
                        .unwrap_or(sql.trim_end())
                        .trim_end()
                        .to_owned();
                    while interrupts.try_recv().is_ok() {}
                    match execute(
                        &service,
                        snapshot.clone(),
                        statement,
                        format,
                        timing,
                        &mut interrupts,
                    )
                    .await
                    {
                        Ok(outcome) => {
                            last_status = outcome.status;
                            for (name, value) in outcome.session_properties {
                                snapshot = sessions.set_option(
                                    &snapshot.id,
                                    snapshot.version,
                                    name,
                                    value,
                                )?;
                            }
                            metadata.invalidate_target(&snapshot.target);
                        }
                        Err(error) => {
                            eprintln!("error: {error}");
                            last_status = format!("failed: {error}");
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                while interrupts.try_recv().is_ok() {}
                buffer.clear();
                println!("^C");
            }
            Err(ReadlineError::Eof) => break,
            Err(error) => return Err(error.into()),
        }
    }
    if history_enabled {
        save_history(&mut editor, history_path)?;
    }
    Ok(())
}

fn choose_target(
    config: &Config,
    requested: Option<&str>,
    editor: &mut Editor<SqlHelper, DefaultHistory>,
) -> Result<ResolvedTarget, ReplError> {
    if let Some(name) = requested {
        return config
            .target(name)
            .cloned()
            .ok_or_else(|| ReplError::UnknownTarget(name.into()));
    }
    let targets = config.targets().collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(ReplError::NoTargets);
    }
    if targets.len() == 1 {
        return Ok(targets[0].clone());
    }
    println!("Select a target:");
    for (index, target) in targets.iter().enumerate() {
        println!("  {}) {} ({})", index + 1, target.name, target.engine);
    }
    loop {
        let answer = editor.readline("target> ")?;
        let numbered_target = answer
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| targets.get(index));
        if let Some(target) = numbered_target {
            return Ok((*target).clone());
        }
        if let Some(target) = config.target(answer.trim()) {
            return Ok(target.clone());
        }
        eprintln!("Choose a target number or exact name.");
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn meta_command(
    line: &str,
    config: &Config,
    sessions: &SessionManager,
    metadata: &MetadataService,
    snapshot: &mut SessionSnapshot,
    format: &mut OutputFormat,
    timing: &mut bool,
    buffer: &mut String,
    status: &mut String,
    properties: &mut BTreeMap<String, qcli_config::ConfigValue>,
    overrides: &mut BTreeMap<String, String>,
    completions: &Arc<RwLock<Vec<String>>>,
) -> Result<bool, ReplError> {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        ["\\q" | "\\quit"] => return Ok(false),
        ["\\help"] => println!(
            "\\targets | \\use TARGET | \\catalogs [PATTERN] | \\schemas [PATTERN] | \\tables [PATTERN] | \\describe OBJECT | \\use-catalog CATALOG | \\use-schema SCHEMA | \\status | \\set NAME VALUE | \\format FORMAT | \\timing [on|off] | \\properties | \\q"
        ),
        ["\\status"] => println!(
            "target={} engine={} catalog={} schema={} session={} version={} status={}",
            snapshot.target,
            snapshot.engine,
            snapshot
                .properties
                .get("catalog")
                .map_or("-", String::as_str),
            snapshot
                .properties
                .get("schema")
                .map_or("-", String::as_str),
            snapshot.id,
            snapshot.version,
            status
        ),
        ["\\targets"] => {
            for target in config.targets() {
                println!(
                    "{}{} ({})",
                    if target.name == snapshot.target {
                        "* "
                    } else {
                        "  "
                    },
                    target.name,
                    target.engine
                );
            }
        }
        ["\\use", target_name] => {
            let Some(target) = config.target(target_name).cloned() else {
                eprintln!(
                    "target '{target_name}' does not exist; still using '{}'",
                    snapshot.target
                );
                return Ok(true);
            };
            let prospective = request_from_target(&target, None);
            match metadata.catalogs(prospective).await {
                Ok(_) => {
                    let old_target = snapshot.target.clone();
                    *snapshot =
                        sessions.switch_target(&snapshot.id, snapshot.version, target.clone())?;
                    *properties = target.properties;
                    overrides.clear();
                    metadata.invalidate_target(&old_target);
                    *format = property_format(snapshot);
                    *timing = property_bool(snapshot, "timing", true);
                    println!("Switched to '{}' ({})", snapshot.target, snapshot.engine);
                }
                Err(error) => eprintln!(
                    "target switch failed; still using '{}': {error}",
                    snapshot.target
                ),
            }
        }
        ["\\catalogs"] | ["\\catalogs", _] => {
            let pattern = parts.get(1).copied();
            let values = metadata
                .catalogs(metadata_request(snapshot, pattern))
                .await?;
            let names = values
                .into_iter()
                .map(|value| value.name)
                .filter(|name| glob_matches(pattern, name))
                .collect::<Vec<_>>();
            extend_completions(completions, &names);
            for name in names {
                println!("{name}");
            }
        }
        ["\\schemas"] | ["\\schemas", _] => {
            let pattern = parts.get(1).copied();
            let values = metadata
                .schemas(metadata_request(snapshot, pattern))
                .await?;
            let names = values
                .into_iter()
                .map(|value| value.name)
                .filter(|name| glob_matches(pattern, name))
                .collect::<Vec<_>>();
            extend_completions(completions, &names);
            for name in names {
                println!("{name}");
            }
        }
        ["\\tables"] | ["\\tables", _] => {
            let pattern = parts.get(1).copied();
            let values = metadata
                .objects(metadata_request(snapshot, pattern))
                .await?;
            let names = values
                .iter()
                .map(|value| value.name.clone())
                .collect::<Vec<_>>();
            extend_completions(completions, &names);
            for value in values {
                let kind = match value.kind {
                    ObjectKind::Table => "table",
                    ObjectKind::View => "view",
                    ObjectKind::Other => "object",
                };
                println!("{:<40} {kind}", value.name);
            }
        }
        ["\\describe", object] => {
            let columns = metadata
                .describe(metadata_request(snapshot, None), object)
                .await?;
            extend_completions(
                completions,
                &columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>(),
            );
            for column in columns {
                println!(
                    "{:<32} {:<28} {}",
                    column.name,
                    column.data_type,
                    column.comment.as_deref().unwrap_or("")
                );
            }
        }
        ["\\use-catalog", catalog] => {
            let available = metadata.catalogs(metadata_request(snapshot, None)).await?;
            if available.iter().any(|value| value.name == *catalog) {
                *snapshot = sessions.set_option(
                    &snapshot.id,
                    snapshot.version,
                    "catalog".into(),
                    (*catalog).into(),
                )?;
                metadata.invalidate_target(&snapshot.target);
                println!("catalog = {catalog}");
            } else {
                eprintln!("catalog '{catalog}' does not exist; context unchanged");
            }
        }
        ["\\use-schema", schema] => {
            let available = metadata.schemas(metadata_request(snapshot, None)).await?;
            if available.iter().any(|value| value.name == *schema) {
                *snapshot = sessions.set_option(
                    &snapshot.id,
                    snapshot.version,
                    "schema".into(),
                    (*schema).into(),
                )?;
                metadata.invalidate_target(&snapshot.target);
                println!("schema = {schema}");
            } else {
                eprintln!("schema '{schema}' does not exist; context unchanged");
            }
        }
        ["\\set", name, value] => {
            *snapshot = sessions.set_option(
                &snapshot.id,
                snapshot.version,
                (*name).into(),
                (*value).into(),
            )?;
            overrides.insert((*name).into(), (*value).into());
            if *name == "output_format" {
                *format = OutputFormat::from_str(value)?;
            }
            if *name == "timing" {
                *timing = parse_on_off(value).unwrap_or(*timing);
            }
            println!(
                "{name} = {}",
                if sensitive_property(name, properties) {
                    "<redacted>"
                } else {
                    value
                }
            );
        }
        ["\\format", value] => {
            *format = OutputFormat::from_str(value)?;
            println!("format = {value}");
        }
        ["\\timing"] => {
            *timing = !*timing;
            println!("timing is {}", if *timing { "on" } else { "off" });
        }
        ["\\timing", value] => {
            if let Some(enabled) = parse_on_off(value) {
                *timing = enabled;
                println!("timing is {}", if enabled { "on" } else { "off" });
            } else {
                eprintln!("usage: \\timing [on|off]");
            }
        }
        ["\\properties"] => {
            for (name, value) in properties.iter() {
                println!(
                    "{name} = {}",
                    if value.is_secret() {
                        "<redacted>"
                    } else {
                        overrides
                            .get(name)
                            .map_or_else(|| value.display_value(), String::as_str)
                    }
                );
            }
            for (name, value) in overrides
                .iter()
                .filter(|(name, _)| !properties.contains_key(*name))
            {
                println!(
                    "{name} = {}",
                    if sensitive_property(name, properties) {
                        "<redacted>"
                    } else {
                        value
                    }
                );
            }
        }
        ["\\p"] => println!("{buffer}"),
        ["\\r"] => {
            buffer.clear();
            println!("Query buffer cleared.");
        }
        _ => eprintln!("unknown command; type \\help"),
    }
    Ok(true)
}

fn sensitive_property(name: &str, properties: &BTreeMap<String, qcli_config::ConfigValue>) -> bool {
    properties
        .get(name)
        .is_some_and(qcli_config::ConfigValue::is_secret)
        || {
            let name = name.to_ascii_lowercase();
            name == "url"
                || ["password", "token", "secret", "private_key"]
                    .iter()
                    .any(|marker| name.contains(marker))
        }
}

fn context_prompt(snapshot: &SessionSnapshot) -> String {
    let catalog = snapshot.properties.get("catalog");
    let schema = snapshot.properties.get("schema");
    match (catalog, schema) {
        (Some(catalog), Some(schema)) => format!("{}[{catalog}.{schema}]> ", snapshot.target),
        (Some(catalog), None) => format!("{}[{catalog}]> ", snapshot.target),
        _ => format!("{}> ", snapshot.target),
    }
}

fn metadata_request(snapshot: &SessionSnapshot, pattern: Option<&str>) -> MetadataRequest {
    MetadataRequest {
        identity: "local-cli".into(),
        target: snapshot.target.clone(),
        engine: snapshot.engine.clone(),
        properties: snapshot.properties.clone(),
        catalog: snapshot.properties.get("catalog").cloned(),
        schema: snapshot.properties.get("schema").cloned(),
        pattern: pattern.map(str::to_owned),
    }
}

fn request_from_target(target: &ResolvedTarget, pattern: Option<&str>) -> MetadataRequest {
    let properties = target
        .properties
        .iter()
        .map(|(name, value)| (name.clone(), value.expose().to_owned()))
        .collect::<BTreeMap<_, _>>();
    MetadataRequest {
        identity: "local-cli".into(),
        target: target.name.clone(),
        engine: target.engine.clone(),
        catalog: properties.get("catalog").cloned(),
        schema: properties.get("schema").cloned(),
        properties,
        pattern: pattern.map(str::to_owned),
    }
}

fn glob_matches(pattern: Option<&str>, value: &str) -> bool {
    let Some(pattern) = pattern else {
        return true;
    };
    let value = value.chars().collect::<Vec<_>>();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for token in pattern.chars() {
        let mut current = vec![false; value.len() + 1];
        if token == '*' {
            current[0] = previous[0];
        }
        for index in 1..=value.len() {
            current[index] = if token == '*' {
                previous[index] || current[index - 1]
            } else {
                previous[index - 1] && (token == '?' || token == value[index - 1])
            };
        }
        previous = current;
    }
    previous[value.len()]
}

fn extend_completions(completions: &Arc<RwLock<Vec<String>>>, values: &[String]) {
    let mut candidates = completions
        .write()
        .expect("completion candidates lock poisoned");
    candidates.extend(values.iter().cloned());
    candidates.sort_unstable();
    candidates.dedup();
}

async fn execute(
    service: &QueryService,
    snapshot: SessionSnapshot,
    sql: String,
    format: OutputFormat,
    timing: bool,
    interrupts: &mut tokio::sync::mpsc::Receiver<()>,
) -> Result<ExecutionOutcome, ReplError> {
    let started = Instant::now();
    let options = DisplayOptions {
        decimal_places: option(&snapshot, "decimal_places", 3),
        string_truncate: option(&snapshot, "string_truncate", 80),
    };
    let mut handle = service.submit(snapshot, sql)?;
    let query_id = handle.id.clone();
    eprintln!("Query ID: {query_id}");
    let mut output = StreamOutput::new(io::BufWriter::new(io::stdout().lock()), format, options)?;
    let mut engine_id = None;
    let mut session_properties = BTreeMap::new();
    let mut cancelled = false;
    loop {
        tokio::select! {
            item = handle.next_item() => match item {
                Some(QueryItem::Batch(batch)) => output.write_batch(&batch)?,
                Some(QueryItem::Event(QueryEvent::EngineQueryId(id))) => engine_id = Some(id),
                Some(QueryItem::Event(QueryEvent::SessionProperties(properties))) => session_properties.extend(properties),
                Some(QueryItem::Event(_)) => {},
                None => break,
            },
            signal = interrupts.recv(), if !cancelled => {
                if signal.is_none() { continue; }
                handle.cancel();
                cancelled = true;
                eprintln!("Cancelling query {query_id}...");
            }
        }
    }
    let rows = output.finish()?;
    handle.finish().await?;
    let elapsed = started.elapsed().as_secs_f64();
    eprintln!("{rows} rows");
    if let Some(id) = &engine_id {
        eprintln!("Engine query ID: {id}");
    }
    if timing {
        eprintln!("Time: {elapsed:.3}s");
    }
    Ok(ExecutionOutcome {
        status: format!(
            "completed: {rows} rows, query {query_id}{}",
            engine_id.map_or_else(String::new, |id| format!(", engine query {id}"))
        ),
        session_properties,
    })
}

#[derive(Debug)]
struct ExecutionOutcome {
    status: String,
    session_properties: BTreeMap<String, String>,
}

fn statement_complete(sql: &str) -> bool {
    let mut single = false;
    let mut double = false;
    let mut escaped = false;
    let mut last = None;
    for character in sql.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' {
            escaped = true;
            continue;
        }
        if character == '\'' && !double {
            single = !single;
        } else if character == '"' && !single {
            double = !double;
        } else if !single && !double && !character.is_whitespace() {
            last = Some(character);
        }
    }
    !single && !double && last == Some(';')
}

fn safe_for_history(sql: &str) -> bool {
    let normalized = sql.to_ascii_lowercase();
    ![
        "password",
        "secret",
        "token",
        "credential",
        "create user",
        "alter user",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
}

fn property_format(snapshot: &SessionSnapshot) -> OutputFormat {
    snapshot
        .properties
        .get("output_format")
        .and_then(|value| value.parse().ok())
        .unwrap_or(OutputFormat::Table)
}
fn property_bool(snapshot: &SessionSnapshot, name: &str, fallback: bool) -> bool {
    snapshot
        .properties
        .get(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
fn option(snapshot: &SessionSnapshot, name: &str, fallback: usize) -> usize {
    snapshot
        .properties
        .get(name)
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
fn parse_on_off(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "on" | "true" => Some(true),
        "off" | "false" => Some(false),
        _ => None,
    }
}
fn save_history(
    editor: &mut Editor<SqlHelper, DefaultHistory>,
    path: &Path,
) -> Result<(), ReplError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    editor.save_history(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[must_use]
pub fn history_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("history")
}

#[cfg(test)]
mod tests {
    use super::*;
    use qcli_driver_demo::DemoAdapter;

    #[test]
    fn multiline_boundary_ignores_semicolons_inside_quotes() {
        assert!(!statement_complete("select ';'"));
        assert!(statement_complete("select ';';"));
        assert!(!statement_complete("select\n  1"));
    }

    #[test]
    fn sensitive_statements_are_not_history_candidates() {
        assert!(!safe_for_history("SET PASSWORD = 'hello';"));
        assert!(!safe_for_history("select token from credentials;"));
        assert!(safe_for_history("select count(*) from events;"));
    }

    #[test]
    fn sensitive_interactive_properties_are_recognized() {
        let properties = BTreeMap::new();
        assert!(sensitive_property("access_token", &properties));
        assert!(sensitive_property("private_key_path", &properties));
        assert!(!sensitive_property("decimal_places", &properties));
    }

    #[test]
    fn metadata_globs_support_star_and_question_mark() {
        assert!(glob_matches(Some("event*"), "event_summary"));
        assert!(glob_matches(Some("sf?"), "sf1"));
        assert!(!glob_matches(Some("sf?"), "sf100"));
    }

    #[test]
    fn completion_replaces_a_unique_metadata_prefix() {
        let candidates = vec!["events".into(), "event_summary".into()];
        let (start, matches) = completion_pairs(&candidates, "event_su", 8);
        assert_eq!(start, 0);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].replacement, "event_summary");
    }

    #[tokio::test]
    async fn injected_interrupt_cancels_the_active_query() {
        let adapters: Vec<Arc<dyn EngineAdapter>> = vec![Arc::new(DemoAdapter)];
        let service = QueryService::new(adapters, 8);
        let snapshot = SessionSnapshot {
            id: "injected-interrupt".into(),
            version: 1,
            target: "demo".into(),
            engine: "demo".into(),
            properties: BTreeMap::new(),
            overrides: BTreeMap::new(),
        };
        let (interrupt_tx, mut interrupts) = tokio::sync::mpsc::channel(1);
        interrupt_tx.send(()).await.unwrap();

        let error = execute(
            &service,
            snapshot,
            "wait-for-cancel".into(),
            OutputFormat::Table,
            false,
            &mut interrupts,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            ReplError::Core(CoreError::Driver(driver)) if driver.code == "cancelled"
        ));
    }
}
