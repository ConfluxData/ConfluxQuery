//! Interactive qcli terminal built on the frontend-neutral core services.

use qcli_config::{Config, ResolvedTarget};
use qcli_core::{CoreError, QueryItem, QueryService, SessionManager, SessionSnapshot};
use qcli_driver_api::{EngineAdapter, QueryEvent};
use qcli_output::{DisplayOptions, OutputError, OutputFormat, StreamOutput};
use rustyline::completion::{Completer, Pair};
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

struct SqlHelper;

impl Completer for SqlHelper {
    type Candidate = Pair;
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
    let mut editor = Editor::<SqlHelper, DefaultHistory>::with_config(line_config)?;
    editor.set_helper(Some(SqlHelper));
    let (interrupt_tx, mut interrupts) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() || interrupt_tx.send(()).await.is_err() {
                break;
            }
        }
    });
    tokio::task::yield_now().await;
    let target = choose_target(config, requested_target, &mut editor)?;
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
    let display_properties = target.properties.clone();
    let sessions = SessionManager::default();
    let mut snapshot = sessions.create(target);
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
            format!("{}> ", snapshot.target)
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
                        &sessions,
                        &mut snapshot,
                        &mut format,
                        &mut timing,
                        &mut buffer,
                        &mut last_status,
                        &display_properties,
                        &mut overrides,
                    )? {
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
                        Ok(status) => last_status = status,
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
        if let Ok(index) = answer.trim().parse::<usize>()
            && let Some(target) = index.checked_sub(1).and_then(|index| targets.get(index))
        {
            return Ok((*target).clone());
        }
        if let Some(target) = config.target(answer.trim()) {
            return Ok(target.clone());
        }
        eprintln!("Choose a target number or exact name.");
    }
}

#[allow(clippy::too_many_arguments)]
fn meta_command(
    line: &str,
    sessions: &SessionManager,
    snapshot: &mut SessionSnapshot,
    format: &mut OutputFormat,
    timing: &mut bool,
    buffer: &mut String,
    status: &mut String,
    properties: &BTreeMap<String, qcli_config::ConfigValue>,
    overrides: &mut BTreeMap<String, String>,
) -> Result<bool, ReplError> {
    let parts = line.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        ["\\q" | "\\quit"] => return Ok(false),
        ["\\help"] => println!(
            "\\q quit | \\status | \\set NAME VALUE | \\format FORMAT | \\timing [on|off] | \\properties | \\p print buffer | \\r clear buffer"
        ),
        ["\\status"] => println!(
            "target={} engine={} session={} version={} status={}",
            snapshot.target, snapshot.engine, snapshot.id, snapshot.version, status
        ),
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
            for (name, value) in properties {
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

async fn execute(
    service: &QueryService,
    snapshot: SessionSnapshot,
    sql: String,
    format: OutputFormat,
    timing: bool,
    interrupts: &mut tokio::sync::mpsc::Receiver<()>,
) -> Result<String, ReplError> {
    #[cfg(unix)]
    let (interrupt_flag, _signal_registration) = {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let id = signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&flag))?;
        (flag, SignalRegistration(id))
    };
    #[cfg(not(unix))]
    let interrupt_flag = ();
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
    let mut cancelled = false;
    loop {
        tokio::select! {
            item = handle.next_item() => match item {
                Some(QueryItem::Batch(batch)) => output.write_batch(&batch)?,
                Some(QueryItem::Event(QueryEvent::EngineQueryId(id))) => engine_id = Some(id),
                Some(QueryItem::Event(_)) => {},
                None => break,
            },
            signal = interrupts.recv(), if !cancelled => {
                if signal.is_none() { continue; }
                handle.cancel();
                cancelled = true;
                eprintln!("Cancelling query {query_id}...");
            }
            () = wait_for_interrupt(&interrupt_flag), if !cancelled => {
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
    Ok(format!(
        "completed: {rows} rows, query {query_id}{}",
        engine_id.map_or_else(String::new, |id| format!(", engine query {id}"))
    ))
}

#[cfg(unix)]
struct SignalRegistration(signal_hook::SigId);

#[cfg(unix)]
impl Drop for SignalRegistration {
    fn drop(&mut self) {
        signal_hook::low_level::unregister(self.0);
    }
}

#[cfg(unix)]
async fn wait_for_interrupt(flag: &std::sync::atomic::AtomicBool) {
    while !flag.load(std::sync::atomic::Ordering::Relaxed) {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[cfg(not(unix))]
async fn wait_for_interrupt(_flag: &()) {
    std::future::pending().await
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
}
