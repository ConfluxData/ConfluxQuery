//! Sectioned configuration parsing, validation, resolution, and redaction.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

const DEFAULT_SECTION: &str = "default";

/// An error with source location where available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub path: PathBuf,
    pub line: Option<usize>,
    pub message: String,
}

impl ConfigError {
    fn at(path: &Path, line: usize, message: impl Into<String>) -> Self {
        Self {
            path: path.to_path_buf(),
            line: Some(line),
            message: message.into(),
        }
    }

    fn file(path: &Path, message: impl Into<String>) -> Self {
        Self {
            path: path.to_path_buf(),
            line: None,
            message: message.into(),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            Some(line) => write!(f, "{}:{line}: {}", self.path.display(), self.message),
            None => write!(f, "{}: {}", self.path.display(), self.message),
        }
    }
}

impl std::error::Error for ConfigError {}

/// A value which remembers whether it is sensitive.
#[derive(Clone, PartialEq, Eq)]
pub struct ConfigValue {
    value: String,
    secret: bool,
}

impl ConfigValue {
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.value
    }

    #[must_use]
    pub const fn is_secret(&self) -> bool {
        self.secret
    }

    #[must_use]
    pub fn display_value(&self) -> &str {
        if self.secret {
            "<redacted>"
        } else {
            &self.value
        }
    }
}

impl fmt::Debug for ConfigValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigValue")
            .field("value", &self.display_value())
            .field("secret", &self.secret)
            .finish()
    }
}

/// A fully resolved target after applying `[default]` values.
#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    pub name: String,
    pub engine: String,
    pub properties: BTreeMap<String, ConfigValue>,
}

/// A validated qcli configuration.
#[derive(Debug, Clone)]
pub struct Config {
    path: PathBuf,
    defaults: BTreeMap<String, ConfigValue>,
    targets: BTreeMap<String, ResolvedTarget>,
}

impl Config {
    /// Load the standard qcli configuration path.
    ///
    /// # Errors
    ///
    /// Returns an error when the home directory cannot be located or the
    /// configuration cannot be read, parsed, validated, or resolved.
    pub fn load_default() -> Result<Self, ConfigError> {
        let path = default_config_path()?;
        Self::load(&path)
    }

    /// Load, parse, validate, and resolve a configuration file.
    ///
    /// # Errors
    ///
    /// Returns a source-located error for I/O, syntax, permission, property,
    /// environment-substitution, or target validation failures.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let source = fs::read_to_string(path).map_err(|error| {
            ConfigError::file(path, format!("cannot read configuration: {error}"))
        })?;
        let parsed = parse(path, &source)?;
        validate_permissions(path, &parsed)?;
        resolve(path, parsed)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn defaults(&self) -> &BTreeMap<String, ConfigValue> {
        &self.defaults
    }

    pub fn targets(&self) -> impl Iterator<Item = &ResolvedTarget> {
        self.targets.values()
    }

    #[must_use]
    pub fn target(&self, name: &str) -> Option<&ResolvedTarget> {
        self.targets.get(name)
    }
}

#[derive(Debug, Clone)]
struct ParsedValue {
    value: String,
    line: usize,
}

type ParsedSections = BTreeMap<String, BTreeMap<String, ParsedValue>>;

/// Return `~/.qcli/.env` without depending on a platform directory crate.
///
/// # Errors
///
/// Returns an error when neither `HOME` nor `USERPROFILE` is available.
pub fn default_config_path() -> Result<PathBuf, ConfigError> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .ok_or_else(|| ConfigError {
            path: PathBuf::from("~/.qcli/.env"),
            line: None,
            message: "cannot determine home directory".into(),
        })?;
    Ok(PathBuf::from(home).join(".qcli").join(".env"))
}

fn parse(path: &Path, source: &str) -> Result<ParsedSections, ConfigError> {
    let mut sections = BTreeMap::<String, BTreeMap<String, ParsedValue>>::new();
    let mut current: Option<String> = None;

    for (index, raw_line) in source.lines().enumerate() {
        let line_no = index + 1;
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        if line.starts_with('[') {
            if !line.ends_with(']') {
                return Err(ConfigError::at(
                    path,
                    line_no,
                    "section header must end with ']'",
                ));
            }
            let name = line[1..line.len() - 1].trim();
            validate_section_name(path, line_no, name)?;
            if sections.contains_key(name) {
                return Err(ConfigError::at(
                    path,
                    line_no,
                    format!("duplicate section [{name}]"),
                ));
            }
            sections.insert(name.to_owned(), BTreeMap::new());
            current = Some(name.to_owned());
            continue;
        }

        let section = current.as_ref().ok_or_else(|| {
            ConfigError::at(path, line_no, "property must appear inside a section")
        })?;
        let (raw_key, raw_value) = line
            .split_once('=')
            .ok_or_else(|| ConfigError::at(path, line_no, "property must use 'name = value'"))?;
        let key = raw_key.trim();
        validate_property_name(path, line_no, key)?;
        let value = parse_value(path, line_no, raw_value.trim())?;
        let properties = sections.get_mut(section).expect("current section exists");
        if properties.contains_key(key) {
            return Err(ConfigError::at(
                path,
                line_no,
                format!("duplicate property '{key}' in [{section}]"),
            ));
        }
        properties.insert(
            key.to_owned(),
            ParsedValue {
                value,
                line: line_no,
            },
        );
    }

    if sections.is_empty() {
        return Err(ConfigError::file(
            path,
            "configuration contains no sections",
        ));
    }
    Ok(sections)
}

fn strip_comment(line: &str) -> &str {
    let mut quote = None;
    let mut escaped = false;
    for (index, character) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quote == Some('"') {
            escaped = true;
            continue;
        }
        if matches!(character, '\'' | '"') {
            if quote == Some(character) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(character);
            }
        } else if character == '#' && quote.is_none() {
            return &line[..index];
        }
    }
    line
}

fn parse_value(path: &Path, line: usize, raw: &str) -> Result<String, ConfigError> {
    if raw.is_empty() {
        return Ok(String::new());
    }
    let value = if raw.starts_with('"') || raw.starts_with('\'') {
        let quote = raw.chars().next().expect("non-empty");
        if raw.len() < 2 || !raw.ends_with(quote) {
            return Err(ConfigError::at(path, line, "unterminated quoted value"));
        }
        &raw[1..raw.len() - 1]
    } else {
        raw
    };
    expand_environment(path, line, value)
}

fn expand_environment(path: &Path, line: usize, value: &str) -> Result<String, ConfigError> {
    let mut output = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| ConfigError::at(path, line, "unterminated environment substitution"))?;
        let name = &after[..end];
        if name.is_empty() || !name.chars().all(|c| c == '_' || c.is_ascii_alphanumeric()) {
            return Err(ConfigError::at(
                path,
                line,
                format!("invalid environment variable name '{name}'"),
            ));
        }
        let resolved = env::var(name).map_err(|_| {
            ConfigError::at(
                path,
                line,
                format!("required environment variable '{name}' is not set"),
            )
        })?;
        output.push_str(&resolved);
        rest = &after[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}

fn validate_section_name(path: &Path, line: usize, name: &str) -> Result<(), ConfigError> {
    if name.is_empty() {
        return Err(ConfigError::at(path, line, "section name cannot be empty"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(ConfigError::at(
            path,
            line,
            format!("invalid section name '{name}'"),
        ));
    }
    Ok(())
}

fn validate_property_name(path: &Path, line: usize, name: &str) -> Result<(), ConfigError> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(ConfigError::at(
            path,
            line,
            format!("invalid property name '{name}'"),
        ));
    }
    Ok(())
}

fn resolve(path: &Path, mut parsed: ParsedSections) -> Result<Config, ConfigError> {
    let raw_defaults = parsed.remove(DEFAULT_SECTION).unwrap_or_default();
    let defaults = validate_properties(path, DEFAULT_SECTION, &raw_defaults, None)?;
    let mut targets = BTreeMap::new();

    for (name, raw_properties) in parsed {
        if raw_properties.is_empty() {
            return Err(ConfigError::file(
                path,
                format!("target section [{name}] cannot be empty"),
            ));
        }
        let engine_value = raw_properties.get("engine").ok_or_else(|| {
            ConfigError::file(path, format!("target [{name}] must define 'engine'"))
        })?;
        let engine = engine_value.value.to_ascii_lowercase();
        validate_engine(path, engine_value.line, &engine)?;
        let target_values = validate_properties(path, &name, &raw_properties, Some(&engine))?;
        let mut properties = defaults.clone();
        properties.extend(target_values);
        targets.insert(
            name.clone(),
            ResolvedTarget {
                name,
                engine,
                properties,
            },
        );
    }

    if targets.is_empty() {
        return Err(ConfigError::file(path, "configuration defines no targets"));
    }
    Ok(Config {
        path: path.to_path_buf(),
        defaults,
        targets,
    })
}

fn validate_engine(path: &Path, line: usize, engine: &str) -> Result<(), ConfigError> {
    if matches!(engine, "trino" | "databricks" | "snowflake" | "demo") {
        Ok(())
    } else {
        Err(ConfigError::at(
            path,
            line,
            format!("unsupported engine '{engine}'"),
        ))
    }
}

fn validate_properties(
    path: &Path,
    section: &str,
    properties: &BTreeMap<String, ParsedValue>,
    engine: Option<&str>,
) -> Result<BTreeMap<String, ConfigValue>, ConfigError> {
    let allowed = allowed_properties(engine);
    let mut result = BTreeMap::new();
    for (name, parsed) in properties {
        if !allowed.contains(name.as_str()) && !name.starts_with("session.") {
            let suggestion = closest(name, &allowed).map_or_else(String::new, |candidate| {
                format!("; did you mean '{candidate}'?")
            });
            return Err(ConfigError::at(
                path,
                parsed.line,
                format!("unknown property '{name}' in [{section}]{suggestion}"),
            ));
        }
        validate_typed_value(path, parsed.line, name, &parsed.value)?;
        result.insert(
            name.clone(),
            ConfigValue {
                value: parsed.value.clone(),
                secret: is_secret(name),
            },
        );
    }
    Ok(result)
}

fn allowed_properties(engine: Option<&str>) -> BTreeSet<&'static str> {
    let common = [
        "engine",
        "output_format",
        "decimal_places",
        "decimal_rounding",
        "strip_trailing_decimal_zeros",
        "string_truncate",
        "binary_format",
        "null_value",
        "table_style",
        "color",
        "expanded",
        "headers",
        "row_numbers",
        "max_column_width",
        "timestamp_format",
        "timezone",
        "timing",
        "query_timeout",
        "connect_timeout",
        "fetch_size",
        "page_size",
        "max_display_rows",
        "progress",
        "retry",
        "history",
        "history_limit",
        "syntax_highlight",
        "completion",
        "pager",
        "editor",
        "prompt",
        "confirm_target_switch",
        "tls_verify",
        "show_query_id",
        "log_level",
    ];
    let mut result = common.into_iter().collect::<BTreeSet<_>>();
    let extras: &[&str] = match engine {
        Some("trino") => &[
            "url",
            "user",
            "password",
            "token",
            "catalog",
            "schema",
            "source",
            "client_tags",
        ],
        Some("databricks") => &[
            "auth_type",
            "host",
            "http_path",
            "token",
            "catalog",
            "schema",
            "user",
        ],
        Some("snowflake") => &[
            "auth_type",
            "account",
            "user",
            "password",
            "private_key",
            "warehouse",
            "database",
            "schema",
            "role",
        ],
        _ => &[],
    };
    result.extend(extras.iter().copied());
    result
}

fn validate_typed_value(
    path: &Path,
    line: usize,
    name: &str,
    value: &str,
) -> Result<(), ConfigError> {
    let boolean = [
        "strip_trailing_decimal_zeros",
        "headers",
        "row_numbers",
        "timing",
        "history",
        "syntax_highlight",
        "completion",
        "confirm_target_switch",
        "tls_verify",
        "show_query_id",
    ];
    let unsigned = [
        "decimal_places",
        "string_truncate",
        "max_column_width",
        "fetch_size",
        "page_size",
        "max_display_rows",
        "history_limit",
    ];
    let duration = ["query_timeout", "connect_timeout"];
    if boolean.contains(&name) && !matches!(value, "true" | "false") {
        return Err(ConfigError::at(
            path,
            line,
            format!("property '{name}' requires true or false"),
        ));
    }
    if unsigned.contains(&name) && value.parse::<u64>().is_err() {
        return Err(ConfigError::at(
            path,
            line,
            format!("property '{name}' requires a non-negative integer"),
        ));
    }
    if duration.contains(&name) && !valid_duration(value) {
        return Err(ConfigError::at(
            path,
            line,
            format!("property '{name}' requires a duration such as 250ms, 15s, 5m, or 2h"),
        ));
    }
    Ok(())
}

fn valid_duration(value: &str) -> bool {
    ["ms", "s", "m", "h"].into_iter().any(|suffix| {
        value
            .strip_suffix(suffix)
            .is_some_and(|number| !number.is_empty() && number.chars().all(|c| c.is_ascii_digit()))
    })
}

fn is_secret(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == "url"
        || lower.contains("password")
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("private_key")
}

fn closest<'a>(name: &str, allowed: &'a BTreeSet<&str>) -> Option<&'a str> {
    allowed
        .iter()
        .copied()
        .min_by_key(|candidate| edit_distance(name, candidate))
        .filter(|candidate| edit_distance(name, candidate) <= 3)
}

fn edit_distance(left: &str, right: &str) -> usize {
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    for (i, l) in left.bytes().enumerate() {
        let mut current = vec![i + 1];
        for (j, r) in right.bytes().enumerate() {
            current.push(
                (current[j] + 1)
                    .min(previous[j + 1] + 1)
                    .min(previous[j] + usize::from(l != r)),
            );
        }
        previous = current;
    }
    previous[right.len()]
}

#[cfg(unix)]
fn validate_permissions(path: &Path, parsed: &ParsedSections) -> Result<(), ConfigError> {
    use std::os::unix::fs::PermissionsExt;
    let contains_secrets = parsed
        .values()
        .flat_map(|values| values.keys())
        .any(|key| is_secret(key));
    if contains_secrets {
        let mode = fs::metadata(path)
            .map_err(|error| {
                ConfigError::file(path, format!("cannot inspect permissions: {error}"))
            })?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(ConfigError::file(
                path,
                format!(
                    "configuration contains credentials but permissions are {:o}; run: chmod 600 {}",
                    mode & 0o777,
                    path.display()
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_permissions(_path: &Path, _parsed: &ParsedSections) -> Result<(), ConfigError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    fn config_file(contents: &str) -> PathBuf {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!("qcli-config-{}-{id}.env", std::process::id()));
        let mut file = fs::File::create(&path).expect("create test config");
        file.write_all(contents.as_bytes())
            .expect("write test config");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("secure permissions");
        }
        path
    }

    #[test]
    fn discovers_targets_and_applies_overrides() {
        let path = config_file(
            "[default]\ndecimal_places = 3\nstring_truncate = 80\n\n[trino]\nengine = trino\nurl = https://example.test\ndecimal_places = 10\n\n[snow]\nengine = snowflake\naccount = acme\n",
        );
        let config = Config::load(&path).expect("valid config");
        let names = config
            .targets()
            .map(|target| target.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["snow", "trino"]);
        assert_eq!(
            config.target("trino").unwrap().properties["decimal_places"].expose(),
            "10"
        );
        assert_eq!(
            config.target("trino").unwrap().properties["string_truncate"].expose(),
            "80"
        );
        assert_eq!(
            config.target("snow").unwrap().properties["decimal_places"].expose(),
            "3"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn redacts_secret_in_debug_and_display() {
        let value = ConfigValue {
            value: "very-secret".into(),
            secret: true,
        };
        assert_eq!(value.display_value(), "<redacted>");
        assert!(!format!("{value:?}").contains("very-secret"));
    }

    #[test]
    fn validates_types_with_source_location() {
        let path = config_file("[trino]\nengine = trino\ndecimal_places = many\n");
        let error = Config::load(&path).unwrap_err();
        assert_eq!(error.line, Some(3));
        assert!(error.message.contains("non-negative integer"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn suggests_unknown_property() {
        let path = config_file("[trino]\nengine = trino\ndecimal_place = 3\n");
        let error = Config::load(&path).unwrap_err();
        assert!(error.message.contains("did you mean 'decimal_places'"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn comments_inside_quotes_are_values() {
        let path = config_file("[trino]\nengine=trino\nprompt = \"value # retained\" # removed\n");
        let config = Config::load(&path).unwrap();
        assert_eq!(
            config.target("trino").unwrap().properties["prompt"].expose(),
            "value # retained"
        );
        let _ = fs::remove_file(path);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_broad_permissions_when_secrets_exist() {
        use std::os::unix::fs::PermissionsExt;
        let path = config_file("[trino]\nengine=trino\npassword=secret\n");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let error = Config::load(&path).unwrap_err();
        assert!(error.message.contains("chmod 600"));
        let _ = fs::remove_file(path);
    }
}
