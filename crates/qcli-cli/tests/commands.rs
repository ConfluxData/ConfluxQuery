use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(unix)]
static PTY_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(unix)]
fn lock_pty_tests() -> std::sync::MutexGuard<'static, ()> {
    PTY_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn config_file(contents: &str) -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("qcli-cli-{}-{id}.env", std::process::id()));
    fs::write(&path, contents).expect("write test configuration");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("secure test configuration");
    }
    path
}

fn qcli(path: &PathBuf, arguments: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_qcli"))
        .arg("--config")
        .arg(path)
        .args(arguments)
        .env("QCLI_TEST_TOKEN", "integration-secret")
        .output()
        .expect("run qcli")
}

fn qcli_with_stdin(path: &PathBuf, arguments: &[&str], input: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_qcli"))
        .arg("--config")
        .arg(path)
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run qcli");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn packaged_binary_reports_workspace_version() {
    let path = config_file("[demo]\nengine=demo\n");
    let output = qcli(&path, &["--version"]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("qcli {}\n", env!("CARGO_PKG_VERSION"))
    );
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_one_commands_resolve_and_redact_targets() {
    let path = config_file(
        "[default]\ndecimal_places=3\nstring_truncate=80\n\n[trino-dev]\nengine=trino\nurl=https://user:password@trino.test\ntoken=${QCLI_TEST_TOKEN}\ndecimal_places=10\n\n[databricks-dev]\nengine=databricks\nhost=https://dbc.test\nhttp_path=/sql/1.0/warehouses/abc\ntoken=${QCLI_TEST_TOKEN}\n\n[snowflake-prod]\nengine=snowflake\naccount=acme\npassword=${QCLI_TEST_TOKEN}\n",
    );

    let check = qcli(&path, &["config", "check"]);
    assert!(check.status.success());
    assert_eq!(
        String::from_utf8_lossy(&check.stdout),
        "Configuration is valid: 3 target(s)\n"
    );

    let list = qcli(&path, &["target", "list"]);
    assert!(list.status.success());
    let list = String::from_utf8_lossy(&list.stdout);
    assert!(list.contains("trino-dev                trino"));
    assert!(list.contains("databricks-dev           databricks"));
    assert!(list.contains("snowflake-prod           snowflake"));

    let show = qcli(&path, &["target", "show", "trino-dev"]);
    assert!(show.status.success());
    let show = String::from_utf8_lossy(&show.stdout);
    assert!(show.contains("decimal_places = 10"));
    assert!(show.contains("string_truncate = 80"));
    assert!(show.contains("token = <redacted>"));
    assert!(show.contains("url = <redacted>"));
    assert!(!show.contains("integration-secret"));
    assert!(!show.contains("user:password"));

    let _ = fs::remove_file(path);
}

#[test]
fn target_list_does_not_require_credentials() {
    let path = config_file(
        "[databricks-dev]\nengine=databricks\nhost=https://dbc.test\ntoken=${QCLI_MISSING_DATABRICKS_TOKEN}\n",
    );
    let output = Command::new(env!("CARGO_BIN_EXE_qcli"))
        .arg("--config")
        .arg(&path)
        .args(["target", "list"])
        .env_remove("QCLI_MISSING_DATABRICKS_TOKEN")
        .output()
        .expect("run qcli");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "databricks-dev           databricks\n"
    );
    assert!(output.stderr.is_empty());
    let _ = fs::remove_file(path);
}

#[test]
fn invalid_value_returns_configuration_exit_code_and_location() {
    let path = config_file("[trino]\nengine=trino\ndecimal_places=many\n");
    let output = qcli(&path, &["config", "check"]);
    assert_eq!(output.status.code(), Some(3));
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains(":3:"));
    assert!(error.contains("non-negative integer"));
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_two_executes_demo_query_through_core() {
    let path =
        config_file("[default]\ndecimal_places=3\nstring_truncate=12\n\n[demo]\nengine=demo\n");
    let output = qcli(
        &path,
        &["--target", "demo", "--command", "select * from sample"],
    );
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("123.457"));
    assert!(stdout.contains("beta-name-t…"));
    assert!(stdout.contains("NULL"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("2 rows"));
    assert!(stderr.contains("Query ID: qcli_"));
    assert!(stderr.contains("Engine query ID: demo-qcli_"));
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_three_machine_formats_are_exact_and_clean() {
    let path =
        config_file("[default]\ndecimal_places=1\nstring_truncate=4\n\n[demo]\nengine=demo\n");
    let cases = [
        (
            "csv",
            "id,name,amount\n1,alpha,123.456789\n2,beta-name-that-can-be-truncated,NULL\n",
        ),
        (
            "tsv",
            "id\tname\tamount\n1\talpha\t123.456789\n2\tbeta-name-that-can-be-truncated\tNULL\n",
        ),
        (
            "json",
            "[{\"id\":1,\"name\":\"alpha\",\"amount\":\"123.456789\"},{\"id\":2,\"name\":\"beta-name-that-can-be-truncated\",\"amount\":null}]\n",
        ),
        (
            "jsonl",
            "{\"id\":1,\"name\":\"alpha\",\"amount\":\"123.456789\"}\n{\"id\":2,\"name\":\"beta-name-that-can-be-truncated\",\"amount\":null}\n",
        ),
    ];
    for (format, expected) in cases {
        let output = qcli(
            &path,
            &[
                "--target",
                "demo",
                "--command",
                "select * from sample",
                "--format",
                format,
            ],
        );
        assert!(
            output.status.success(),
            "{format}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            expected,
            "{format}"
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains("2 rows"));
    }
    let vertical = qcli(
        &path,
        &[
            "--target",
            "demo",
            "--command",
            "select * from sample",
            "--format",
            "vertical",
        ],
    );
    let vertical = String::from_utf8(vertical.stdout).unwrap();
    assert!(vertical.contains("1. row"));
    assert!(vertical.contains("amount: 123.5"));
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_three_reads_query_file_and_stdin() {
    let path = config_file("[demo]\nengine=demo\n");
    let sql_path = std::env::temp_dir().join(format!("qcli-query-{}.sql", std::process::id()));
    fs::write(&sql_path, "generate 3").unwrap();
    let from_file = qcli(
        &path,
        &[
            "--target",
            "demo",
            "--file",
            sql_path.to_str().unwrap(),
            "--format",
            "csv",
        ],
    );
    assert!(from_file.status.success());
    assert_eq!(
        String::from_utf8_lossy(&from_file.stdout).lines().count(),
        4
    );
    let from_stdin = qcli_with_stdin(
        &path,
        &["--target", "demo", "--file", "-", "--format", "jsonl"],
        "generate 2\n",
    );
    assert!(from_stdin.status.success());
    assert_eq!(
        String::from_utf8_lossy(&from_stdin.stdout).lines().count(),
        2
    );
    let _ = fs::remove_file(sql_path);
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_three_has_stable_usage_and_query_exit_codes() {
    let path = config_file("[demo]\nengine=demo\n");
    assert_eq!(
        qcli(&path, &["--target", "demo", "--format", "xml"])
            .status
            .code(),
        Some(2)
    );
    assert_eq!(
        qcli(&path, &["--target", "demo", "--command", "fail"])
            .status
            .code(),
        Some(5)
    );
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_four_tests_targets_through_the_generic_adapter_registry() {
    let path = config_file("[demo]\nengine=demo\n");
    let output = qcli(&path, &["target", "test", "demo"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Target 'demo' is reachable (demo, 2 test row(s))"));
    assert!(stdout.contains("Engine query ID: demo-qcli_"));
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_four_reserves_exit_four_for_connection_failures() {
    let path = config_file(
        "[trino]\nengine=trino\nurl=http://127.0.0.1:9\nuser=test\nconnect_timeout=100ms\n",
    );
    let output = qcli(&path, &["target", "test", "trino"]);
    assert_eq!(output.status.code(), Some(4));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("driver error:"),
        "unexpected stderr: {stderr}"
    );
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_two_surfaces_structured_demo_failure() {
    let path = config_file("[demo]\nengine=demo\n");
    let output = qcli(&path, &["--target", "demo", "--command", "fail"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("demo_failure"));
    assert!(stderr.contains("requested deterministic failure"));
    let _ = fs::remove_file(path);
}

#[cfg(unix)]
#[test]
fn milestone_five_interactive_flow_runs_in_a_pseudo_terminal() {
    use expectrl::{Eof, Expect, Session};

    let _pty_guard = lock_pty_tests();
    let path = config_file(
        "[default]\ndecimal_places=3\nstring_truncate=12\ntiming=true\n\n[other]\nengine=demo\n\n[demo]\nengine=demo\n",
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_qcli"));
    command.arg("--config").arg(&path);
    let mut terminal = Session::spawn(command).expect("spawn qcli in a pseudo terminal");
    terminal.set_expect_timeout(Some(Duration::from_secs(30)));
    terminal.expect("Select a target:").unwrap();
    terminal.expect("target> ").unwrap();
    terminal.send_line("demo").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send_line("select *").unwrap();
    terminal.expect("-> ").unwrap();
    terminal.send_line("from sample;").unwrap();
    terminal.expect("2 rows").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send_line("\\set decimal_places 8").unwrap();
    terminal.expect("decimal_places = 8").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send_line("\\status").unwrap();
    terminal.expect("version=2").unwrap();
    terminal.expect("completed: 2 rows").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send_line("\\properties").unwrap();
    terminal.expect("decimal_places = 8").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send_line("\\q").unwrap();
    terminal.expect(Eof).unwrap();
    let _ = fs::remove_file(path);
}

#[cfg(unix)]
#[test]
fn milestone_five_ctrl_c_cancels_query_without_exiting_shell() {
    use expectrl::{ControlCode, Eof, Expect, Session};

    let _pty_guard = lock_pty_tests();
    let path = config_file("[demo]\nengine=demo\n");
    let mut command = Command::new(env!("CARGO_BIN_EXE_qcli"));
    command
        .arg("--config")
        .arg(&path)
        .arg("--target")
        .arg("demo");
    let mut terminal = Session::spawn(command).expect("spawn qcli in a pseudo terminal");
    terminal.set_expect_timeout(Some(Duration::from_secs(30)));
    terminal.expect("demo> ").unwrap();
    terminal.send_line("wait-for-cancel;").unwrap();
    terminal.expect("Query ID: qcli_").unwrap();
    terminal.send(ControlCode::EndOfText).unwrap();
    terminal.expect("Cancelling query").unwrap();
    terminal.expect("query was cancelled").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send(ControlCode::EndOfTransmission).unwrap();
    terminal.expect(Eof).unwrap();
    let _ = fs::remove_file(path);
}

#[test]
fn milestone_nine_reports_capabilities_without_connecting() {
    let path = config_file(
        "[snowflake-dev]\nengine=snowflake\nauth_type=password\naccount=acme\nuser=alice\npassword=${QCLI_TEST_TOKEN}\n",
    );
    let output = qcli(&path, &["target", "capabilities", "snowflake-dev"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("engine = snowflake"));
    assert!(stdout.contains("stream_results = yes"));
    assert!(stdout.contains("cancel_query = no"));
    assert!(!stdout.contains("integration-secret"));
    let _ = fs::remove_file(path);
}

#[cfg(unix)]
#[test]
fn milestone_six_navigation_and_atomic_target_switch_run_in_a_pseudo_terminal() {
    use expectrl::{Eof, Expect, Session};

    let _pty_guard = lock_pty_tests();
    let path = config_file("[demo]\nengine=demo\n\n[other]\nengine=demo\n");
    let mut command = Command::new(env!("CARGO_BIN_EXE_qcli"));
    command
        .arg("--config")
        .arg(&path)
        .arg("--target")
        .arg("demo");
    let mut terminal = Session::spawn(command).expect("spawn qcli in a pseudo terminal");
    terminal.set_expect_timeout(Some(Duration::from_secs(30)));
    terminal.expect("demo> ").unwrap();
    terminal.send_line("\\targets").unwrap();
    terminal.expect("* demo (demo)").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send_line("\\catalogs").unwrap();
    terminal.expect("demo").unwrap();
    terminal.expect("demo> ").unwrap();
    terminal.send_line("\\use-catalog demo").unwrap();
    terminal.expect("catalog = demo").unwrap();
    terminal.expect("demo[demo]> ").unwrap();
    terminal.send_line("\\schemas").unwrap();
    terminal.expect("public").unwrap();
    terminal.expect("demo[demo]> ").unwrap();
    terminal.send_line("\\use-schema public").unwrap();
    terminal.expect("schema = public").unwrap();
    terminal.expect("demo[demo.public]> ").unwrap();
    terminal.send_line("\\tables event*").unwrap();
    terminal.expect("events").unwrap();
    terminal.expect("event_summary").unwrap();
    terminal.expect("demo[demo.public]> ").unwrap();
    terminal.send_line("\\describe events").unwrap();
    terminal.expect("event_id").unwrap();
    terminal.expect("event_name").unwrap();
    terminal.expect("demo[demo.public]> ").unwrap();
    terminal.send_line("\\use missing").unwrap();
    terminal.expect("still using 'demo'").unwrap();
    terminal.expect("demo[demo.public]> ").unwrap();
    terminal.send_line("\\use other").unwrap();
    terminal.expect("Switched to 'other'").unwrap();
    terminal.expect("other> ").unwrap();
    terminal.send_line("\\status").unwrap();
    terminal.expect("target=other").unwrap();
    terminal.expect("version=4").unwrap();
    terminal.expect("other> ").unwrap();
    terminal.send_line("\\q").unwrap();
    terminal.expect(Eof).unwrap();
    let _ = fs::remove_file(path);
}
