use qcli_config::{Config, ConfigError, ResolvedTarget, default_config_path};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(3)
        }
    }
}

fn run(args: Vec<String>) -> Result<(), ConfigError> {
    let (config_path, command) = parse_global_args(args)?;
    match command.as_slice() {
        [group, action] if group == "config" && action == "path" => {
            println!("{}", config_path.display());
            Ok(())
        }
        [group, action] if group == "config" && action == "check" => {
            let config = Config::load(&config_path)?;
            println!(
                "Configuration is valid: {} target(s)",
                config.targets().count()
            );
            Ok(())
        }
        [group, action] if group == "config" && action == "show" => {
            let config = Config::load(&config_path)?;
            show_config(&config);
            Ok(())
        }
        [group, action] if group == "target" && action == "list" => {
            let config = Config::load(&config_path)?;
            for target in config.targets() {
                println!("{:<24} {}", target.name, target.engine);
            }
            Ok(())
        }
        [group, action, name] if group == "target" && action == "show" => {
            let config = Config::load(&config_path)?;
            let target = config.target(name).ok_or_else(|| ConfigError {
                path: config_path,
                line: None,
                message: format!("target '{name}' does not exist"),
            })?;
            show_target(target);
            Ok(())
        }
        [] => {
            print_help();
            Ok(())
        }
        _ => Err(ConfigError {
            path: config_path,
            line: None,
            message: "unknown command; run qcli without arguments for help".into(),
        }),
    }
}

fn parse_global_args(mut args: Vec<String>) -> Result<(PathBuf, Vec<String>), ConfigError> {
    let mut path = default_config_path()?;
    if args.first().is_some_and(|argument| argument == "--config") {
        if args.len() < 2 {
            return Err(ConfigError {
                path,
                line: None,
                message: "--config requires a path".into(),
            });
        }
        path = PathBuf::from(args.remove(1));
        args.remove(0);
    }
    Ok((path, args))
}

fn show_config(config: &Config) {
    println!("path = {}", config.path().display());
    if !config.defaults().is_empty() {
        println!("\n[default]");
        for (name, value) in config.defaults() {
            println!("{name} = {}", value.display_value());
        }
    }
    for target in config.targets() {
        println!("\n[{}]", target.name);
        for (name, value) in &target.properties {
            println!("{name} = {}", value.display_value());
        }
    }
}

fn show_target(target: &ResolvedTarget) {
    println!("target = {}", target.name);
    println!("engine = {}", target.engine);
    for (name, value) in &target.properties {
        if name != "engine" {
            println!("{name} = {}", value.display_value());
        }
    }
}

fn print_help() {
    println!("qcli — one query shell for cloud data platforms");
    println!();
    println!("Usage: qcli [--config PATH] <command>");
    println!();
    println!("Commands:");
    println!("  config path          Print the configuration path");
    println!("  config check         Validate configuration and targets");
    println!("  config show          Show resolved configuration with secrets redacted");
    println!("  target list          List configured targets");
    println!("  target show NAME     Show one resolved target with secrets redacted");
}
