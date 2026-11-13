#![allow(clippy::multiple_crate_versions)]

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use s3_sidecar::cli::{Cli, Command, ConfigFlags, read_stdin_secret};
use s3_sidecar::config::loader::{LoadOptions, LoadedConfig, load};
use s3_sidecar::config::show_table;
use s3_sidecar::observability;
use s3_sidecar::server;

#[tokio::main]
async fn main() -> ExitCode {
    observability::init();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("ERROR: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Serve(args) => serve(&args.config).await,
        Command::Check(args) => check(&args.config, args.show_config),
    }
}

async fn serve(flags: &ConfigFlags) -> Result<(), String> {
    let loaded = load_config(flags)?;
    server::log_config(&loaded.config, loaded.config_path.as_deref());
    server::serve(loaded.config, &loaded.warnings)
        .await
        .map_err(|error| error.to_string())
}

fn check(flags: &ConfigFlags, show: bool) -> Result<(), String> {
    let loaded = load_config(flags)?;
    if show {
        print!("{}", show_table(&loaded.config, &loaded.sources));
    } else {
        println!("configuration is valid");
    }
    for warning in &loaded.warnings {
        eprintln!("WARNING: {warning}");
    }
    Ok(())
}

fn load_config(flags: &ConfigFlags) -> Result<LoadedConfig, String> {
    let target = flags.stdin_target().map_err(|error| error.to_string())?;
    let stdin_secret = if target.is_some() {
        Some(read_stdin_secret().map_err(|error| error.to_string())?)
    } else {
        None
    };
    let layer = flags
        .to_layer(stdin_secret.as_ref())
        .map_err(|error| error.to_string())?;
    let cwd = std::env::current_dir()
        .map_err(|error| format!("cannot read the current directory: {error}"))?;
    let options = LoadOptions {
        config_flag: flags.config.clone(),
        env: std::env::vars().collect(),
        flags: layer,
        cwd,
        default_paths: vec![
            PathBuf::from("sidecar.toml"),
            PathBuf::from("/etc/s3-sidecar/config.toml"),
        ],
    };
    load(&options).map_err(|error| error.to_string())
}
