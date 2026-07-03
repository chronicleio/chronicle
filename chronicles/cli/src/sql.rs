use catalog::{CatalogOptions, build_catalog};
use chronicle_lens::{Lens, LensOutput};
use clap::Args;
use libxunit::RowBatch;
use serde::Deserialize;
use std::io::{self, Write};
use std::path::Path;
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "/etc/chronicle/chronicled.toml";

#[derive(Debug, Args)]
pub struct SqlArgs {
    #[arg(short, long)]
    pub config: Option<String>,

    #[arg(short = 'e', long)]
    pub execute: Option<String>,
}

pub async fn run(args: SqlArgs) -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config(args.config.as_deref())?;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log.level)),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact()
        .try_init();

    let catalog = build_catalog(&config.catalog).await?;
    let lens = Lens::new(catalog);

    if let Some(statement) = args.execute {
        execute_and_print(&lens, &statement).await?;
        return Ok(());
    }

    repl(&lens).await
}

async fn repl(lens: &Lens) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        print!("chronicle> ");
        io::stdout().flush()?;

        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        let statement = line.trim();
        if statement.is_empty() {
            continue;
        }
        if statement.eq_ignore_ascii_case("quit")
            || statement.eq_ignore_ascii_case("exit")
            || statement.eq_ignore_ascii_case("\\q")
        {
            break;
        }

        if let Err(error) = execute_and_print(lens, statement).await {
            eprintln!("error: {error}");
        }
    }

    Ok(())
}

async fn execute_and_print(lens: &Lens, statement: &str) -> Result<(), Box<dyn std::error::Error>> {
    match lens.execute(statement).await? {
        LensOutput::Empty => println!("OK"),
        LensOutput::Message(message) => println!("{message}"),
        LensOutput::Datasets(datasets) => {
            if datasets.is_empty() {
                println!("no datasets");
            } else {
                for dataset in datasets {
                    println!(
                        "{}\tv{}\t{} fields",
                        dataset.value.name,
                        dataset.version,
                        dataset.value.schema.fields.len()
                    );
                }
            }
        }
        LensOutput::Action(action) => {
            println!(
                "{}\t{:?}\t{:?}\t{}",
                action.value.id,
                action.value.status,
                action.value.request.kind,
                action.value.request.dataset
            );
        }
        LensOutput::Rows(batches) => print_rows(batches),
    }
    Ok(())
}

fn print_rows(batches: Vec<RowBatch>) {
    for batch in batches {
        for row in batch.rows {
            println!(
                "{}\t{}\t{}",
                row.offset,
                batch.schema_id,
                String::from_utf8_lossy(&row.payload)
            );
        }
    }
}

#[derive(Debug, Deserialize)]
struct SqlConfig {
    #[serde(default)]
    catalog: CatalogOptions,
    #[serde(default)]
    log: LogConfig,
}

#[derive(Debug, Deserialize)]
struct LogConfig {
    #[serde(default = "default_log_level")]
    level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn load_config(path: Option<&str>) -> Result<SqlConfig, Box<dyn std::error::Error>> {
    match resolve_config_path(path) {
        Some(path) => read_config(&path),
        None => Ok(SqlConfig {
            catalog: CatalogOptions::default(),
            log: LogConfig::default(),
        }),
    }
}

fn resolve_config_path(path: Option<&str>) -> Option<String> {
    if let Some(path) = path {
        return Some(path.to_string());
    }
    if let Ok(path) = std::env::var("CHRONICLE_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(path);
    }
    if Path::new(DEFAULT_CONFIG_PATH).exists() {
        return Some(DEFAULT_CONFIG_PATH.to_string());
    }
    None
}

fn read_config(path: &str) -> Result<SqlConfig, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file '{}': {}", path, error))?;
    toml::from_str(&contents)
        .map_err(|error| format!("failed to parse config file '{}': {}", path, error).into())
}
