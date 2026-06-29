use crate::banner;
use crate::process;
use chronicle_unit::option::unit_options::UnitOptions;
use chronicle_unit::unit::unit::Unit;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const DEFAULT_PID_FILE: &str = "chronicle-unit.pid";

#[derive(clap::Subcommand)]
pub enum UnitAction {
    Start {
        #[arg(short, long)]
        config: Option<String>,

        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },

    Stop {
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },
}

pub async fn run(action: UnitAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        UnitAction::Start { config, pid_file } => {
            let options: UnitOptions = match config {
                Some(path) => {
                    let contents = std::fs::read_to_string(&path)
                        .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;
                    toml::from_str(&contents)
                        .map_err(|e| format!("failed to parse config file '{}': {}", path, e))?
                }
                None => UnitOptions::default(),
            };

            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new(&options.log.level)),
                )
                .with_ansi(std::io::stderr().is_terminal())
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .compact()
                .init();

            banner::print_banner("Unit");

            process::write_pid_file(&pid_file)?;

            let catalog = loop {
                let opts = options.catalog.clone();
                let task = tokio::spawn(async move { catalog::build_catalog(&opts).await });
                tokio::select! {
                    result = task => match result {
                        Ok(Ok(c)) => break c,
                        Ok(Err(e)) => {
                            warn!(error = %e, "catalog connection failed, retrying in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                        Err(e) => {
                            warn!(error = %e, "catalog task panicked, retrying in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {
                        warn!("catalog connection timed out after 15s, retrying");
                    }
                }
            };
            let unit = Unit::new(options, Arc::new(catalog)).await?;

            process::wait_for_shutdown().await;

            info!("received shutdown signal");
            unit.stop().await;

            process::remove_pid_file(&pid_file);
        }

        UnitAction::Stop { pid_file } => {
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!("sent stop signal to unit (pid {})", pid);
        }
    }

    Ok(())
}
