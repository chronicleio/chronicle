use clap::Parser;
use lyra_cli::sql::SqlArgs;
use lyra_cli::unit::UnitAction;
use lyra_cli::verify::VerifyArgs;

#[derive(Parser)]
#[command(name = "lyra", about = "Lyra distributed streaming engine CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    Unit {
        #[command(subcommand)]
        action: UnitAction,
    },
    Sql(SqlArgs),
    Verify(VerifyArgs),
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { action } => lyra_cli::unit::run(action).await?,
        Commands::Sql(args) => lyra_cli::sql::run(args).await?,
        Commands::Verify(args) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
                .with_target(false)
                .with_thread_names(false)
                .compact()
                .init();
            lyra_cli::verify::run(args).await?;
        }
    }

    Ok(())
}
