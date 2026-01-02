mod commands;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "loghaven")]
#[command(version)]
#[command(about = "Local-first observability runtime", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Init,

    Run,

    Status,

    Stop,
}


pub fn execute(cli: Cli) -> Result<(), String> {
      match cli.command {
        Commands::Init => commands::init(),
        Commands::Run => commands::run(),
        Commands::Status => commands::status(),
        Commands::Stop => commands::stop(),
    }
}