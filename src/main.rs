mod cli;
use clap::Parser;
use cli::Cli;

mod config;
mod daemon;
mod error;
mod ipc;

fn main() {
    let cli = Cli::parse();

    if let Err(e) = cli::execute(cli) {
        cli::style::error(&format!("{}", e));
        std::process::exit(1);
    }
}
