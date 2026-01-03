mod cli;
use cli::Cli;
use clap::Parser;

mod error;
mod config;


fn main() {
    let cli = Cli::parse();

    if let Err(e) = cli::execute(cli) {
        cli::style::error(&format!("{}", e));
        std::process::exit(1);
    }
}
