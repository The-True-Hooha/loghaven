use clap::Parser;

use crate::cli::Cli;

mod cli;
fn main() {
    let cli = Cli::parse();

    if let Err(e) = cli::execute(cli) {
        eprintln!("Error: {}", e);

        std::process::exit(1);
    }
}
