mod commands;
pub(crate) mod style;

use clap::{Parser, Subcommand};

const AFTER_HELP: &str = "\
EXAMPLES:
    loghaven init                Initialize configuration
    loghaven run                 Start the agent
    loghaven status              Check agent status
    loghaven stop                Stop the agent

LEARN MORE:
    Website:       https://loghaven.dev
    Documentation: https://loghaven.dev/docs
";

// TODO: add discord group later on to the list of accessible dir

#[derive(Parser)]
#[command(name = "loghaven")]
#[command(version)]
#[command(about = "Local-first observability runtime for on-chain and off-chain systems")]
#[command(long_about = None)]
#[command(after_help = AFTER_HELP)]
#[command(disable_help_subcommand = true)] 
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(visible_alias = "i")]
    Init {
        #[arg(long)]
        force: bool,

        #[arg(long, value_name = "TYPE")]
        storage: Option<String>,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(visible_alias = "r")]
    Run {
        #[arg(long)]
        foreground: bool,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(visible_alias = "s")]
    Status {
        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(visible_alias = "st")]
    Stop {
        #[arg(long)]
        force: bool,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },
}

pub fn execute(cli: Cli) -> crate::error::Result<()> {
    match cli.command {
         Some(Commands::Init { force, storage, profile }) => {
            commands::init(force, storage, profile.as_deref())
        }
        Some(Commands::Run { foreground, profile }) => {
            commands::run(foreground, profile.as_deref())
        }
        Some(Commands::Status { profile }) => {
            commands::status(profile.as_deref())
        }
        Some(Commands::Stop { force, profile }) => {
            commands::stop(force, profile.as_deref())
        }
        None => {
            style::print_banner();
            println!("Run 'loghaven --help' for usage information\n");
            Ok(())
        }
    }
}
