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
pub enum WatchCommands {
    #[command(about = "Poll Ethereum contract logs (settings: [chains.ethereum] in config)")]
    Ethereum {
        #[arg(long, help = "Process the available blocks once and exit")]
        once: bool,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(visible_alias = "i")]
    Init {
        #[arg(long)]
        force: bool,

        #[arg(long, value_name = "TYPE")]
        storage: Option<String>,

        #[arg(long, value_name = "PATH")]
        storage_path: Option<String>,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(about = "Get or set configuration values")]
    Config {
        #[arg(value_name = "KEY")]
        key: String,

        #[arg(value_name = "VALUE")]
        value: Option<String>,

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

    #[command(about = "Query logs from the daemon")]
    Logs {
        #[arg(value_name = "APP")]
        app: String,

        #[arg(long)]
        level: Option<String>,

        #[arg(long)]
        source: Option<String>,

        #[arg(long, value_name = "MS")]
        from: Option<i64>,

        #[arg(long, value_name = "MS")]
        to: Option<i64>,

        #[arg(long)]
        text: Option<String>,

        #[arg(long, default_value = "100")]
        limit: usize,

        #[arg(
            long,
            value_name = "TOKEN",
            help = "Session token_id; minted automatically from the local private key when omitted"
        )]
        token: Option<String>,

        #[arg(
            long,
            help = "Keep running and print new records as they become queryable"
        )]
        follow: bool,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(about = "Show the records that carry one transaction hash, oldest first")]
    Trace {
        #[arg(value_name = "TX_HASH")]
        tx_hash: String,

        #[arg(
            long = "app",
            value_name = "APP",
            required = true,
            help = "App to search; repeat to correlate several apps"
        )]
        apps: Vec<String>,

        #[arg(long, value_name = "MS")]
        from: Option<i64>,

        #[arg(long, value_name = "MS")]
        to: Option<i64>,

        #[arg(
            long,
            default_value = "1000",
            help = "Newest N records to fetch per app and to print overall"
        )]
        limit: usize,

        #[arg(
            long,
            value_name = "TOKEN",
            help = "Session token_id; only valid for one app, so omit it with several --app"
        )]
        token: Option<String>,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(about = "Watch a blockchain and record its events as logs")]
    Watch {
        #[command(subcommand)]
        chain: WatchCommands,
    },

    #[command(about = "Send log records to the local daemon (for testing and scripts)")]
    Send {
        #[arg(value_name = "APP")]
        app: String,

        #[arg(value_name = "MESSAGE")]
        message: String,

        #[arg(long, default_value = "info")]
        level: String,

        #[arg(long, default_value = "cli")]
        source: String,

        #[arg(long, default_value = "1", help = "Send the message this many times")]
        count: usize,

        #[arg(long)]
        chain: Option<String>,

        #[arg(long)]
        tx_hash: Option<String>,

        #[arg(long)]
        trace_id: Option<String>,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(about = "Manage authentication keys and session tokens")]
    Auth {
        #[command(subcommand)]
        command: AuthCommands,
    },
}

#[derive(Subcommand)]
pub enum AuthCommands {
    #[command(about = "Generate RSA-2048 keypair for signing and verifying JWTs")]
    Keygen {
        #[arg(long)]
        force: bool,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(about = "Print a signed JWT for an app (exchange it at POST /auth for a session)")]
    Token {
        #[arg(value_name = "APP")]
        app: String,

        #[arg(long, default_value = "3600", value_name = "SECS")]
        ttl_secs: u64,

        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },

    #[command(about = "Print the public key path (share with SDK callers)")]
    PublicKey {
        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },
}

pub fn execute(cli: Cli) -> crate::error::Result<()> {
    match cli.command {
        Some(Commands::Init {
            force,
            storage,
            storage_path,
            profile,
        }) => commands::init(force, storage, storage_path, profile.as_deref()),
        Some(Commands::Config {
            key,
            value,
            profile,
        }) => commands::config(key, value, profile.as_deref()),
        Some(Commands::Run {
            foreground,
            profile,
        }) => commands::run(foreground, profile.as_deref()),
        Some(Commands::Status { profile }) => commands::status(profile.as_deref()),
        Some(Commands::Stop { force, profile }) => commands::stop(force, profile.as_deref()),
        Some(Commands::Logs {
            app,
            level,
            source,
            from,
            to,
            text,
            limit,
            token,
            follow,
            profile,
        }) => commands::logs(
            app,
            level,
            source,
            from,
            to,
            text,
            limit,
            token,
            follow,
            profile.as_deref(),
        ),
        Some(Commands::Watch {
            chain: WatchCommands::Ethereum { once, profile },
        }) => commands::watch_ethereum(once, profile.as_deref()),
        Some(Commands::Trace {
            tx_hash,
            apps,
            from,
            to,
            limit,
            token,
            profile,
        }) => commands::trace(tx_hash, apps, from, to, limit, token, profile.as_deref()),
        Some(Commands::Send {
            app,
            message,
            level,
            source,
            count,
            chain,
            tx_hash,
            trace_id,
            profile,
        }) => commands::send(
            app,
            message,
            level,
            source,
            count,
            chain,
            tx_hash,
            trace_id,
            profile.as_deref(),
        ),
        Some(Commands::Auth { command }) => match command {
            AuthCommands::Keygen { force, profile } => {
                commands::auth_keygen(force, profile.as_deref())
            }
            AuthCommands::Token {
                app,
                ttl_secs,
                profile,
            } => commands::auth_token(app, ttl_secs, profile.as_deref()),
            AuthCommands::PublicKey { profile } => commands::auth_public_key(profile.as_deref()),
        },
        None => {
            style::print_banner();
            println!("Run 'loghaven --help' for usage information\n");
            Ok(())
        }
    }
}
