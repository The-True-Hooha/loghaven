use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum LogHavenError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOML parsing error: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("TOML serialization error: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("invalid app name: {0}")]
    InvalidApp(&'static str),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("Environment variable error: {0}")]
    Env(String),

    #[error("Daemon error: {0}")]
    Daemon(String),

    /// A node answered HTTP 429: it is working, only too busy for this many requests.
    #[error("Daemon error: {0}")]
    RateLimited(String),
}

pub type Result<T> = std::result::Result<T, LogHavenError>;
