use thiserror::Error;

#[derive(Error, Debug)]
pub enum LogHavenError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("TOMl parsing error: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("Toml serialization error: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("storage error: {0}")]
    #[allow(dead_code)]
    Storage(String),

    #[error("Environment variable error: {0}")]
    Env(String),
}

pub type Result<T> = std::result::Result<T, LogHavenError>;
