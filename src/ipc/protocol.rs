use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Status,
    Stop,
    Reload,
    /// Exchange an RS256-signed JWT for a session token (token_id + hmac_secret).
    Auth { jwt: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub success: bool,
    pub data: Option<ResponseData>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResponseData {
    Status(StatusData),
    Message(String),
    Session(SessionData),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionData {
    pub token_id: String,
    pub secret_hex: String, // hex-encoded 32-byte HMAC key
    pub expires_in_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusData {
    pub pid: u32,
    pub uptime: u64,
    pub version: String,
    pub status: String,
    pub storage_backend: String,
    pub log_level: String,
}

impl Response {
    pub fn success(data: ResponseData) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(msg: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg),
        }
    }
}
