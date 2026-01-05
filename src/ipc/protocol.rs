use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Status,
    Stop,
    Reload,
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
