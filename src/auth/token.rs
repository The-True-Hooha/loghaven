use crate::error::{LogHavenError, Result};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String, // app identifier
    pub exp: u64,    // unix timestamp
    pub iat: u64,
}

pub fn verify_jwt(token: &str, public_key_pem: &str) -> Result<Claims> {
    let key = DecodingKey::from_rsa_pem(public_key_pem.as_bytes())
        .map_err(|e| LogHavenError::Config(format!("invalid public key: {}", e)))?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.validate_exp = true;

    let data = decode::<Claims>(token, &key, &validation)
        .map_err(|e| LogHavenError::Config(format!("JWT verify failed: {}", e)))?;

    Ok(data.claims)
}

pub fn issue_jwt(app: &str, private_key_pem: &str, ttl_secs: u64) -> Result<String> {
    use jsonwebtoken::{EncodingKey, Header, encode};
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let claims = Claims {
        sub: app.to_string(),
        iat: now,
        exp: now + ttl_secs,
    };

    let key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
        .map_err(|e| LogHavenError::Config(format!("invalid private key: {}", e)))?;

    encode(&Header::new(Algorithm::RS256), &claims, &key)
        .map_err(|e| LogHavenError::Config(format!("JWT sign failed: {}", e)))
}
