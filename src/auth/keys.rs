use crate::error::{LogHavenError, Result};
use rsa::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::{RsaPrivateKey, RsaPublicKey};
use std::path::Path;

const KEY_BITS: usize = 2048;

pub fn generate_keypair(private_path: &Path, public_path: &Path) -> Result<()> {
    let mut rng = rand::thread_rng();
    let private_key = RsaPrivateKey::new(&mut rng, KEY_BITS)
        .map_err(|e| LogHavenError::Config(format!("keygen failed: {}", e)))?;
    let public_key = RsaPublicKey::from(&private_key);

    if let Some(parent) = private_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    private_key
        .write_pkcs8_pem_file(private_path, LineEnding::LF)
        .map_err(|e| LogHavenError::Config(format!("write private key: {}", e)))?;

    public_key
        .write_public_key_pem_file(public_path, LineEnding::LF)
        .map_err(|e| LogHavenError::Config(format!("write public key: {}", e)))?;

    // private key must not be world-readable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(private_path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

pub fn load_private_key(path: &Path) -> Result<RsaPrivateKey> {
    let pem = std::fs::read_to_string(path)?;
    RsaPrivateKey::from_pkcs8_pem(&pem)
        .map_err(|e| LogHavenError::Config(format!("load private key: {}", e)))
}

pub fn load_public_key(path: &Path) -> Result<RsaPublicKey> {
    let pem = std::fs::read_to_string(path)?;
    RsaPublicKey::from_public_key_pem(&pem)
        .map_err(|e| LogHavenError::Config(format!("load public key: {}", e)))
}
