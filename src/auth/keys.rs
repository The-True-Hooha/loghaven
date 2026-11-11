use crate::error::{LogHavenError, Result};
use rsa::pkcs8::{
    DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding,
};
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
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }

    let pem = private_key
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|e| LogHavenError::Config(format!("encode private key: {}", e)))?;
    write_private(private_path, pem.as_bytes())?;

    public_key
        .write_public_key_pem_file(public_path, LineEnding::LF)
        .map_err(|e| LogHavenError::Config(format!("write public key: {}", e)))?;

    Ok(())
}

/// Creates the file with mode 0600 from the start, so the key is never
/// readable by others, not even briefly.
pub(super) fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // an existing file keeps its old mode on open
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    f.write_all(bytes)?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn private_key_and_dir_are_not_readable_by_others() {
        let dir = std::env::temp_dir().join(format!("lh-kp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (private, public) = (dir.join("private.pem"), dir.join("public.pem"));
        generate_keypair(&private, &public).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&private), 0o600);
        assert_eq!(mode(&dir), 0o700);
        // regenerating over a loose file tightens it
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o644)).unwrap();
        generate_keypair(&private, &public).unwrap();
        assert_eq!(mode(&private), 0o600);
        assert!(load_private_key(&private).is_ok() && load_public_key(&public).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
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
