//! Client-side encryption of chunks on their way to a cloud store.
//!
//! Local chunks stay plain so queries can read them; only the copy that leaves the machine is
//! encrypted. File layout: `MAGIC || nonce (12 bytes) || AES-256-GCM ciphertext and tag`.

use crate::error::{LogHavenError, Result};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use rand::RngCore;
use std::path::Path;

const MAGIC: &[u8] = b"LHENC1\n";
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

/// Appended to the object key of an encrypted upload, so it is never mistaken for a Parquet file.
pub const OBJECT_SUFFIX: &str = ".enc";

pub struct Encryptor {
    cipher: Aes256Gcm,
}

fn err(what: &str) -> LogHavenError {
    LogHavenError::Storage(what.to_string())
}

impl Encryptor {
    fn from_key(key: &[u8]) -> Result<Self> {
        if key.len() != KEY_LEN {
            return Err(err("storage key must be 32 bytes"));
        }
        Ok(Self {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
        })
    }

    /// Reads the key file; with `create`, makes a new random key (mode 0600) when there is none.
    /// An existing file is never overwritten: replacing the key would make every earlier upload
    /// unreadable.
    pub fn load(path: &Path, create: bool) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let key = hex::decode(text.trim()).map_err(|_| {
                    LogHavenError::Config(format!("{} is not a hex key", path.display()))
                })?;
                Self::from_key(&key).map_err(|_| {
                    LogHavenError::Config(format!("{} must hold 64 hex characters", path.display()))
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && create => {
                let mut key = [0u8; KEY_LEN];
                rand::thread_rng().fill_bytes(&mut key);
                write_new_private(path, hex::encode(key).as_bytes())?;
                Self::from_key(&key)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(LogHavenError::Config(
                format!("no storage key at {}", path.display()),
            )),
            Err(e) => Err(e.into()),
        }
    }

    pub fn encrypt(&self, plain: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce);
        let sealed = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plain,
                    aad: MAGIC,
                },
            )
            .map_err(|_| err("encryption failed"))?;
        let mut out = Vec::with_capacity(MAGIC.len() + NONCE_LEN + sealed.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let body = data
            .strip_prefix(MAGIC)
            .filter(|b| b.len() >= NONCE_LEN + 16)
            .ok_or_else(|| err("not a LogHaven encrypted file"))?;
        let (nonce, sealed) = body.split_at(NONCE_LEN);
        self.cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: sealed,
                    aad: MAGIC,
                },
            )
            .map_err(|_| err("wrong key or damaged file"))
    }
}

fn write_new_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)?.write_all(bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn round_trip_and_every_encryption_differs() {
        let d = dir();
        let e = Encryptor::load(&d.path().join("k"), true).unwrap();
        let a = e.encrypt(b"parquet bytes").unwrap();
        let b = e.encrypt(b"parquet bytes").unwrap();
        assert_ne!(a, b, "a fresh nonce each time");
        assert!(!a.windows(7).any(|w| w == b"parquet"));
        assert_eq!(e.decrypt(&a).unwrap(), b"parquet bytes");
        assert_eq!(e.decrypt(&e.encrypt(b"").unwrap()).unwrap(), b"");
    }

    #[test]
    fn tampering_a_wrong_key_and_foreign_files_are_refused() {
        let d = dir();
        let e = Encryptor::load(&d.path().join("k1"), true).unwrap();
        let other = Encryptor::load(&d.path().join("k2"), true).unwrap();
        let sealed = e.encrypt(b"secret").unwrap();
        assert!(other.decrypt(&sealed).is_err());
        for i in [0, MAGIC.len() + 1, sealed.len() - 1] {
            let mut bad = sealed.clone();
            bad[i] ^= 1;
            assert!(e.decrypt(&bad).is_err(), "byte {i}");
        }
        assert!(e.decrypt(&sealed[..sealed.len() - 1]).is_err());
        assert!(e.decrypt(b"PAR1 plain parquet").is_err());
        assert!(e.decrypt(b"").is_err());
    }

    #[test]
    fn the_key_file_is_created_once_private_and_never_replaced() {
        let d = dir();
        let p = d.path().join("sub").join("storage.key");
        let first = Encryptor::load(&p, true).unwrap();
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(text.len(), 64);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let again = Encryptor::load(&p, true).unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), text);
        assert_eq!(again.decrypt(&first.encrypt(b"x").unwrap()).unwrap(), b"x");
    }

    #[test]
    fn a_missing_or_malformed_key_is_an_error_and_decrypt_never_creates_one() {
        let d = dir();
        let p = d.path().join("k");
        assert!(Encryptor::load(&p, false).is_err());
        assert!(!p.exists());
        for bad in ["zz", "abcd", &"a".repeat(63), ""] {
            std::fs::write(&p, bad).unwrap();
            assert!(Encryptor::load(&p, true).is_err(), "{bad:?}");
        }
    }
}
