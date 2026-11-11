//! Client-side encryption of chunks on their way to a cloud store.
//!
//! Local chunks stay plain so queries can read them; only the copy that leaves the machine is
//! encrypted. File layout: `MAGIC || key id (8 bytes) || nonce (12 bytes) || AES-256-GCM
//! ciphertext and tag`. The header and the chunk's name (`<app>/<date>/<chunk>.parquet`) are
//! authenticated, so a chunk cannot be passed off under another name, and the key id says which
//! key made it, which is what allows a key to be replaced.

use crate::error::{LogHavenError, Result};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const MAGIC: &[u8] = b"LHENC2\n";
const ID_LEN: usize = 8;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const KEY_LEN: usize = 32;

/// Appended to the object key of an encrypted upload, so it is never mistaken for a Parquet file.
pub const OBJECT_SUFFIX: &str = ".enc";
/// When set (64 hex characters), this is the key; no key file is read or created.
pub const KEY_ENV: &str = "LOGHAVEN_STORAGE_KEY";
const KEY_FILE: &str = "storage.key";

pub struct Encryptor {
    cipher: Aes256Gcm,
    id: [u8; ID_LEN],
}

fn err(what: &str) -> LogHavenError {
    LogHavenError::Storage(what.to_string())
}

fn key_id(key: &[u8]) -> [u8; ID_LEN] {
    let mut id = [0u8; ID_LEN];
    id.copy_from_slice(&Sha256::digest(key)[..ID_LEN]);
    id
}

fn parse_key(text: &str, what: &str) -> Result<Vec<u8>> {
    hex::decode(text.trim())
        .ok()
        .filter(|k| k.len() == KEY_LEN)
        .ok_or_else(|| LogHavenError::Config(format!("{what} must hold 64 hex characters")))
}

/// `storage.key.<16 hex>` files; anything else with a similar name (a backup, a swap file) is not a key.
fn archived_keys(keys_dir: &Path) -> Vec<PathBuf> {
    let prefix = format!("{KEY_FILE}.");
    let mut found: Vec<PathBuf> = std::fs::read_dir(keys_dir)
        .map(|d| {
            d.flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|n| n.strip_prefix(&prefix))
                        .is_some_and(|id| {
                            id.len() == ID_LEN * 2 && id.bytes().all(|b| b.is_ascii_hexdigit())
                        })
                })
                .collect()
        })
        .unwrap_or_default();
    found.sort();
    found
}

/// What the tag covers besides the ciphertext.
fn aad(id: &[u8; ID_LEN], name: &str) -> Vec<u8> {
    [MAGIC, id, name.as_bytes()].concat()
}

impl Encryptor {
    fn from_key(key: &[u8]) -> Self {
        Self {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
            id: key_id(key),
        }
    }

    pub fn id(&self) -> String {
        hex::encode(self.id)
    }

    /// The key for new uploads: `LOGHAVEN_STORAGE_KEY` if set, else the key file, which is made
    /// (mode 0600) when there is none. An existing file is never overwritten: replacing the key
    /// would make every earlier upload unreadable. Use `rotate` to change it.
    pub fn current(keys_dir: &Path) -> Result<Self> {
        if let Ok(v) = std::env::var(KEY_ENV) {
            return Ok(Self::from_key(&parse_key(&v, KEY_ENV)?));
        }
        let path = keys_dir.join(KEY_FILE);
        match std::fs::read_to_string(&path) {
            Ok(text) => Ok(Self::from_key(&parse_key(
                &text,
                &path.display().to_string(),
            )?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // a missing key next to archived ones is a lost or unmounted file, and a new key
                // would quietly split the uploads between two keys
                if !archived_keys(keys_dir).is_empty() {
                    return Err(LogHavenError::Config(format!(
                        "{} is missing but archived keys are next to it; restore it (or copy the \
                         newest storage.key.<id> back to storage.key) before starting",
                        path.display()
                    )));
                }
                let key = Self::create(&path)?;
                eprintln!(
                    "Created a new upload encryption key {} at {}. Back it up: without it, \
                     uploaded chunks cannot be read.",
                    key.id(),
                    path.display()
                );
                Ok(key)
            }
            Err(e) => Err(e.into()),
        }
    }

    fn create(path: &Path) -> Result<Self> {
        let mut key = [0u8; KEY_LEN];
        rand::thread_rng().fill_bytes(&mut key);
        write_new_private(path, hex::encode(key).as_bytes())?;
        Ok(Self::from_key(&key))
    }

    /// Every key that can be found, for reading: the environment key, the current file and the
    /// archived `storage.key.<id>` files. Never creates anything.
    pub fn all(keys_dir: &Path) -> Result<Vec<Self>> {
        let mut keys = vec![];
        if let Ok(v) = std::env::var(KEY_ENV) {
            keys.push(Self::from_key(&parse_key(&v, KEY_ENV)?));
        }
        let mut paths = archived_keys(keys_dir);
        if keys_dir.join(KEY_FILE).exists() {
            paths.push(keys_dir.join(KEY_FILE));
        }
        for p in paths {
            keys.push(Self::from_key(&parse_key(
                &std::fs::read_to_string(&p)?,
                &p.display().to_string(),
            )?));
        }
        if keys.is_empty() {
            return Err(LogHavenError::Config(format!(
                "no storage key: set {KEY_ENV} or put one in {}",
                keys_dir.join(KEY_FILE).display()
            )));
        }
        Ok(keys)
    }

    /// Archives the current key file as `storage.key.<id>` and makes a new one. Chunks already
    /// uploaded stay readable with the archived key; new uploads use the new one.
    pub fn rotate(keys_dir: &Path) -> Result<(String, String)> {
        if std::env::var(KEY_ENV).is_ok() {
            return Err(LogHavenError::Config(format!(
                "{KEY_ENV} is set: change the key where you set it, and keep the old one for reading"
            )));
        }
        let path = keys_dir.join(KEY_FILE);
        let old = Self::from_key(&parse_key(
            &std::fs::read_to_string(&path).map_err(|_| {
                LogHavenError::Config(format!("no key at {} to rotate", path.display()))
            })?,
            &path.display().to_string(),
        )?);
        let archived = keys_dir.join(format!("{KEY_FILE}.{}", old.id()));
        // `storage.key` is never missing: the new key is written beside it, the old one gets a
        // second name (which fails if that name exists), then the new one replaces it in one step
        let fresh = keys_dir.join(format!("{KEY_FILE}.new"));
        let _ = std::fs::remove_file(&fresh);
        let new = Self::create(&fresh)?;
        let linked = std::fs::hard_link(&path, &archived).map_err(|e| {
            let _ = std::fs::remove_file(&fresh);
            LogHavenError::Config(format!(
                "cannot archive the key as {}: {e}",
                archived.display()
            ))
        });
        linked?;
        std::fs::rename(&fresh, &path)?;
        Ok((old.id(), new.id()))
    }

    pub fn encrypt(&self, name: &str, plain: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_LEN];
        rand::thread_rng().fill_bytes(&mut nonce);
        let sealed = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plain,
                    aad: &aad(&self.id, name),
                },
            )
            .map_err(|_| err("encryption failed"))?;
        let mut out = Vec::with_capacity(MAGIC.len() + ID_LEN + NONCE_LEN + sealed.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.id);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    /// The id of the key `data` was encrypted with.
    fn id_in(data: &[u8]) -> Result<[u8; ID_LEN]> {
        let body = data
            .strip_prefix(MAGIC)
            .filter(|b| b.len() >= ID_LEN + NONCE_LEN + TAG_LEN)
            .ok_or_else(|| err("not a LogHaven encrypted file"))?;
        let mut id = [0u8; ID_LEN];
        id.copy_from_slice(&body[..ID_LEN]);
        Ok(id)
    }

    /// `name` must be the name the chunk was encrypted under. A key that is not this one's is
    /// reported as such, so a missing archived key is not mistaken for a damaged file.
    pub fn decrypt(&self, name: &str, data: &[u8]) -> Result<Vec<u8>> {
        let id = Self::id_in(data)?;
        if id != self.id {
            return Err(err(&format!(
                "encrypted with key {}, not {}",
                hex::encode(id),
                self.id()
            )));
        }
        let body = &data[MAGIC.len() + ID_LEN..];
        let (nonce, sealed) = body.split_at(NONCE_LEN);
        self.cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: sealed,
                    aad: &aad(&id, name),
                },
            )
            .map_err(|_| err("damaged file, or it belongs under a different name"))
    }
}

/// Decrypts with whichever of `keys` made `data`.
pub fn decrypt_with(keys: &[Encryptor], name: &str, data: &[u8]) -> Result<Vec<u8>> {
    let id = Encryptor::id_in(data)?;
    match keys.iter().find(|k| k.id == id) {
        Some(k) => k.decrypt(name, data),
        None => Err(err(&format!(
            "encrypted with key {}, which is not among the keys found ({})",
            hex::encode(id),
            keys.iter()
                .map(Encryptor::id)
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

pub(crate) fn write_new_private(path: &Path, bytes: &[u8]) -> Result<()> {
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

/// The environment is shared by every test thread, so tests that depend on the key variable
/// hold this; it also clears the variable.
#[cfg(test)]
pub(crate) fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let g = ENV.lock().unwrap_or_else(|e| e.into_inner());
    unsafe { std::env::remove_var(KEY_ENV) };
    g
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAME: &str = "web/2024-01-01/1-1.parquet";

    use super::env_lock;

    #[test]
    fn round_trip_and_every_encryption_differs() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        let e = Encryptor::current(d.path()).unwrap();
        let a = e.encrypt(NAME, b"parquet bytes").unwrap();
        let b = e.encrypt(NAME, b"parquet bytes").unwrap();
        assert_ne!(a, b, "a fresh nonce each time");
        assert!(!a.windows(7).any(|w| w == b"parquet"));
        assert_eq!(e.decrypt(NAME, &a).unwrap(), b"parquet bytes");
        assert_eq!(
            e.decrypt(NAME, &e.encrypt(NAME, b"").unwrap()).unwrap(),
            b""
        );
    }

    #[test]
    fn a_chunk_cannot_be_read_under_another_name() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        let e = Encryptor::current(d.path()).unwrap();
        let sealed = e.encrypt(NAME, b"rows").unwrap();
        for other in [
            "web/2024-01-01/1-2.parquet",
            "api/2024-01-01/1-1.parquet",
            "",
        ] {
            let m = e.decrypt(other, &sealed).unwrap_err().to_string();
            assert!(m.contains("different name"), "{other:?}: {m}");
        }
    }

    #[test]
    fn tampering_and_foreign_files_are_refused() {
        let _g = env_lock();
        let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let e = Encryptor::current(d1.path()).unwrap();
        let other = Encryptor::current(d2.path()).unwrap();
        let sealed = e.encrypt(NAME, b"secret").unwrap();
        let m = other.decrypt(NAME, &sealed).unwrap_err().to_string();
        assert!(m.contains(&e.id()) && m.contains(&other.id()), "{m}");
        let body_start = MAGIC.len() + ID_LEN;
        for i in [0, MAGIC.len() + 1, body_start + 1, sealed.len() - 1] {
            let mut bad = sealed.clone();
            bad[i] ^= 1;
            assert!(e.decrypt(NAME, &bad).is_err(), "byte {i}");
        }
        assert!(e.decrypt(NAME, &sealed[..sealed.len() - 1]).is_err());
        assert!(e.decrypt(NAME, b"PAR1 plain parquet").is_err());
        assert!(e.decrypt(NAME, b"").is_err());
        assert!(e.decrypt(NAME, MAGIC).is_err());
    }

    #[test]
    fn the_key_file_is_created_once_private_and_never_replaced() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("keys");
        let first = Encryptor::current(&dir).unwrap();
        let p = dir.join(KEY_FILE);
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
        let again = Encryptor::current(&dir).unwrap();
        assert_eq!(std::fs::read_to_string(&p).unwrap(), text);
        assert_eq!(first.id(), again.id());
    }

    #[test]
    fn a_malformed_key_is_an_error_and_reading_never_creates_one() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        assert!(Encryptor::all(d.path()).is_err());
        assert!(!d.path().join(KEY_FILE).exists());
        let p = d.path().join(KEY_FILE);
        for bad in ["zz", "abcd", &"a".repeat(63), ""] {
            std::fs::write(&p, bad).unwrap();
            assert!(Encryptor::current(d.path()).is_err(), "{bad:?}");
            assert!(Encryptor::all(d.path()).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn the_environment_key_wins_and_creates_no_file() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var(KEY_ENV, "ab".repeat(32)) };
        let e = Encryptor::current(d.path()).unwrap();
        assert!(!d.path().join(KEY_FILE).exists());
        assert_eq!(
            e.id(),
            key_id(&[0xab; 32])
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        unsafe { std::env::set_var(KEY_ENV, "nothex") };
        assert!(Encryptor::current(d.path()).is_err());
        assert!(Encryptor::rotate(d.path()).is_err());
        unsafe { std::env::remove_var(KEY_ENV) };
    }

    #[test]
    fn rotation_keeps_old_uploads_readable_and_new_ones_use_the_new_key() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        let old = Encryptor::current(d.path()).unwrap();
        let sealed_old = old.encrypt(NAME, b"before").unwrap();
        let (was, now) = Encryptor::rotate(d.path()).unwrap();
        assert_eq!(was, old.id());
        let new = Encryptor::current(d.path()).unwrap();
        assert_eq!(new.id(), now);
        assert_ne!(old.id(), new.id());
        assert!(d.path().join(format!("{KEY_FILE}.{was}")).exists());
        let sealed_new = new.encrypt(NAME, b"after").unwrap();
        assert!(new.decrypt(NAME, &sealed_old).is_err());

        let keys = Encryptor::all(d.path()).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(decrypt_with(&keys, NAME, &sealed_old).unwrap(), b"before");
        assert_eq!(decrypt_with(&keys, NAME, &sealed_new).unwrap(), b"after");
        let lost = vec![Encryptor::current(d.path()).unwrap()];
        let m = decrypt_with(&lost, NAME, &sealed_old)
            .unwrap_err()
            .to_string();
        assert!(m.contains("not among the keys found"), "{m}");
        assert!(Encryptor::rotate(&d.path().join("nothing")).is_err());
    }

    #[test]
    fn stray_files_named_like_keys_do_not_break_reading() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        let e = Encryptor::current(d.path()).unwrap();
        for stray in [
            "storage.key.bak",
            "storage.key.swp",
            "storage.key.new",
            "storage.key.zzzzzzzzzzzzzzzz",
        ] {
            std::fs::write(d.path().join(stray), "not a key").unwrap();
        }
        let keys = Encryptor::all(d.path()).unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].id(), e.id());
    }

    #[test]
    fn a_missing_key_next_to_archived_ones_is_an_error_not_a_new_key() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        Encryptor::current(d.path()).unwrap();
        Encryptor::rotate(d.path()).unwrap();
        std::fs::remove_file(d.path().join(KEY_FILE)).unwrap();
        let m = Encryptor::current(d.path()).err().unwrap().to_string();
        assert!(m.contains("archived keys"), "{m}");
        assert!(!d.path().join(KEY_FILE).exists());
    }

    #[test]
    fn rotation_leaves_a_key_in_place_at_every_step_and_cleans_up_after_a_refusal() {
        let _g = env_lock();
        let d = tempfile::tempdir().unwrap();
        let old = Encryptor::current(d.path()).unwrap();
        // the archive name is taken: nothing changes
        std::fs::write(d.path().join(format!("{KEY_FILE}.{}", old.id())), "taken").unwrap();
        assert!(Encryptor::rotate(d.path()).is_err());
        assert_eq!(Encryptor::current(d.path()).unwrap().id(), old.id());
        assert!(!d.path().join("storage.key.new").exists());
        // a leftover from a crashed rotation does not block the next one
        std::fs::remove_file(d.path().join(format!("{KEY_FILE}.{}", old.id()))).unwrap();
        std::fs::write(d.path().join("storage.key.new"), "left over").unwrap();
        let (was, _) = Encryptor::rotate(d.path()).unwrap();
        assert_eq!(was, old.id());
        assert!(d.path().join(KEY_FILE).exists());
        assert!(!d.path().join("storage.key.new").exists());
    }
}
