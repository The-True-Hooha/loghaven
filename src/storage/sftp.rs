use crate::error::{LogHavenError, Result};
use crate::storage::cloud::{
    BoxFuture, MAX_LISTED, ObjectStore, STALL_TIMEOUT, Sink, upload_budget,
};
use russh::client;
use russh::keys::{HashAlg, PrivateKeyWithHashAlg};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{FileType, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Part of the name of the file an upload is written to before it is renamed into place.
const PART_MARK: &str = ".part-";
const MAX_DEPTH: usize = 32;

#[derive(Clone)]
pub(crate) enum Login {
    Password(String),
    Key {
        file: PathBuf,
        passphrase: Option<String>,
    },
}

/// A directory on an SFTP server as an object store: a key is a path below `root`.
pub(crate) struct SftpStore {
    host: String,
    port: u16,
    user: String,
    login: Login,
    /// `SHA256:...` as `ssh-keygen -lf` prints it.
    host_key: String,
    root: String,
}

fn fail(what: &str, detail: impl std::fmt::Display) -> LogHavenError {
    LogHavenError::Storage(format!("SFTP {what} failed: {detail}"))
}

/// Accepts the server only if its key is the pinned one, and keeps the key it saw to tell the
/// user when it was another.
struct Pinned {
    expected: String,
    seen: Arc<Mutex<Option<String>>>,
}

impl client::Handler for Pinned {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &russh::keys::PublicKeyOrCertificate,
    ) -> std::result::Result<bool, Self::Error> {
        let russh::keys::PublicKeyOrCertificate::PublicKey { key, .. } = key else {
            return Ok(false);
        };
        let seen = key.fingerprint(HashAlg::Sha256).to_string();
        let ok = seen == self.expected;
        *self.seen.lock().unwrap() = Some(seen);
        Ok(ok)
    }
}

impl SftpStore {
    pub(crate) fn new(
        host: &str,
        port: u16,
        user: &str,
        login: Login,
        host_key: &str,
        root: &str,
    ) -> Self {
        Self {
            host: host.to_string(),
            port,
            user: user.to_string(),
            login,
            host_key: host_key.to_string(),
            root: root.trim_end_matches('/').to_string(),
        }
    }

    fn remote(&self, key: &str) -> String {
        let key = key.trim_matches('/');
        match (self.root.is_empty(), key.is_empty()) {
            (true, true) => ".".to_string(),
            (true, false) => key.to_string(),
            (false, true) => self.root.clone(),
            (false, false) => format!("{}/{key}", self.root),
        }
    }

    async fn connect(&self) -> Result<(SftpSession, client::Handle<Pinned>)> {
        let seen = Arc::new(Mutex::new(None));
        let handler = Pinned {
            expected: self.host_key.clone(),
            seen: seen.clone(),
        };
        let config = Arc::new(client::Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(120)),
            ..Default::default()
        });
        let mut session = tokio::time::timeout(
            CONNECT_TIMEOUT,
            client::connect(config, (self.host.as_str(), self.port), handler),
        )
        .await
        .map_err(|_| fail("connect", "timed out"))?
        .map_err(|e| {
            let other = seen.lock().unwrap().clone();
            match other {
                Some(k) if k != self.host_key => fail(
                    "connect",
                    format!(
                        "the server's host key is {k}, not the pinned {}",
                        self.host_key
                    ),
                ),
                _ => fail("connect", e),
            }
        })?;

        let authed = match &self.login {
            Login::Password(pw) => session
                .authenticate_password(self.user.clone(), pw.clone())
                .await
                .map_err(|e| fail("login", e))?,
            Login::Key { file, passphrase } => {
                let text = std::fs::read_to_string(file).map_err(|e| {
                    LogHavenError::Config(format!("cannot read key_file {}: {e}", file.display()))
                })?;
                let key = russh::keys::decode_secret_key(&text, passphrase.as_deref())
                    .map_err(|e| LogHavenError::Config(format!("cannot use key_file: {e}")))?;
                let hash = session
                    .best_supported_rsa_hash()
                    .await
                    .map_err(|e| fail("login", e))?
                    .flatten();
                session
                    .authenticate_publickey(
                        self.user.clone(),
                        PrivateKeyWithHashAlg::new(Arc::new(key), hash),
                    )
                    .await
                    .map_err(|e| fail("login", e))?
            }
        };
        if !authed.success() {
            return Err(fail("login", "the server refused the credentials"));
        }
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| fail("open", e))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| fail("open", e))?;
        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| fail("open", e))?;
        Ok((sftp, session))
    }

    /// Runs `f` on a fresh connection and closes it however `f` ends.
    async fn with_session<T>(&self, f: impl AsyncFnOnce(&SftpSession) -> Result<T>) -> Result<T> {
        let (sftp, handle) = self.connect().await?;
        let result = f(&sftp).await;
        drop(sftp);
        let _ = handle
            .disconnect(russh::Disconnect::ByApplication, "", "en")
            .await;
        result
    }

    async fn make_dirs(&self, sftp: &SftpSession, key: &str) -> Result<()> {
        let parts: Vec<&str> = key.split('/').filter(|s| !s.is_empty()).collect();
        let mut path = String::new();
        let root = self.remote("");
        if root != "." && !root.is_empty() {
            // the root itself, and every directory above it, may be missing on a new server
            let absolute = root.starts_with('/');
            let mut acc = String::new();
            for seg in root.split('/').filter(|s| !s.is_empty()) {
                if absolute || !acc.is_empty() {
                    acc.push('/');
                }
                acc.push_str(seg);
                ensure_dir(sftp, &acc).await?;
            }
        }
        for part in &parts[..parts.len().saturating_sub(1)] {
            if !path.is_empty() {
                path.push('/');
            }
            path.push_str(part);
            ensure_dir(sftp, &self.remote(&path)).await?;
        }
        Ok(())
    }

    async fn send_file(&self, key: &str, file: &Path) -> Result<()> {
        self.with_session(async |sftp| {
            self.make_dirs(sftp, key).await?;
            let target = self.remote(key);
            let tmp = format!("{target}{PART_MARK}{:016x}", rand::random::<u64>());
            let placed = match self.write_part(sftp, file, &tmp).await {
                Ok(()) => replace(sftp, &tmp, &target).await,
                Err(e) => Err(e),
            };
            if placed.is_err() {
                let _ = sftp.remove_file(tmp).await;
            }
            placed
        })
        .await
    }

    async fn write_part(&self, sftp: &SftpSession, file: &Path, tmp: &str) -> Result<()> {
        let mut input = tokio::fs::File::open(file)
            .await
            .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?;
        let mut out = sftp
            .open_with_flags(
                tmp.to_string(),
                OpenFlags::CREATE | OpenFlags::EXCLUDE | OpenFlags::WRITE,
            )
            .await
            .map_err(|e| fail("write", e))?;
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = input
                .read(&mut buf)
                .await
                .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n])
                .await
                .map_err(|e| fail("write", e))?;
        }
        out.shutdown().await.map_err(|e| fail("write", e))
    }

    async fn walk(&self, prefix: &str) -> Result<Vec<String>> {
        self.with_session(async |sftp| {
            let root = self.remote("");
            let mut files = Vec::new();
            let mut todo = vec![(self.remote(prefix), 0usize)];
            while let Some((dir, depth)) = todo.pop() {
                if depth > MAX_DEPTH || files.len() > MAX_LISTED {
                    return Err(fail("list", "the listing is too deep or too large"));
                }
                let stat = tokio::time::timeout(STALL_TIMEOUT, sftp.metadata(dir.clone()))
                    .await
                    .map_err(|_| LogHavenError::Storage("listing the store timed out".into()))?;
                if stat.is_err() {
                    continue;
                }
                let entries = tokio::time::timeout(STALL_TIMEOUT, sftp.read_dir(dir.clone()))
                    .await
                    .map_err(|_| LogHavenError::Storage("listing the store timed out".into()))?
                    .map_err(|e| fail("list", e))?;
                for entry in entries {
                    let name = entry.file_name();
                    if name == "." || name == ".." || is_part_file(&name) {
                        continue;
                    }
                    let full = format!("{}/{name}", dir.trim_end_matches('/'));
                    match entry.file_type() {
                        FileType::Dir => todo.push((full, depth + 1)),
                        FileType::File => files.push(relative_to(&full, &root)),
                        _ => {}
                    }
                }
            }
            files.sort();
            Ok(files)
        })
        .await
    }

    async fn read(&self, key: &str, dest: &Path, max_bytes: u64) -> Result<u64> {
        let mut sink = Sink::create(dest, max_bytes).await?;
        self.with_session(async |sftp| {
            let path = self.remote(key);
            let stalled = || LogHavenError::Storage("download from cloud timed out".into());
            let size = tokio::time::timeout(STALL_TIMEOUT, sftp.metadata(path.clone()))
                .await
                .map_err(|_| stalled())?
                .map_err(|e| fail("read", e))?
                .size;
            if let Some(n) = size {
                sink.refuse_if_larger(n)?;
            }
            let mut file = tokio::time::timeout(STALL_TIMEOUT, sftp.open(path))
                .await
                .map_err(|_| stalled())?
                .map_err(|e| fail("read", e))?;
            let mut buf = vec![0u8; 256 * 1024];
            let all = async {
                loop {
                    let n = tokio::time::timeout(STALL_TIMEOUT, file.read(&mut buf))
                        .await
                        .map_err(|_| stalled())?
                        .map_err(|e| fail("read", e))?;
                    if n == 0 {
                        return sink.finish().await;
                    }
                    sink.push(&buf[..n]).await?;
                }
            };
            tokio::time::timeout(upload_budget(size.unwrap_or(max_bytes)), all)
                .await
                .map_err(|_| stalled())?
        })
        .await
    }
}

/// Puts `tmp` in place of `target`. SFTP (v3) does not replace a file by renaming, so an
/// existing target is first moved aside, and moved back if the swap fails: there is no moment
/// when neither the old nor the new object exists.
async fn replace(sftp: &SftpSession, tmp: &str, target: &str) -> Result<()> {
    if sftp
        .rename(tmp.to_string(), target.to_string())
        .await
        .is_ok()
    {
        return Ok(());
    }
    if sftp.metadata(target.to_string()).await.is_err() {
        return Err(fail("rename", "cannot move the uploaded file into place"));
    }
    let aside = format!("{target}{PART_MARK}{:016x}", rand::random::<u64>());
    sftp.rename(target.to_string(), aside.clone())
        .await
        .map_err(|e| fail("rename", e))?;
    match sftp.rename(tmp.to_string(), target.to_string()).await {
        Ok(()) => {
            let _ = sftp.remove_file(aside).await;
            Ok(())
        }
        Err(e) => {
            let _ = sftp.rename(aside, target.to_string()).await;
            Err(fail("rename", e))
        }
    }
}

/// `<name>.part-<16 hex digits>`: an upload in progress or one that was cut short.
fn is_part_file(name: &str) -> bool {
    name.rsplit_once(PART_MARK)
        .is_some_and(|(_, id)| id.len() == 16 && id.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// `full` as a key below `root`, with no `./` in front when the root is the login directory.
fn relative_to(full: &str, root: &str) -> String {
    let rel = match root {
        "." | "" => full.strip_prefix("./").unwrap_or(full),
        _ => full
            .strip_prefix(root)
            .map_or(full, |r| r.trim_start_matches('/')),
    };
    rel.to_string()
}

async fn ensure_dir(sftp: &SftpSession, path: &str) -> Result<()> {
    if sftp.create_dir(path.to_string()).await.is_ok() {
        return Ok(());
    }
    match sftp.metadata(path.to_string()).await {
        Ok(m) if m.is_dir() => Ok(()),
        _ => Err(fail("mkdir", format!("cannot create the directory {path}"))),
    }
}

impl ObjectStore for SftpStore {
    fn put<'a>(&'a self, key: &'a str, file: &'a Path) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let len = std::fs::metadata(file)
                .map_err(|e| LogHavenError::Storage(format!("read chunk for upload: {e}")))?
                .len();
            tokio::time::timeout(upload_budget(len), self.send_file(key, file))
                .await
                .map_err(|_| LogHavenError::Storage("upload to cloud timed out".into()))?
        })
    }

    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(self.walk(prefix))
    }

    fn download<'a>(
        &'a self,
        key: &'a str,
        dest: &'a Path,
        max_bytes: u64,
    ) -> BoxFuture<'a, Result<u64>> {
        Box::pin(self.read(key, dest, max_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(root: &str) -> SftpStore {
        SftpStore::new(
            "127.0.0.1",
            1,
            "user",
            Login::Password("secret-password".into()),
            "SHA256:x",
            root,
        )
    }

    #[test]
    fn keys_are_joined_to_the_root_without_doubled_or_missing_slashes() {
        assert_eq!(store("").remote("web/a.parquet"), "web/a.parquet");
        assert_eq!(store("").remote(""), ".");
        assert_eq!(
            store("/srv/logs/").remote("web/a.parquet"),
            "/srv/logs/web/a.parquet"
        );
        assert_eq!(store("upload").remote("/web/"), "upload/web");
        assert_eq!(store("upload").remote(""), "upload");
    }

    #[tokio::test]
    async fn a_server_that_cannot_be_reached_gives_an_error_without_the_password() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("c.parquet");
        std::fs::write(&file, b"x").unwrap();
        let e = store("")
            .put("web/2024-01-01/c.parquet", &file)
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("SFTP connect failed"), "{e}");
        assert!(!e.contains("secret-password"), "{e}");
    }

    #[test]
    fn only_the_names_an_upload_makes_are_taken_for_leftovers() {
        assert!(is_part_file("a.parquet.part-0123456789abcdef"));
        assert!(!is_part_file("a.parquet"));
        assert!(!is_part_file("my.part-notes.parquet"));
        assert!(!is_part_file("a.parquet.part-0123456789abcdeg"));
        assert!(!is_part_file("a.parquet.part-123"));
    }

    #[test]
    fn listed_paths_become_keys_below_the_root() {
        assert_eq!(relative_to("./web/a.parquet", "."), "web/a.parquet");
        assert_eq!(relative_to("web/a.parquet", ""), "web/a.parquet");
        assert_eq!(
            relative_to("/srv/logs/web/a.parquet", "/srv/logs"),
            "web/a.parquet"
        );
        assert_eq!(
            relative_to("up/logs/web/a.parquet", "up/logs"),
            "web/a.parquet"
        );
    }
}
