//! An SFTP server over a real directory, for tests: the SSH handshake, the host key and the
//! password check are real, and files are read and written on disk. Like SFTP v3 it refuses to
//! rename onto an existing file.

use russh::keys::{HashAlg, PrivateKey};
use russh::server::{Auth, ChannelOpenHandle, Msg, Server as _, Session};
use russh::{Channel, ChannelId};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Status, StatusCode, Version,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub(crate) const USER: &str = "tester";
pub(crate) const PASSWORD: &str = "right-password";

pub(crate) struct Running {
    pub port: u16,
    pub fingerprint: String,
}

/// Serves the whole file system (paths are used as they are) on a free local port.
pub(crate) async fn start() -> Running {
    start_with(false).await
}

/// With `stall_reads` the server answers no read, as a server whose disk hangs would.
pub(crate) async fn start_with(stall_reads: bool) -> Running {
    let key = PrivateKey::from(russh::keys::ssh_key::private::Ed25519Keypair::from_seed(
        &rand::random::<[u8; 32]>(),
    ));
    let fingerprint = key.public_key().fingerprint(HashAlg::Sha256).to_string();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config = Arc::new(russh::server::Config {
        keys: vec![key],
        auth_rejection_time: std::time::Duration::from_millis(1),
        auth_rejection_time_initial: Some(std::time::Duration::ZERO),
        ..Default::default()
    });
    tokio::spawn(async move {
        let _ = Listener(stall_reads)
            .run_on_address(config, ("127.0.0.1", port))
            .await;
    });
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Running { port, fingerprint }
}

#[derive(Clone)]
struct Listener(bool);

impl russh::server::Server for Listener {
    type Handler = Ssh;
    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Ssh {
        Ssh(HashMap::new(), self.0)
    }
}

struct Ssh(HashMap<ChannelId, Channel<Msg>>, bool);

impl russh::server::Handler for Ssh {
    type Error = russh::Error;

    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth, Self::Error> {
        Ok(if user == USER && password == PASSWORD {
            Auth::Accept
        } else {
            Auth::reject()
        })
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.0.insert(channel.id(), channel);
        reply.accept().await;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        match (name, self.0.remove(&id)) {
            ("sftp", Some(channel)) => {
                session.channel_success(id)?;
                russh_sftp::server::run(
                    channel.into_stream(),
                    Files {
                        stall_reads: self.1,
                        ..Default::default()
                    },
                )
                .await;
            }
            _ => session.channel_failure(id)?,
        }
        Ok(())
    }
}

enum Open {
    File(tokio::fs::File),
    Dir(Option<Vec<File>>),
}

#[derive(Default)]
struct Files {
    open: HashMap<String, Open>,
    next: u64,
    stall_reads: bool,
}

fn status(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: "Ok".into(),
        language_tag: "en-US".into(),
    }
}

fn code(e: std::io::Error) -> StatusCode {
    match e.kind() {
        std::io::ErrorKind::NotFound => StatusCode::NoSuchFile,
        std::io::ErrorKind::PermissionDenied => StatusCode::PermissionDenied,
        _ => StatusCode::Failure,
    }
}

fn attrs(meta: &std::fs::Metadata) -> FileAttributes {
    let mut a = FileAttributes {
        size: Some(meta.len()),
        ..Default::default()
    };
    if meta.is_dir() {
        a.set_dir(true);
    } else {
        a.set_regular(true);
    }
    a
}

impl Files {
    fn handle(&mut self, open: Open) -> String {
        self.next += 1;
        let h = format!("h{}", self.next);
        self.open.insert(h.clone(), open);
        h
    }
}

impl russh_sftp::server::Handler for Files {
    type Error = StatusCode;

    fn unimplemented(&self) -> StatusCode {
        StatusCode::OpUnsupported
    }

    async fn init(&mut self, _: u32, _: HashMap<String, String>) -> Result<Version, StatusCode> {
        Ok(Version::new())
    }

    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, StatusCode> {
        Ok(Name {
            id,
            files: vec![File::dummy(path)],
        })
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, StatusCode> {
        let meta = tokio::fs::metadata(&path).await.map_err(code)?;
        Ok(Attrs {
            id,
            attrs: attrs(&meta),
        })
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, StatusCode> {
        self.stat(id, path).await
    }

    async fn open(
        &mut self,
        id: u32,
        path: String,
        flags: OpenFlags,
        _: FileAttributes,
    ) -> Result<Handle, StatusCode> {
        let mut o = tokio::fs::OpenOptions::new();
        o.read(flags.contains(OpenFlags::READ))
            .write(flags.contains(OpenFlags::WRITE))
            .truncate(flags.contains(OpenFlags::TRUNCATE));
        if flags.contains(OpenFlags::CREATE) {
            if flags.contains(OpenFlags::EXCLUDE) {
                o.create_new(true);
            } else {
                o.create(true);
            }
        }
        let file = o.open(&path).await.map_err(code)?;
        Ok(Handle {
            id,
            handle: self.handle(Open::File(file)),
        })
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, StatusCode> {
        if let Some(Open::File(mut f)) = self.open.remove(&handle) {
            f.flush().await.map_err(code)?;
        }
        Ok(status(id))
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, StatusCode> {
        if self.stall_reads {
            std::future::pending::<()>().await;
        }
        let Some(Open::File(f)) = self.open.get_mut(&handle) else {
            return Err(StatusCode::Failure);
        };
        f.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(code)?;
        let mut data = vec![0u8; len as usize];
        let n = f.read(&mut data).await.map_err(code)?;
        if n == 0 {
            return Err(StatusCode::Eof);
        }
        data.truncate(n);
        Ok(Data { id, data })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, StatusCode> {
        let Some(Open::File(f)) = self.open.get_mut(&handle) else {
            return Err(StatusCode::Failure);
        };
        f.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(code)?;
        f.write_all(&data).await.map_err(code)?;
        Ok(status(id))
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, StatusCode> {
        let mut files = Vec::new();
        let mut dir = tokio::fs::read_dir(&path).await.map_err(code)?;
        while let Some(e) = dir.next_entry().await.map_err(code)? {
            let meta = e.metadata().await.map_err(code)?;
            files.push(File::new(
                e.file_name().to_string_lossy().into_owned(),
                attrs(&meta),
            ));
        }
        Ok(Handle {
            id,
            handle: self.handle(Open::Dir(Some(files))),
        })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, StatusCode> {
        match self.open.get_mut(&handle) {
            Some(Open::Dir(files)) => match files.take() {
                Some(files) => Ok(Name { id, files }),
                None => Err(StatusCode::Eof),
            },
            _ => Err(StatusCode::Failure),
        }
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        _: FileAttributes,
    ) -> Result<Status, StatusCode> {
        tokio::fs::create_dir(&path).await.map_err(code)?;
        Ok(status(id))
    }

    async fn rename(&mut self, id: u32, from: String, to: String) -> Result<Status, StatusCode> {
        if tokio::fs::symlink_metadata(&to).await.is_ok() {
            return Err(StatusCode::Failure);
        }
        tokio::fs::rename(&from, &to).await.map_err(code)?;
        Ok(status(id))
    }

    async fn remove(&mut self, id: u32, path: String) -> Result<Status, StatusCode> {
        tokio::fs::remove_file(&path).await.map_err(code)?;
        Ok(status(id))
    }
}
