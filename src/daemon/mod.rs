mod process;
mod server;
mod state;

use crate::config::Config;
use crate::error::Result;
pub use process::DaemonProcess;
pub use server::DaemonServer;
pub use state::DaemonState;

pub struct Daemon {
    config: Config,
    state: DaemonState,
}

impl Daemon {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            state: DaemonState::new(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.state.mark_started();

        let server = DaemonServer::new(&self.config).await?;
        server.listen().await?;

        Ok(())
    }
}
