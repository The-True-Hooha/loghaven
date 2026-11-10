use crate::config::Config;
use crate::error::Result;
use std::time::{Duration, Instant};

/// A query token that is minted on demand and renewed at 80% of the lifetime
/// the daemon actually granted.
pub struct QueryToken {
    cfg: Config,
    app: String,
    explicit: Option<String>,
    ttl_secs: u64,
    current: Option<(String, Instant)>,
}

impl QueryToken {
    pub fn new(cfg: &Config, app: &str, explicit: Option<String>, ttl_secs: u64) -> Self {
        Self {
            cfg: cfg.clone(),
            app: app.to_string(),
            explicit,
            ttl_secs,
            current: None,
        }
    }

    pub async fn get(&mut self) -> Result<Option<String>> {
        if let Some(t) = &self.explicit {
            return Ok(Some(t.clone()));
        }
        if !self.cfg.auth.require_auth {
            return Ok(None);
        }
        let stale = self
            .current
            .as_ref()
            .is_none_or(|(_, renew_at)| Instant::now() >= *renew_at);
        if stale {
            let s = super::open_session(&self.cfg, &self.app, self.ttl_secs).await?;
            let renew_in = Duration::from_secs(s.expires_in_secs * 4 / 5);
            self.current = Some((s.token_id, Instant::now() + renew_in));
        }
        Ok(self.current.as_ref().map(|(t, _)| t.clone()))
    }
}
