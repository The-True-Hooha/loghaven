//! New-block notifications over WebSocket (`eth_subscribe newHeads`).
//!
//! The notification only ends the wait between polls early. What is read, the
//! cursor and the confirmation depth all stay on the HTTP poll path, so a dead,
//! silent or lying socket costs latency, never correctness.

use crate::cli::style;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep_until, timeout_at};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

type Conn = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Polls at most this often however fast blocks arrive.
const MIN_GAP: Duration = Duration::from_secs(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ERR_CHARS: usize = 200;

/// Nodes announce a head every few seconds; a bigger message is not a head.
const MAX_MESSAGE_BYTES: usize = 1 << 20;

pub struct Heads {
    url: String,
    conn: Option<Conn>,
    /// The node does not know `eth_subscribe`; it is not asked again.
    unsupported: bool,
    last_warning: String,
}

enum Heard {
    Head,
    Refused(String, bool),
    Gone(String),
    Other,
}

fn classify(message: Message) -> Heard {
    let bytes = match &message {
        Message::Text(t) => t.as_bytes(),
        Message::Binary(b) => b.as_ref(),
        Message::Close(_) => return Heard::Gone("websocket closed by the node".into()),
        _ => return Heard::Other,
    };
    let msg: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
    if msg.get("method").and_then(Value::as_str) == Some("eth_subscription") {
        return Heard::Head;
    }
    let Some(err) = msg.get("error") else {
        return Heard::Other;
    };
    let text: String = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_ERR_CHARS)
        .collect();
    // only "no such method" is permanent; a rate limit or a busy node may pass
    let permanent = err.get("code").and_then(Value::as_i64) == Some(-32601);
    Heard::Refused(text, permanent)
}

impl Heads {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            conn: None,
            unsupported: false,
            last_warning: String::new(),
        }
    }

    fn warn(&mut self, msg: String) {
        if msg != self.last_warning {
            style::warning(&msg);
            self.last_warning = msg;
        }
    }

    /// The URL often embeds an API key, so errors never carry it. Gives up at `deadline`.
    async fn connect(&self, deadline: Instant) -> std::result::Result<Conn, String> {
        let fail = |what: &str, e: &dyn std::fmt::Display| {
            let text: String = e.to_string().chars().take(MAX_ERR_CHARS).collect();
            format!("websocket {what} failed: {text}; polling on the interval")
        };
        let timed_out = |what: &str| format!("websocket {what} timed out; polling on the interval");
        let deadline = deadline.min(Instant::now() + CONNECT_TIMEOUT);
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MESSAGE_BYTES));
        let (mut conn, _) = timeout_at(
            deadline,
            connect_async_with_config(self.url.as_str(), Some(config), false),
        )
        .await
        .map_err(|_| timed_out("connect"))?
        .map_err(|e| fail("connect", &e))?;
        let subscribe =
            json!({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]});
        timeout_at(deadline, conn.send(Message::text(subscribe.to_string())))
            .await
            .map_err(|_| timed_out("subscribe"))?
            .map_err(|e| fail("subscribe", &e))?;
        Ok(conn)
    }

    /// Returns when a new block is announced or after `interval`, whichever is first,
    /// but never sooner than `MIN_GAP` after the call, and never later than `interval`.
    ///
    /// The socket is read only here, so the node's pings go unanswered while a poll
    /// runs; a node that drops quiet clients costs a reconnect, not data.
    pub async fn wait(&mut self, interval: Duration) {
        let start = Instant::now();
        let deadline = start + interval;
        let floor = start + MIN_GAP.min(interval);
        if self.unsupported {
            return sleep_until(deadline).await;
        }
        if self.conn.is_none() {
            match self.connect(deadline).await {
                Ok(conn) => {
                    self.last_warning.clear();
                    self.conn = Some(conn);
                }
                Err(msg) => {
                    self.warn(msg);
                    return sleep_until(deadline).await;
                }
            }
        }
        loop {
            let Some(conn) = self.conn.as_mut() else {
                return sleep_until(deadline).await;
            };
            let heard = match timeout_at(deadline, conn.next()).await {
                Err(_) => return,
                Ok(None) => Heard::Gone("websocket closed by the node".into()),
                Ok(Some(Err(e))) => {
                    let text: String = e.to_string().chars().take(MAX_ERR_CHARS).collect();
                    Heard::Gone(format!("websocket failed: {text}"))
                }
                Ok(Some(Ok(message))) => classify(message),
            };
            match heard {
                Heard::Head => {
                    sleep_until(floor).await;
                    // heads that piled up meanwhile are one wake-up, not many
                    while let Some(next) = self.conn.as_mut().and_then(|c| c.next().now_or_never())
                    {
                        let alive = match next {
                            Some(Ok(m)) => matches!(classify(m), Heard::Head | Heard::Other),
                            _ => false,
                        };
                        if !alive {
                            self.conn = None;
                            break;
                        }
                    }
                    return;
                }
                Heard::Refused(text, permanent) => {
                    self.conn = None;
                    self.unsupported = permanent;
                    self.warn(format!(
                        "node refused eth_subscribe newHeads ({text}); polling on the interval"
                    ));
                    return sleep_until(deadline).await;
                }
                Heard::Gone(msg) => {
                    self.conn = None;
                    self.warn(format!("{msg}; polling on the interval"));
                    return sleep_until(deadline).await;
                }
                Heard::Other => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::accept_async;

    enum Cmd {
        Head,
        Error,
        Busy,
        Close,
        HeadThenClose,
    }

    /// A node that accepts subscriptions and sends what the test tells it to.
    async fn node() -> (String, mpsc::UnboundedSender<Cmd>, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/secret-key", listener.local_addr().unwrap());
        let (tx, mut rx) = mpsc::unbounded_channel::<Cmd>();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    return;
                };
                count.fetch_add(1, Ordering::SeqCst);
                let mut ws = accept_async(sock).await.unwrap();
                let sub = ws.next().await.unwrap().unwrap();
                assert!(sub.to_text().unwrap().contains("newHeads"));
                loop {
                    let Some(cmd) = rx.recv().await else { return };
                    let refusal = matches!(cmd, Cmd::Error | Cmd::Busy);
                    let out = match cmd {
                        Cmd::Head => json!({"jsonrpc":"2.0","method":"eth_subscription",
                            "params":{"subscription":"0x1","result":{"number":"0x10"}}}),
                        Cmd::Error => {
                            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}})
                        }
                        Cmd::Busy => {
                            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limited"}})
                        }
                        Cmd::Close => break,
                        Cmd::HeadThenClose => {
                            let head = json!({"method":"eth_subscription","params":{"result":{}}});
                            ws.send(Message::text(head.to_string())).await.unwrap();
                            break;
                        }
                    };
                    ws.send(Message::text(out.to_string())).await.unwrap();
                    if refusal {
                        break;
                    }
                }
                let _ = ws.close(None).await;
            }
        });
        (url, tx, accepted)
    }

    async fn timed(h: &mut Heads, interval: Duration) -> Duration {
        let t = Instant::now();
        h.wait(interval).await;
        t.elapsed()
    }

    const LONG: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn a_new_head_ends_the_wait_early_but_not_before_the_minimum_gap() {
        let (url, tx, _) = node().await;
        let mut h = Heads::new(&url);
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            tx.send(Cmd::Head).unwrap();
            tx
        });
        let took = timed(&mut h, LONG).await;
        let _tx = sender.await.unwrap();
        assert!(took >= Duration::from_millis(900), "{took:?}");
        assert!(took < Duration::from_secs(15), "{took:?}");
    }

    #[tokio::test]
    async fn without_a_head_the_wait_lasts_the_whole_interval() {
        let (url, _tx, _) = node().await;
        let mut h = Heads::new(&url);
        let took = timed(&mut h, Duration::from_millis(1500)).await;
        assert!(took >= Duration::from_millis(1450), "{took:?}");
        assert!(took < Duration::from_secs(10), "{took:?}");
    }

    #[tokio::test]
    async fn heads_that_piled_up_count_as_one_wake_up() {
        let (url, tx, _) = node().await;
        let mut h = Heads::new(&url);
        let interval = Duration::from_secs(2);
        let t = Instant::now();
        let first = timed(&mut h, interval);
        for _ in 0..5 {
            tx.send(Cmd::Head).unwrap();
        }
        first.await;
        assert!(t.elapsed() < interval, "woke early for the first head");
        let second = timed(&mut h, interval).await;
        assert!(second >= Duration::from_millis(1900), "{second:?}");
    }

    #[tokio::test]
    async fn an_unreachable_node_falls_back_to_the_interval_and_hides_the_url() {
        let mut h = Heads::new("ws://127.0.0.1:1/secret-key");
        let took = timed(&mut h, Duration::from_millis(1200)).await;
        assert!(took >= Duration::from_millis(1150), "{took:?}");
        let msg = h
            .connect(Instant::now() + Duration::from_secs(5))
            .await
            .err()
            .unwrap();
        assert!(
            !msg.contains("secret-key") && !msg.contains("127.0.0.1:1/"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn a_closed_socket_is_reopened_on_the_next_wait() {
        let (url, tx, accepted) = node().await;
        let mut h = Heads::new(&url);
        tx.send(Cmd::Close).unwrap();
        let took = timed(&mut h, Duration::from_millis(1500)).await;
        assert!(
            took >= Duration::from_millis(1450),
            "closed socket must not end the wait early"
        );
        assert!(h.conn.is_none());
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            tx.send(Cmd::Head).unwrap();
            tx
        });
        let took = timed(&mut h, LONG).await;
        let _tx = sender.await.unwrap();
        assert!(took < Duration::from_secs(5), "{took:?}");
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_node_that_refuses_the_subscription_is_not_asked_again() {
        let (url, tx, accepted) = node().await;
        let mut h = Heads::new(&url);
        tx.send(Cmd::Error).unwrap();
        let took = timed(&mut h, Duration::from_millis(1200)).await;
        assert!(took >= Duration::from_millis(1150), "{took:?}");
        assert!(h.unsupported && h.conn.is_none());
        timed(&mut h, Duration::from_millis(300)).await;
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_node_that_never_finishes_the_handshake_does_not_stretch_the_wait() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _held = listener.accept().await;
            std::future::pending::<()>().await;
        });
        let mut h = Heads::new(&url);
        let took = timed(&mut h, Duration::from_millis(1500)).await;
        assert!(took >= Duration::from_millis(1450), "{took:?}");
        assert!(
            took < Duration::from_secs(4),
            "must not wait out the 10 s connect timeout: {took:?}"
        );
    }

    #[tokio::test]
    async fn a_close_that_follows_a_head_drops_the_connection_at_once() {
        let (url, tx, accepted) = node().await;
        let mut h = Heads::new(&url);
        tx.send(Cmd::HeadThenClose).unwrap();
        timed(&mut h, LONG).await;
        assert!(h.conn.is_none(), "a dead connection must not be kept");
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            tx.send(Cmd::Head).unwrap();
            tx
        });
        let took = timed(&mut h, LONG).await;
        let _tx = sender.await.unwrap();
        assert!(
            took < Duration::from_secs(15),
            "reconnected at once: {took:?}"
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_temporary_refusal_is_retried_but_method_not_found_is_final() {
        let (url, tx, accepted) = node().await;
        let mut h = Heads::new(&url);
        tx.send(Cmd::Busy).unwrap();
        timed(&mut h, Duration::from_millis(1100)).await;
        assert!(!h.unsupported && h.conn.is_none());
        tx.send(Cmd::Error).unwrap();
        timed(&mut h, Duration::from_millis(1100)).await;
        assert!(h.unsupported);
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            2,
            "the busy refusal was retried"
        );
    }

    #[test]
    fn heads_sent_as_binary_frames_count() {
        let head = br#"{"method":"eth_subscription","params":{}}"#.to_vec();
        assert!(matches!(
            classify(Message::Binary(head.into())),
            Heard::Head
        ));
        assert!(matches!(
            classify(Message::Text("not json".into())),
            Heard::Other
        ));
    }
}
