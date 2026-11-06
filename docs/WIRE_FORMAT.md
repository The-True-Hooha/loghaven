# LogHaven wire format

How to send logs to a LogHaven daemon from any language. The Rust reference
implementation is `src/client/mod.rs`.

## Endpoints (all loopback only)

| Purpose | Address | Default |
|---------|---------|---------|
| Ingest | UDP `127.0.0.1:<daemon.ingest_port>` | 9100 |
| Auth and query | HTTP `127.0.0.1:<auth.query_port>` | 9200 |

## 1. Get a session (when `auth.require_auth = true`, the default)

1. Create a JWT signed RS256 with the daemon's private key (`loghaven auth token <app>` prints one). Claims: `sub` = app name, `iat`, `exp` (unix seconds; no clock leeway).
2. `POST /auth` with body `{"jwt": "<jwt>"}`. Response:

```json
{"token_id": "<64 hex chars>", "secret_hex": "<64 hex chars = 32 bytes>", "expires_in_secs": 3600}
```

The session is bound to the JWT's `sub` (the app) and lives for the shorter of
`auth.session_ttl_secs` and the JWT's remaining life. At most 32 live sessions
exist per app; the oldest is evicted. Sessions are in memory only: a daemon
restart invalidates them.

The daemon reads `public.pem` once at startup. After `auth keygen`, restart it.

Anyone who can read `private.pem` can mint a token for any app, so the key is
created with mode 0600 in a 0700 directory. `loghaven send`, `loghaven logs`
(without `--token`) and `loghaven auth token` all read this key: run them as the
user that owns it.

## 2. Datagram

```
<token_id>:<counter>:<hmac_hex>\n<payload bytes>
```

- `counter`: decimal u64, starts at 1, must be unique per session and below `2^64-1`. A counter is accepted once; counters up to 64 behind the highest seen are still accepted (UDP reordering), older ones are dropped.
- `hmac_hex`: lowercase hex of `HMAC-SHA256(secret, counter as 8 bytes big-endian || payload)`, where `secret` is the decoded `secret_hex`.
- The whole datagram must fit in one UDP packet (65507 bytes; on macOS the default `net.inet.udp.maxdgram` limits it to 9216, and the Rust client uses 9000). Keep payloads under about 65300 bytes, or under 9000 on macOS. Sending thousands of datagrams back to back can overflow the daemon's socket buffer and drop some silently; pace large bursts.

With `auth.require_auth = false` (dev only) the datagram is just the payload, with no header.

## 3. Payload

JSON. The daemon tries JSON first; only if that fails and the first byte is not `{` or `[` does it try MessagePack with the same structure:

```json
{
  "app": "web",
  "records": [
    {
      "level": "info",
      "source": "checkout",
      "message": "payment confirmed",
      "trace_id": "optional",
      "span_id": "optional",
      "chain": "ethereum",
      "tx_hash": "0x...",
      "tags": ["any", "json"],
      "metadata": {"any": "json"}
    }
  ]
}
```

`level`, `source`, `message` are required; the rest are optional. `app` must
equal the session's app and match `[a-z0-9._-]{1,64}` (no leading or trailing
dot, no Windows device names such as `con`). The daemon assigns the id and the
timestamp when it receives the datagram.

## Delivery semantics

UDP is fire-and-forget: there is no acknowledgement. Invalid, replayed,
cross-app or unsigned datagrams are dropped silently (the daemon logs a
rate-limited line). Accepted records are buffered and written to disk on the
next flush (`storage.local.flush_interval_secs`, default 30 s) and are
queryable after that.

## Querying

`GET /logs?app=<app>&level=&source=&from_ms=&to_ms=&text=&tx_hash=&limit=` with header
`Authorization: Bearer <token_id>` when auth is on. The token must belong to
the same app. `limit` is 1..=10000 (default 1000); results are newest first.
`text` is a case-insensitive substring match on message, tags and metadata.
`tx_hash` is an exact match, except that a `0x`+hex value ignores ASCII case; an empty `tx_hash` is a 400.
