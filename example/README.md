# Example: a shop that logs to LogHaven

A small Python program (`shop.py`, standard library only) that sends records to a LogHaven daemon over signed UDP, and a script (`demo.sh`) that runs the whole thing on your machine.

**Nothing is deployed.** LogHaven is a local daemon. `demo.sh` starts one inside `example/.demo-home` (its own config, keys and data), sends some records to it, queries them, and stops it when you press Enter. It touches nothing outside this folder (plus a socket file in your temp directory) and sends nothing over the network. It needs `python3`, a `loghaven` binary, and free local ports 9100 (UDP) and 9200 (TCP), so stop any other daemon first.

## Run it

```
cargo build                      # or download a release binary
example/demo.sh                  # uses `loghaven` on your PATH, else target/debug/loghaven
example/demo.sh /path/to/loghaven
```

It then:

1. logs 20 orders (`shop.py`): every order writes a record with a `trace_id`; paid orders also carry `chain=ethereum` and a `tx_hash`; some are declined (`error`) or slow (`warn`);
2. lists the errors and warnings with `loghaven logs`;
3. follows one payment by its transaction hash with `loghaven trace`;
4. prints a dashboard URL. Open it in a browser while the script waits. In another terminal you can also run the terminal UI, whose command the script prints.

## How the app talks to the daemon

`shop.py` shows the whole protocol in about 40 lines:

1. get a short-lived JWT for the app (`loghaven auth token shop`; the demo passes it in);
2. `POST /auth` on the query port with the JWT to get a session (`token_id` and a secret);
3. send each datagram to `127.0.0.1:9100` as `token_id:counter:hmac\n` followed by the JSON payload, where the HMAC-SHA256 covers the counter (8 bytes, big endian) and the payload, and the counter must increase.

In your own app you would more likely shell out to `loghaven send` or wrap those steps in a small client for your language. A token is scoped to one app name.

## Try more

- Point it at a store: add a `[[storage.targets]]` block to `.demo-home/.../config.toml` (see [docs/CONFIGURATION.md](../docs/CONFIGURATION.md)); a local MinIO container works without an account.
- Watch a chain: set `[chains.ethereum]` and run `loghaven watch ethereum` with the same `HOME`, then trace one of its transaction hashes across `shop` and `ethereum`.
