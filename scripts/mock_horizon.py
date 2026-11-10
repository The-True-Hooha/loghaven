#!/usr/bin/env python3
"""Fake Stellar Horizon server for manually testing `loghaven watch stellar`.

Usage: scripts/mock_horizon.py STATE_DIR
Listens on 127.0.0.1:18000. Transactions 1..tip all involve the watched account;
every 7th failed. Behaviour is controlled by files in STATE_DIR, read on every
request, so you can change it while the watcher runs:

  tip         highest transaction number (default 100)
  passphrase  network passphrase (default the testnet one)
  status      an HTTP status code to answer every request with
  bigmemo     "1" gives every transaction a 5000-character memo

Watched account: GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7
Transaction n has hash %064x of n and paging token n * 4096.
"""

import json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

ACCOUNT = "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"
STATE = sys.argv[1]


def rd(name, default=""):
    try:
        return open(os.path.join(STATE, name)).read().strip()
    except OSError:
        return default


def tx(n):
    memo = "x" * 5000 if rd("bigmemo") == "1" else "memo %d" % n
    return {
        "hash": "%064x" % n,
        "paging_token": str(n * 4096),
        "ledger": 100 + n,
        "created_at": "2024-01-01T00:00:00Z",
        "successful": n % 7 != 0,
        "fee_charged": "100",
        "operation_count": 1,
        "source_account": ACCOUNT,
        "memo_type": "text",
        "memo": memo,
    }


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def reply(self, code, body):
        out = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if rd("status"):
            return self.reply(int(rd("status")), {"status": int(rd("status"))})
        if u.path == "/":
            return self.reply(200, {"network_passphrase": rd("passphrase", "Test SDF Network ; September 2015")})
        if u.path != "/accounts/%s/transactions" % ACCOUNT:
            return self.reply(404, {"status": 404})
        ns = list(range(1, int(rd("tip", "100")) + 1))
        if "cursor" in q:
            ns = [n for n in ns if n * 4096 > int(q["cursor"])]
        if q.get("order") == "desc":
            ns.reverse()
        ns = ns[: min(int(q.get("limit", "10")), 200)]
        self.reply(200, {"_embedded": {"records": [tx(n) for n in ns]}})


HTTPServer(("127.0.0.1", 18000), H).serve_forever()
