#!/usr/bin/env python3
"""Fake Solana JSON-RPC node for manually testing `loghaven watch solana`.

Usage: scripts/mock_sol_node.py STATE_DIR
Listens on 127.0.0.1:18899. Transactions 1..tip all touch the watched address;
every 7th one failed. Behaviour is controlled by files in STATE_DIR, read on
every request, so you can change it while the watcher runs:

  tip       highest transaction number (default 100)
  genesis   cluster genesis hash (default the mainnet-beta one)
  nulltx    "1" makes getTransaction return null (pruned)
  big       "1" gives every transaction 300 log lines of 600 characters
  fail      "1" makes getTransaction return an error

Watched address: So11111111111111111111111111111111111111112
A transaction's signature is 64 'S' followed by its number, digits written as a..j.
"""

import json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

ADDR = "So11111111111111111111111111111111111111112"
STATE = sys.argv[1]


def rd(name, default=""):
    try:
        return open(os.path.join(STATE, name)).read().strip()
    except OSError:
        return default


def sig(n):
    return "S" * 64 + "".join(chr(ord("a") + int(d)) for d in str(n))


def num(s):
    return int("".join(str(ord(c) - ord("a")) for c in s[64:]))


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        m, p = req["method"], req["params"]
        err = res = None
        tip = int(rd("tip", "100"))
        if m == "getGenesisHash":
            res = rd("genesis", "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d")
        elif m == "getSlot":
            res = 1000 + tip
        elif m == "getSignaturesForAddress":
            o = p[1]
            ns = list(range(tip, 0, -1)) if p[0] == ADDR else []
            if "before" in o:
                ns = [n for n in ns if n < num(o["before"])]
            if "until" in o:
                ns = [n for n in ns if n > num(o["until"])]
            res = [
                {
                    "signature": sig(n),
                    "slot": 1000 + n,
                    "err": {"InstructionError": [0, "Custom"]} if n % 7 == 0 else None,
                    "blockTime": 1700000000 + n,
                    "confirmationStatus": "finalized",
                }
                for n in ns[: o["limit"]]
            ]
        elif m == "getTransaction":
            n = num(p[0])
            if rd("fail") == "1":
                err = "boom"
            elif rd("nulltx") == "1":
                res = None
            else:
                big = rd("big") == "1"
                logs = ["Program log: %d " % n + "x" * 600] * 300 if big else [
                    "Program X invoke [1]", "Program log: tx %d" % n]
                res = {"slot": 1000 + n, "meta": {"err": None, "fee": 5000,
                       "computeUnitsConsumed": 1200, "logMessages": logs}}
        else:
            err = "unknown method"
        body = {"jsonrpc": "2.0", "id": req["id"]}
        if err:
            body["error"] = {"code": -32000, "message": err}
        else:
            body["result"] = res
        out = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


HTTPServer(("127.0.0.1", 18899), H).serve_forever()
