#!/usr/bin/env python3
"""Send signed UDP records at a fixed rate, then count what the daemon stored.

Every record's message is "seq=<n>", so the check reports records lost, duplicated and any that
arrived out of nothing. Usage: udp_load.py JWT APP RATE SECONDS [PER_DATAGRAM]
Prints one line: sent=<n> stored=<n> lost=<n> duplicated=<n> lost_pct=<p> rate=<achieved>/s
"""
import hashlib
import hmac
import json
import re
import struct
import sys
import time
import urllib.request
import socket
import os
import signal
import subprocess

QUERY = "http://127.0.0.1:9200"
INGEST = ("127.0.0.1", 9100)
WINDOW_MS = 50
API_LIMIT = 10000


def call(path, token=None, body=None):
    req = urllib.request.Request(QUERY + path, data=json.dumps(body).encode() if body else None)
    if body:
        req.add_header("content-type", "application/json")
    if token:
        req.add_header("authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def kill_and_restart():
    """kill -9 the daemon, which stops the load too: what was sent before is what is checked."""
    out = subprocess.run([os.environ["LH_BIN"], "status"], capture_output=True, text=True).stdout
    pid = int(re.search(r"PID:?\s*(\d+)", out).group(1))
    os.kill(pid, signal.SIGKILL)
    time.sleep(1)
    subprocess.Popen([os.environ["LH_BIN"], "run"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    for _ in range(100):
        try:
            urllib.request.urlopen(QUERY + "/dashboard", timeout=1)
            return
        except Exception:
            time.sleep(0.2)
    sys.exit("the daemon did not come back after kill -9")


def send(jwt, app, rate, seconds, per_datagram):
    session = call("/auth", body={"jwt": jwt})
    secret = bytes.fromhex(session["secret_hex"])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
    counter = 0
    seq = 0
    start = time.time()
    first_ms = int(start * 1000) - 1000
    tick = 0.01
    due = 0.0
    kill_at = float(os.environ.get("LH_KILL_AT", "0"))
    while time.time() - start < seconds:
        if kill_at and time.time() - start >= kill_at:
            kill_and_restart()
            return seq, time.time() - start, first_ms, int(time.time() * 1000) + 1000
        due += rate * tick
        while seq < int(due):
            n = min(per_datagram, int(due) - seq)
            records = [
                {"level": "info", "source": "load", "message": "seq=%d" % (seq + i)}
                for i in range(n)
            ]
            payload = json.dumps({"app": app, "records": records}).encode()
            counter += 1
            mac = hmac.new(secret, struct.pack(">Q", counter) + payload, hashlib.sha256).hexdigest()
            sock.sendto(("%s:%d:%s\n" % (session["token_id"], counter, mac)).encode() + payload, INGEST)
            seq += n
        time.sleep(max(0.0, start + (round((time.time() - start) / tick) + 1) * tick - time.time()))
    return seq, time.time() - start, first_ms, int(time.time() * 1000) + 1000


def fetch(token, app, first_ms, last_ms):
    seen = {}
    t = first_ms
    while t < last_ms:
        q = "/logs?app=%s&from_ms=%d&to_ms=%d&limit=%d" % (app, t, t + WINDOW_MS - 1, API_LIMIT)
        recs = call(q, token)["records"]
        if len(recs) >= API_LIMIT:
            sys.exit("window of %d ms holds %d records, the API limit: narrow WINDOW_MS" % (WINDOW_MS, len(recs)))
        for r in recs:
            m = re.fullmatch(r"seq=(\d+)", r["message"])
            if m:
                seen[int(m.group(1))] = seen.get(int(m.group(1)), 0) + 1
        t += WINDOW_MS
    return seen


def main():
    jwt, app, rate, seconds = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
    per = int(sys.argv[5]) if len(sys.argv) > 5 else 1
    sent, took, first_ms, last_ms = send(jwt, app, rate, seconds, per)
    time.sleep(3)
    token = call("/auth", body={"jwt": jwt})["token_id"]
    seen = fetch(token, app, first_ms, last_ms)
    stored = len(seen)
    dup = sum(c - 1 for c in seen.values())
    lost = sent - stored
    print("sent=%d stored=%d lost=%d duplicated=%d lost_pct=%.2f rate=%d/s" % (
        sent, stored, lost, dup, 100.0 * lost / max(sent, 1), sent / took))


main()
