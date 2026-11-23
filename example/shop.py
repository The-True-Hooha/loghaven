#!/usr/bin/env python3
"""A tiny shop that logs to a local LogHaven daemon over signed UDP, using only the standard library.

Each order writes a few records. Paid orders also record the on-chain payment's transaction hash, so
`loghaven trace <hash> --app shop --app payments` shows the shop's side and the chain's side together.

Usage: shop.py JWT [ORDERS]
"""
import hashlib, hmac, json, random, socket, struct, sys, time, urllib.request

QUERY = "http://127.0.0.1:9200"
INGEST = ("127.0.0.1", 9100)


def open_session(jwt):
    req = urllib.request.Request(QUERY + "/auth", data=json.dumps({"jwt": jwt}).encode(),
                                 headers={"content-type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=10))


class Logger:
    def __init__(self, session):
        self.id, self.secret = session["token_id"], bytes.fromhex(session["secret_hex"])
        self.counter = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, app, level, message, **fields):
        record = {"level": level, "source": "shop.py", "message": message, **fields}
        payload = json.dumps({"app": app, "records": [record]}).encode()
        self.counter += 1
        mac = hmac.new(self.secret, struct.pack(">Q", self.counter) + payload, hashlib.sha256).hexdigest()
        self.sock.sendto(f"{self.id}:{self.counter}:{mac}\n".encode() + payload, INGEST)


def main():
    jwt, orders = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 20
    # one session per app: a token is scoped to the app it was minted for
    shop = Logger(open_session(jwt))
    rng = random.Random(7)
    for n in range(1, orders + 1):
        trace = f"order-{n}"
        shop.send("shop", "info", f"order {n} placed", trace_id=trace, tags={"items": rng.randint(1, 5)})
        if rng.random() < 0.15:
            shop.send("shop", "error", f"order {n}: card declined", trace_id=trace)
            continue
        tx = "0x" + hashlib.sha256(f"payment-{n}".encode()).hexdigest()
        shop.send("shop", "info", f"order {n} paid", trace_id=trace, chain="ethereum", tx_hash=tx)
        if rng.random() < 0.2:
            shop.send("shop", "warn", f"order {n}: payment took longer than usual", trace_id=trace, tx_hash=tx)
        time.sleep(0.05)
    print(f"sent records for {orders} orders")


main()
