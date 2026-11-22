#!/usr/bin/env python3
"""Minimal Azure Blob REST client (shared key) for the Azurite tests: create a container, list
blobs under a prefix, download one blob.

Usage: azure_rest.py <endpoint> <account> <key> create <container>
       azure_rest.py <endpoint> <account> <key> list <container> <prefix>
       azure_rest.py <endpoint> <account> <key> get <container> <blob> <outfile>
       azure_rest.py <endpoint> <account> <key> sas <container>    # prints an account SAS token
"""
import base64, datetime, hashlib, hmac, sys, urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET

VERSION = "2021-08-06"


def sign(key, text):
    return base64.b64encode(hmac.new(base64.b64decode(key), text.encode(), hashlib.sha256).digest()).decode()


def request(endpoint, account, key, verb, path, query=None, headers=None):
    query = query or {}
    headers = dict(headers or {})
    headers["x-ms-date"] = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    headers["x-ms-version"] = VERSION
    canon_headers = "".join(f"{k.lower()}:{v}\n" for k, v in sorted(headers.items()) if k.lower().startswith("x-ms-"))
    base_path = urllib.parse.urlparse(endpoint).path.rstrip("/")
    resource = f"/{account}{base_path}{path}"
    if base_path == "":
        resource = f"/{account}{path}"
    canon_resource = resource + "".join(f"\n{k.lower()}:{v}" for k, v in sorted(query.items()))
    to_sign = f"{verb}\n\n\n\n\n\n\n\n\n\n\n\n{canon_headers}{canon_resource}"
    headers["Authorization"] = f"SharedKey {account}:{sign(key, to_sign)}"
    url = endpoint.rstrip("/") + path + ("?" + urllib.parse.urlencode(query) if query else "")
    req = urllib.request.Request(url, method=verb, headers=headers)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def account_sas(account, key):
    exp = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # string to sign: account, permissions, services, resource types, start, expiry, ip, protocol, version, encryption scope
    to_sign = "\n".join([account, "rwdlacup", "b", "sco", "", exp, "", "https,http", VERSION, ""]) + "\n"
    sig = sign(key, to_sign)
    q = {"sv": VERSION, "ss": "b", "srt": "sco", "sp": "rwdlacup", "se": exp, "spr": "https,http", "sig": sig}
    return urllib.parse.urlencode(q)


def main():
    endpoint, account, key, cmd, *rest = sys.argv[1:]
    if cmd == "create":
        code, body = request(endpoint, account, key, "PUT", f"/{rest[0]}", {"restype": "container"})
        print(code)
        sys.exit(0 if code in (201, 409) else 1)
    if cmd == "list":
        container, prefix = rest
        code, body = request(endpoint, account, key, "GET", f"/{container}", {"restype": "container", "comp": "list", "prefix": prefix})
        if code != 200:
            print(body.decode(), file=sys.stderr)
            sys.exit(1)
        for n in ET.fromstring(body).iter("Name"):
            print(n.text)
        return
    if cmd == "get":
        container, blob, out = rest
        code, body = request(endpoint, account, key, "GET", f"/{container}/{blob}")
        if code != 200:
            sys.exit(1)
        open(out, "wb").write(body)
        return
    if cmd == "sas":
        print(account_sas(account, key))
        return
    sys.exit(2)


main()
