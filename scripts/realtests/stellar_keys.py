#!/usr/bin/env python3
"""Prints N valid Stellar account ids (G...). The 32 key bytes are random; the network accepts any
32 bytes as the key of a new account, which is all a watcher test needs."""
import base64, os, sys


def crc16_xmodem(data):
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


for _ in range(int(sys.argv[1])):
    payload = bytes([6 << 3]) + os.urandom(32)
    crc = crc16_xmodem(payload)
    print(base64.b32encode(payload + crc.to_bytes(2, "little")).decode().rstrip("="))
