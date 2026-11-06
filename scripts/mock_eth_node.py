#!/usr/bin/env python3
"""Fake Ethereum JSON-RPC node for manually testing `loghaven watch ethereum`.

Usage: scripts/mock_eth_node.py STATE_DIR
Listens on 127.0.0.1:18545. It emits one log for the watched address at every
block divisible by 10. Behaviour is controlled by files in STATE_DIR, read on
every request, so you can change it while the watcher runs:

  head      chain head block number (default 1000)
  maxspan   reject eth_getLogs spans of this many blocks or more
  reject    "1" rejects every eth_getLogs call
  big       "1" makes event data 80000 bytes
  removed   "1" marks logs on blocks divisible by 20 as removed
  nulltime  "1" makes eth_getBlockByNumber return null
  lastfilter.json  (written) the last eth_getLogs filter received
"""
