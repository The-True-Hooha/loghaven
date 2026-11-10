#!/usr/bin/env python3
"""Fake Ethereum JSON-RPC node for manually testing `loghaven watch ethereum`.

Usage: scripts/mock_eth_node.py STATE_DIR
Listens on 127.0.0.1:18545. It emits one log for the watched address at every
block divisible by 10. Behaviour is controlled by files in STATE_DIR, read on
every request, so you can change it while the watcher runs:

  head      chain head block number (default 1000)
  chainid   chain id (default 1)
  maxspan   reject eth_getLogs spans of this many blocks or more
  reject    "1" rejects every eth_getLogs call
  big       "1" makes event data 80000 bytes
  removed   "1" marks logs on blocks divisible by 20 as removed
  nulltime  "1" makes eth_getBlockByNumber return null
  lastfilter.json  (written) the last eth_getLogs filter received
"""

import json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
ADDR="0x"+"aa"*20; TOPIC="0x"+"dd"*32
def tx(n): return "0x"+("%064x"%n)
STATE=sys.argv[1]
def rd(name, default=''):
    try: return open(os.path.join(STATE,name)).read().strip()
    except OSError: return default
class H(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def do_POST(self):
        req=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        m,p=req['method'],req['params']; err=None; res=None
        if m=='eth_blockNumber': res=hex(int(rd('head','1000')))
        elif m=='eth_chainId': res=hex(int(rd('chainid','1')))
        elif m=='eth_getLogs':
            open(os.path.join(STATE,'lastfilter.json'),'w').write(json.dumps(p[0]))
            f=int(p[0]['fromBlock'],16); t=int(p[0]['toBlock'],16)
            ms=int(rd('maxspan','0') or 0)
            if rd('reject')=='1': err="query returned more than 10000 results"
            elif ms and t-f>=ms: err="query returned more than 10000 results"
            else:
                big="0x"+"ab"*40000 if rd('big')=='1' else "0x01"
                rem=rd('removed')=='1'
                res=[{"address":ADDR,"topics":[TOPIC],"data":big,"blockNumber":hex(b),"logIndex":"0x0","transactionHash":tx(b),"removed":rem and b%20==0} for b in range(f,t+1) if b%10==0]
        elif m=='eth_getBlockByNumber': res=None if rd('nulltime')=='1' else {"timestamp":hex(1700000000+int(p[0],16))}
        body={"jsonrpc":"2.0","id":1}
        if err: body["error"]={"code":-32005,"message":err}
        else: body["result"]=res
        self.send_response(200); self.send_header('Content-Type','application/json'); self.end_headers()
        self.wfile.write(json.dumps(body).encode())
HTTPServer(('127.0.0.1',18545),H).serve_forever()
