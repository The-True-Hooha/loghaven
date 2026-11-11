#!/usr/bin/env python3
"""Runs `loghaven tui` in a pseudo-terminal: it must draw the records, enter and leave the
alternate screen, show the record text neutralised, and exit 0 on `q`.

Usage: tui_e2e.py LOGHAVEN_BINARY APP EXPECTED_TEXT
"""
import os, pty, select, sys, time

binary, app, expected = sys.argv[1:4]
pid, fd = pty.fork()
if pid == 0:
    os.environ["TERM"] = "xterm-256color"
    os.execv(binary, [binary, "tui", "--app", app])

import fcntl, struct, termios
fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 30, 120, 0, 0))
out = b""
deadline = time.time() + 15
while time.time() < deadline and expected.encode() not in out:
    if select.select([fd], [], [], 0.2)[0]:
        try:
            out += os.read(fd, 65536)
        except OSError:
            break
os.write(fd, b"q")
status = None
end = time.time() + 5
while time.time() < end and status is None:
    if select.select([fd], [], [], 0.1)[0]:
        try:
            out += os.read(fd, 65536)
        except OSError:
            pass
    done, st = os.waitpid(pid, os.WNOHANG)
    if done:
        status = st
if status is None:
    os.kill(pid, 9)
    sys.exit("tui did not exit after q")

def check(ok, what):
    print(("PASS " if ok else "FAIL ") + what)
    return ok

results = [
    check(expected.encode() in out, "record text drawn"),
    check(b"\x1b[?1049h" in out, "entered the alternate screen"),
    check(b"\x1b[?1049l" in out, "left the alternate screen"),
    check(b"\x1b[2J\x1b]0;pwned" not in out, "escape sequence from a record was not passed through"),
    check(os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, "exit status 0 after q"),
]
sys.exit(0 if all(results) else 1)
