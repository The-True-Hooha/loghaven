#!/usr/bin/env python3
"""`loghaven tui` in a pseudo-terminal against a live daemon: it must redraw after a resize, show
a polling error while the daemon is down, and show new records once the daemon is back.

Usage: tui_pty.py LOGHAVEN_BINARY APP   (the daemon runs; HOME is the isolated one)
Prints PASS/FAIL lines; exit 1 on any FAIL.
"""
import fcntl, os, pty, re, select, signal, struct, subprocess, sys, termios, time, urllib.request

binary, app = sys.argv[1:3]
ok_all = True


def check(ok, what):
    global ok_all
    ok_all &= bool(ok)
    print(("PASS " if ok else "FAIL ") + what, flush=True)
    return ok


def size(fd, rows, cols):
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


def drain(fd, until=None, seconds=15):
    """Read until `until` (bytes) shows in what was read since the call, or time runs out."""
    out = b""
    end = time.time() + seconds
    while time.time() < end and (until is None or until not in out):
        if select.select([fd], [], [], 0.2)[0]:
            try:
                out += os.read(fd, 65536)
            except OSError:
                break
    return out


def up():
    try:
        urllib.request.urlopen("http://127.0.0.1:9200/dashboard", timeout=1)
        return True
    except Exception:
        return False


def daemon_pid():
    out = subprocess.run([binary, "status"], capture_output=True, text=True).stdout
    return int(re.search(r"PID:?\s*(\d+)", out).group(1))


def send(msg):
    subprocess.run([binary, "send", app, msg], capture_output=True)


send("first record")
pid, fd = pty.fork()
if pid == 0:
    os.environ["TERM"] = "xterm-256color"
    os.execv(binary, [binary, "tui", "--app", app])
size(fd, 30, 120)
out = drain(fd, b"first record")
check(b"first record" in out, "the record is drawn at 120x30")

size(fd, 12, 50)
os.kill(pid, signal.SIGWINCH)
out = drain(fd, b"first record", 5)
check(b"first record" in out, "the screen is redrawn after shrinking to 50x12")
size(fd, 40, 160)
os.kill(pid, signal.SIGWINCH)
out = drain(fd, b"first record", 5)
check(b"first record" in out, "the screen is redrawn after growing to 160x40")

quiet = drain(fd, None, 3)
os.kill(daemon_pid(), signal.SIGKILL)
time.sleep(1)
out = drain(fd, None, 8)
ERR = rb"cannot reach|refused|Connection|error sending"
check(not re.search(ERR, quiet, re.I), "no polling error is shown while the daemon is up")
check(re.search(ERR, out, re.I), "a polling error is shown while the daemon is down")
check(os.waitpid(pid, os.WNOHANG) == (0, 0), "the TUI is still running while the daemon is down")

subprocess.Popen([binary, "run"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
for _ in range(100):
    if up():
        break
    time.sleep(0.2)
send("after the restart")
out = drain(fd, b"after the restart", 30)
check(b"after the restart" in out, "a record sent after the restart is drawn: the TUI reconnected")

os.write(fd, b"q")
end = time.time() + 5
status = None
while time.time() < end and status is None:
    drain(fd, None, 0.3)
    done, st = os.waitpid(pid, os.WNOHANG)
    if done:
        status = st
if status is None:
    os.kill(pid, 9)
check(status is not None and os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, "exit status 0 after q")
sys.exit(0 if ok_all else 1)
