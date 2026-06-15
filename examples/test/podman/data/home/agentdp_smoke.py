import os
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path

SMOKE_IMAGE = "quay.io/libpod/alpine:latest"
CA_KEYS_ENV = "AGENTDP_CA_ENV_VARS"
CA_PATH = "/run/agentdp/ca/ca-bundle.pem"


def run(args, *, env=None, timeout=60, check=True):
    merged = os.environ.copy()
    if env:
        merged.update(env)
    subprocess.run(args, check=check, env=merged, timeout=timeout)


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def dump_command(args, *, env=None, timeout=15):
    log("$ " + " ".join(args))
    merged = os.environ.copy()
    if env:
        merged.update(env)
    try:
        subprocess.run(args, check=False, env=merged, timeout=timeout)
    except Exception as error:
        log(f"diagnostic command failed: {error}")


def ca_assertion(ca_path=CA_PATH):
    keys = "${" + CA_KEYS_ENV + ":?}"
    checks = "\n".join(
        [
            f'for key in $(printf "%s" "{keys}" | tr "," " "); do',
            '  eval "value=\\${$key:-}"',
            f'  test "$value" = "{ca_path}" || {{ echo "$key=$value"; exit 1; }}',
            "done",
        ]
    )
    return f"""
set -eu
test -n "{keys}"
{checks}
test -r {ca_path}
apk add --no-cache curl >/dev/null
curl -fsS https://api.nuget.org/v3/index.json >/tmp/nuget-index.json
"""


def supervise(name, worker, *, diagnostics=None, timeout=60):
    state_dir = Path(f"/tmp/agentdp-{name}-smoke")
    log_file = state_dir / "smoke.log"
    pid_file = state_dir / "worker.pid"
    result_file = state_dir / "result"
    started_file = state_dir / "started"

    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        return worker_main(worker, diagnostics, result_file)
    return healthcheck_main(state_dir, log_file, pid_file, result_file, started_file, timeout)


def worker_main(worker, diagnostics, result_file):
    try:
        worker()
        result_file.write_text("ok\n")
        log("smoke passed")
        return 0
    except Exception:
        result_file.write_text("failed\n")
        log("smoke failed")
        traceback.print_exc()
        if diagnostics:
            diagnostics()
        return 1


def healthcheck_main(state_dir, log_file, pid_file, result_file, started_file, timeout):
    if result_file.exists():
        result = read_text(result_file)
        print(f"{state_dir.name} result={result}")
        print(output_tail(log_file))
        return 0 if result == "ok" else 1

    pid_text = read_text(pid_file)
    if not pid_text:
        return start_worker(state_dir, log_file, pid_file, started_file)

    pid = int(pid_text)
    if not pid_is_running(pid):
        result_file.write_text("failed\n")
        print(f"{state_dir.name} worker pid={pid} exited without result")
        print(output_tail(log_file))
        return 1

    started = float(read_text(started_file) or "0")
    elapsed = int(time.time() - started)
    if elapsed > timeout:
        stop_worker(pid)
        result_file.write_text("failed\n")
        print(f"{state_dir.name} worker pid={pid} exceeded {timeout}s")
        print(output_tail(log_file))
        return 1

    print(f"{state_dir.name} worker pid={pid} running for {elapsed}s")
    print(output_tail(log_file, lines=25))
    return 1


def start_worker(state_dir, log_file, pid_file, started_file):
    state_dir.mkdir(parents=True, exist_ok=True)
    for path in (pid_file, started_file, state_dir / "result"):
        path.unlink(missing_ok=True)
    started_file.write_text(str(time.time()))
    with log_file.open("ab", buffering=0) as log:
        process = subprocess.Popen(
            [sys.executable, sys.argv[0], "--worker"],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    pid_file.write_text(str(process.pid))
    print(f"started {state_dir.name} worker pid={process.pid} log={log_file}")
    return 1


def read_text(path):
    return path.read_text(errors="replace").strip() if path.exists() else ""


def output_tail(path, lines=80):
    if not path.exists():
        return "<no log yet>"
    content = path.read_text(errors="replace").splitlines()
    return "\n".join(content[-lines:]) if content else "<empty log>"


def pid_is_running(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def stop_worker(pid):
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    time.sleep(2)
    if pid_is_running(pid):
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
