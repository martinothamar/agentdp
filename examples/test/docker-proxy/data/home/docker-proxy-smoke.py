#!/usr/bin/env python3
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

CA_KEYS_ENV = "AGENTDP_CA_ENV_VARS"
SMOKE_IMAGE = "quay.io/libpod/alpine:latest"
DOCKER_IMAGE = "agentdp/docker-proxy-smoke:latest"
DOCKER_BUILDKIT_IMAGE = "agentdp/docker-proxy-buildkit-smoke:latest"
DOCKER_BUILDX_IMAGE = "agentdp/docker-proxy-buildx-smoke:latest"
DOCKER_SOURCE_ANCHOR_IMAGE = "agentdp/docker-proxy-source-anchor-smoke:latest"
DOCKER_COMPOSE_IMAGE = "agentdp/docker-proxy-compose-smoke:latest"
STATE_DIR = Path("/tmp/agentdp-docker-proxy-smoke")
LOG_FILE = STATE_DIR / "smoke.log"
PID_FILE = STATE_DIR / "worker.pid"
RESULT_FILE = STATE_DIR / "result"
STARTED_FILE = STATE_DIR / "started"
WORKER_TIMEOUT_SECONDS = 60


def log(message):
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def run(args, *, env=None, timeout=60, check=True):
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.run(args, check=check, env=merged, timeout=timeout)


def output(args, *, env=None, timeout=60):
    merged = os.environ.copy()
    if env:
        merged.update(env)
    return subprocess.check_output(args, env=merged, timeout=timeout, text=True)


def output_tail(path, lines=80):
    if not path.exists():
        return "<no log yet>"
    content = path.read_text(errors="replace").splitlines()
    return "\n".join(content[-lines:]) if content else "<empty log>"


def read_text(path):
    return path.read_text(errors="replace").strip() if path.exists() else ""


def pid_is_running(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def dump_command(args, *, timeout=10):
    log("$ " + " ".join(args))
    try:
        subprocess.run(args, check=False, timeout=timeout)
    except Exception as error:
        log(f"diagnostic command failed: {error}")


def dump_diagnostics():
    log("diagnostics: systemd units")
    dump_command(
        [
            "sudo",
            "-n",
            "systemctl",
            "--no-pager",
            "--full",
            "status",
            "docker.service",
            "docker.socket",
            "agentdp-docker-proxy.socket",
            "agentdp-docker-proxy.service",
        ]
    )
    log("diagnostics: recent docker/proxy journal")
    dump_command(
        [
            "sudo",
            "-n",
            "journalctl",
            "--no-pager",
            "-u",
            "docker.service",
            "-u",
            "docker.socket",
            "-u",
            "agentdp-docker-proxy.socket",
            "-u",
            "agentdp-docker-proxy.service",
            "-n",
            "160",
        ],
        timeout=20,
    )
    log("diagnostics: sockets")
    dump_command(["sh", "-lc", 'sudo -n ss -xlpn | grep -E "docker|agentdp" || true'])
    dump_command(["sh", "-lc", "ls -l /run/docker.sock /run/agentdp/docker/docker.sock 2>&1 || true"])
    log("diagnostics: docker state")
    dump_command(["docker", "ps", "-a"], timeout=20)
    dump_command(["docker", "images"], timeout=20)


def ca_assertion_commands(expected):
    keys = "${" + CA_KEYS_ENV + ":?}"
    return [
        "set -eu",
        f'test -n "{keys}"',
        f'for key in $(printf "%s" "{keys}" | tr "," " "); do eval "value=\\${{$key:-}}"; test "$value" = "{expected}" || {{ echo "$key=$value"; exit 1; }}; done',
        f"test -r {expected}",
        "apk add --no-cache curl >/dev/null",
        "curl -fsS https://api.nuget.org/v3/index.json >/tmp/nuget-index.json",
    ]


def ca_assertion(expected):
    return "\n".join(ca_assertion_commands(expected)) + "\n"


def dockerfile_ca_assertion(expected):
    return "; \\\n    ".join(ca_assertion_commands(expected))


def unset_ca_env_command():
    keys = "${" + CA_KEYS_ENV + ":-}"
    return f'for key in $(printf "%s" "{keys}" | tr "," " "); do unset "$key"; done; unset {CA_KEYS_ENV}'


def system_ca_assertion_commands():
    return [
        "set -eu",
        "apk add --no-cache ca-certificates curl >/dev/null",
        "rm -f /etc/ssl/certs/ca-certificates.crt",
        "update-ca-certificates >/dev/null",
        unset_ca_env_command(),
        "curl -fsS https://api.nuget.org/v3/index.json >/tmp/nuget-index.json",
    ]


def system_ca_assertion():
    return "\n".join(system_ca_assertion_commands())


def dockerfile_system_ca_assertion():
    return "; \\\n    ".join(system_ca_assertion_commands())


def docker_api(env, engine):
    name = f"agentdp-{engine}-create-smoke"
    run(["docker", "rm", "-f", name], env=env, check=False)

    create_script = f"""
set -eu
test "$AGENTDP_SMOKE" = create
grep -q agentdp-create.local /etc/hosts
test "$PWD" = /tmp
{ca_assertion("/run/agentdp/ca/ca-bundle.pem")}
"""
    cid = output(
        [
            "docker",
            "create",
            "--name",
            name,
            "--label",
            f"agentdp.smoke={engine}",
            "--env",
            "AGENTDP_SMOKE=create",
            "--add-host",
            "agentdp-create.local:127.0.0.1",
            "--workdir",
            "/tmp",
            SMOKE_IMAGE,
            "sh",
            "-lc",
            create_script,
        ],
        env=env,
    ).strip()
    inspected = output(
        [
            "docker",
            "inspect",
            cid,
            "--format",
            "{{json .Config.Labels}} {{json .Config.Env}} {{json .HostConfig.ExtraHosts}} {{.Config.WorkingDir}}",
        ],
        env=env,
    )
    assert "agentdp.smoke" in inspected
    run(["docker", "start", "-a", cid], env=env)
    run(["docker", "rm", cid], env=env)

    run_script = f"""
set -eu
test "$AGENTDP_SMOKE" = run
grep -q agentdp-run.local /etc/hosts
test "$PWD" = /tmp
{ca_assertion("/run/agentdp/ca/ca-bundle.pem")}
"""
    run(
        [
            "docker",
            "run",
            "--rm",
            "--name",
            f"agentdp-{engine}-run-smoke",
            "--label",
            f"agentdp.smoke={engine}",
            "--env",
            "AGENTDP_SMOKE=run",
            "--add-host",
            "agentdp-run.local:127.0.0.1",
            "--workdir",
            "/tmp",
            SMOKE_IMAGE,
            "sh",
            "-lc",
            run_script,
        ],
        env=env,
    )


def docker_builds():
    with tempfile.TemporaryDirectory() as workdir:
        Path(workdir, "Dockerfile").write_text(
            f"""
FROM {SMOKE_IMAGE} AS build
RUN {dockerfile_ca_assertion("/tmp/agentdp-ca-bundle.crt")}
FROM scratch
COPY --from=build /etc/passwd /passwd
""".lstrip()
        )
        log("docker classic build")
        run(
            ["docker", "build", "-t", DOCKER_IMAGE, workdir],
            env={"DOCKER_BUILDKIT": "0"},
            timeout=45,
        )
        run(["docker", "image", "rm", DOCKER_IMAGE])
        log("docker buildkit build")
        run(
            [
                "docker",
                "build",
                "--no-cache",
                "--progress=plain",
                "--provenance=false",
                "--sbom=false",
                "-t",
                DOCKER_BUILDKIT_IMAGE,
                workdir,
            ],
            env={"DOCKER_BUILDKIT": "1"},
            timeout=60,
        )
        run(["docker", "image", "rm", DOCKER_BUILDKIT_IMAGE])
        log("docker buildx build")
        run(
            [
                "docker",
                "buildx",
                "build",
                "--load",
                "--no-cache",
                "--progress=plain",
                "--provenance=false",
                "--sbom=false",
                "-t",
                DOCKER_BUILDX_IMAGE,
                workdir,
            ],
            timeout=60,
        )
        run(["docker", "image", "rm", DOCKER_BUILDX_IMAGE])


def docker_source_anchor_build():
    with tempfile.TemporaryDirectory() as workdir:
        Path(workdir, "Dockerfile").write_text(
            f"""
FROM {SMOKE_IMAGE} AS build
RUN {dockerfile_system_ca_assertion()}
FROM scratch
COPY --from=build /tmp/nuget-index.json /nuget-index.json
""".lstrip()
        )
        log("docker source-anchor build")
        run(
            [
                "docker",
                "build",
                "--no-cache",
                "--progress=plain",
                "--provenance=false",
                "--sbom=false",
                "-t",
                DOCKER_SOURCE_ANCHOR_IMAGE,
                workdir,
            ],
            timeout=60,
        )
        run(["docker", "image", "rm", DOCKER_SOURCE_ANCHOR_IMAGE])


def docker_compose_build():
    with tempfile.TemporaryDirectory() as workdir:
        Path(workdir, "Dockerfile").write_text(
            f"""
FROM {SMOKE_IMAGE}
RUN {dockerfile_ca_assertion("/tmp/agentdp-ca-bundle.crt")}
CMD ["sh", "-lc", "sleep 30"]
""".lstrip()
        )
        Path(workdir, "compose.yaml").write_text(
            f"""
services:
  smoke:
    image: {DOCKER_COMPOSE_IMAGE}
    build:
      context: .
      dockerfile: Dockerfile
    labels:
      agentdp.smoke: docker-compose
""".lstrip()
        )

        log("docker compose up --build")
        try:
            run(
                [
                    "docker",
                    "compose",
                    "--progress",
                    "plain",
                    "-f",
                    str(Path(workdir, "compose.yaml")),
                    "up",
                    "-d",
                    "--build",
                ],
                timeout=60,
            )
            run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(Path(workdir, "compose.yaml")),
                    "exec",
                    "-T",
                    "smoke",
                    "sh",
                    "-lc",
                    ca_assertion("/run/agentdp/ca/ca-bundle.pem"),
                ],
                timeout=45,
            )
        finally:
            run(
                [
                    "docker",
                    "compose",
                    "-f",
                    str(Path(workdir, "compose.yaml")),
                    "down",
                    "--remove-orphans",
                ],
                timeout=45,
                check=False,
            )
            run(["docker", "image", "rm", DOCKER_COMPOSE_IMAGE], check=False)


def smoke_main():
    log("systemd socket state")
    run(["sudo", "-n", "systemctl", "is-active", "--quiet", "docker.socket"])
    run(["sudo", "-n", "systemctl", "is-active", "--quiet", "agentdp-docker-proxy.socket"])
    run(["sudo", "-n", "test", "-S", "/run/docker.sock"])
    run(["sudo", "-n", "test", "-S", "/run/agentdp/docker/docker.sock"])
    assert output(["sudo", "-n", "stat", "-c", "%a %U %G", "/run/docker.sock"]).strip() == "660 root docker"
    assert shutil.which("docker") == "/usr/local/bin/docker"

    log("docker version")
    run(["docker", "version"], timeout=30)
    run(["sudo", "-n", "systemctl", "is-active", "--quiet", "agentdp-docker-proxy.service"])
    run(["sudo", "-n", "systemctl", "is-active", "--quiet", "docker.service"])
    log("docker service restart")
    run(["sudo", "-n", "systemctl", "restart", "docker.service"])
    run(["sudo", "-n", "systemctl", "is-active", "--quiet", "agentdp-docker-proxy.socket"])
    run(["sudo", "-n", "test", "-S", "/run/docker.sock"])
    assert output(["sudo", "-n", "stat", "-c", "%a %U %G", "/run/docker.sock"]).strip() == "660 root docker"
    run(["docker", "ps"])

    log("docker pull base image")
    run(["docker", "pull", SMOKE_IMAGE], timeout=60)
    log("docker run")
    run(["docker", "run", "--rm", SMOKE_IMAGE, "sh", "-lc", ca_assertion("/run/agentdp/ca/ca-bundle.pem")])
    log("docker api create/run")
    docker_api({"DOCKER_HOST": "unix:///run/docker.sock"}, "docker")
    docker_builds()
    docker_source_anchor_build()
    docker_compose_build()

    log("podman pull base image")
    run(["podman", "pull", SMOKE_IMAGE], timeout=60)
    log("podman run")
    run(["podman", "run", "--rm", SMOKE_IMAGE, "sh", "-lc", ca_assertion("/run/agentdp/ca/ca-bundle.pem")])


def worker_main():
    try:
        smoke_main()
        RESULT_FILE.write_text("ok\n")
        log("smoke passed")
    except Exception:
        RESULT_FILE.write_text("failed\n")
        log("smoke failed")
        traceback.print_exc()
        dump_diagnostics()
        return 1
    return 0


def start_worker():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    for path in (RESULT_FILE, PID_FILE, STARTED_FILE):
        path.unlink(missing_ok=True)
    STARTED_FILE.write_text(str(time.time()))
    with LOG_FILE.open("ab", buffering=0) as log_file:
        process = subprocess.Popen(
            [sys.executable, __file__, "--worker"],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    PID_FILE.write_text(str(process.pid))
    print(f"started docker proxy smoke worker pid={process.pid} log={LOG_FILE}")
    return 1


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


def healthcheck_main():
    if RESULT_FILE.exists():
        result = read_text(RESULT_FILE)
        print(f"docker proxy smoke result={result}")
        print(output_tail(LOG_FILE))
        return 0 if result == "ok" else 1

    pid_text = read_text(PID_FILE)
    if not pid_text:
        return start_worker()

    pid = int(pid_text)
    if not pid_is_running(pid):
        RESULT_FILE.write_text("failed\n")
        print(f"docker proxy smoke worker pid={pid} exited without result")
        print(output_tail(LOG_FILE))
        return 1

    started = float(read_text(STARTED_FILE) or "0")
    elapsed = int(time.time() - started)
    if elapsed > WORKER_TIMEOUT_SECONDS:
        stop_worker(pid)
        RESULT_FILE.write_text("failed\n")
        print(f"docker proxy smoke worker pid={pid} exceeded {WORKER_TIMEOUT_SECONDS}s")
        print(output_tail(LOG_FILE))
        return 1

    print(f"docker proxy smoke worker pid={pid} running for {elapsed}s")
    print(output_tail(LOG_FILE, lines=25))
    return 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        raise SystemExit(worker_main())
    raise SystemExit(healthcheck_main())
