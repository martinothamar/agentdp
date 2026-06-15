#!/usr/bin/env python3
import os

from agentdp_smoke import SMOKE_IMAGE, ca_assertion, dump_command, log, run, supervise


xdg_runtime = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
docker_env = {"XDG_RUNTIME_DIR": xdg_runtime, "DOCKER_HOST": f"unix://{xdg_runtime}/podman/podman.sock"}


def smoke():
    log("podman socket state")
    run(["systemctl", "--user", "is-active", "--quiet", "podman.socket"], env=docker_env)
    log("podman info")
    run(["podman", "info"], env=docker_env)
    log("docker api version")
    run(["docker", "version"], env=docker_env)
    log("podman pull base image")
    run(["podman", "pull", SMOKE_IMAGE], env=docker_env, timeout=60)
    log("docker api run")
    run(
        [
            "docker",
            "run",
            "--rm",
            "--pids-limit=-1",
            "--name",
            "agentdp-podman-api-run-smoke",
            "--label",
            "agentdp.smoke=podman-api",
            "--env",
            "AGENTDP_SMOKE=run",
            "--add-host",
            "agentdp-podman-api.local:127.0.0.1",
            "--workdir",
            "/tmp",
            SMOKE_IMAGE,
            "sh",
            "-lc",
            f'test "$AGENTDP_SMOKE" = run && grep -q agentdp-podman-api.local /etc/hosts && test "$PWD" = /tmp && {ca_assertion()}',
        ],
        env=docker_env,
    )


def diagnostics():
    dump_command(["systemctl", "--user", "--no-pager", "--full", "status", "podman.socket"], env=docker_env)
    dump_command(["podman", "info"], env=docker_env, timeout=20)
    dump_command(["podman", "ps", "-a"], env=docker_env, timeout=20)
    dump_command(["podman", "images"], env=docker_env, timeout=20)


if __name__ == "__main__":
    raise SystemExit(supervise("podman-docker-api", smoke, diagnostics=diagnostics))
