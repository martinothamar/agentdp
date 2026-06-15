#!/usr/bin/env python3
import json
import shutil
import tempfile
from pathlib import Path

from agentdp_smoke import CA_KEYS_ENV, SMOKE_IMAGE, ca_assertion, dump_command, log, run, supervise


COMPOSE_PROJECT = "agentdp-podman-compose-smoke"
COMPOSE_BUILD_IMAGE = "agentdp/podman-compose-build-smoke:latest"


def has_compose_provider():
    return any(shutil.which(name) for name in ("podman-compose", "docker-compose"))


def dockerfile_ca_assertion(ca_path):
    keys = "${" + CA_KEYS_ENV + ":?}"
    return "; \\\n    ".join(
        [
            "set -eu",
            f'test -n "{keys}"',
            f'for key in $(printf "%s" "{keys}" | tr "," " "); do eval "value=\\${{$key:-}}"; test "$value" = "{ca_path}" || {{ echo "$key=$value"; exit 1; }}; done',
            f"test -r {ca_path}",
            "apk add --no-cache curl >/dev/null",
            "curl -fsS https://api.nuget.org/v3/index.json >/tmp/nuget-index.json",
        ]
    )


def podman_compose():
    with tempfile.TemporaryDirectory() as workdir:
        compose_file = Path(workdir, "compose.yaml")
        compose_file.write_text(
            f"""
services:
  smoke:
    image: {SMOKE_IMAGE}
    command: {json.dumps(["sh", "-lc", ca_assertion()])}
    labels:
      agentdp.smoke: podman-compose
""".lstrip()
        )

        log("podman compose up")
        try:
            run(
                [
                    "podman",
                    "compose",
                    "-p",
                    COMPOSE_PROJECT,
                    "-f",
                    str(compose_file),
                    "up",
                    "--abort-on-container-exit",
                ],
                timeout=60,
            )
        finally:
            run(
                [
                    "podman",
                    "compose",
                    "-p",
                    COMPOSE_PROJECT,
                    "-f",
                    str(compose_file),
                    "down",
                ],
                timeout=45,
                check=False,
            )


def podman_compose_build():
    with tempfile.TemporaryDirectory() as workdir:
        Path(workdir, "Dockerfile").write_text(
            f"""
FROM {SMOKE_IMAGE}
RUN {dockerfile_ca_assertion("/tmp/agentdp-ca-bundle.crt")}
CMD ["sh", "-lc", "true"]
""".lstrip()
        )
        compose_file = Path(workdir, "compose.yaml")
        compose_file.write_text(
            f"""
services:
  built:
    image: {COMPOSE_BUILD_IMAGE}
    build:
      context: .
      dockerfile: Dockerfile
    command: ["sh", "-lc", "true"]
    labels:
      agentdp.smoke: podman-compose-build
""".lstrip()
        )

        log("podman compose build")
        try:
            run(
                [
                    "podman",
                    "compose",
                    "-p",
                    COMPOSE_PROJECT,
                    "-f",
                    str(compose_file),
                    "build",
                    "--no-cache",
                    "built",
                ],
                timeout=60,
            )
        finally:
            run(["podman", "image", "rm", COMPOSE_BUILD_IMAGE], timeout=30, check=False)


def smoke():
    log("podman pull base image")
    run(["podman", "pull", SMOKE_IMAGE], timeout=60)
    log("podman run")
    run(["podman", "run", "--rm", SMOKE_IMAGE, "sh", "-lc", ca_assertion()])
    if has_compose_provider():
        podman_compose()
        podman_compose_build()
    else:
        log("podman compose skipped: no compose provider installed")


def diagnostics():
    dump_command(["podman", "info"], timeout=20)
    dump_command(["podman", "ps", "-a"], timeout=20)
    dump_command(["podman", "images"], timeout=20)


if __name__ == "__main__":
    raise SystemExit(supervise("podman", smoke, diagnostics=diagnostics))
