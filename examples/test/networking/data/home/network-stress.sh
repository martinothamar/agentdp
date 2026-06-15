#!/usr/bin/env bash
set -euo pipefail

rounds="${1:-50}"
concurrency="${2:-8}"
allowed_url="${AGENTDP_NETWORK_ALLOWED_URL:-https://example.com}"
blocked_url="${AGENTDP_NETWORK_BLOCKED_URL:-https://www.microsoft.com}"

if ! [[ "$rounds" =~ ^[0-9]+$ ]] || [ "$rounds" -lt 1 ]; then
  echo "rounds must be a positive integer" >&2
  exit 2
fi

if ! [[ "$concurrency" =~ ^[0-9]+$ ]] || [ "$concurrency" -lt 1 ]; then
  echo "concurrency must be a positive integer" >&2
  exit 2
fi

run_one() {
  local index="$1"

  curl -fsSI --connect-timeout 5 --max-time 20 "$allowed_url" >/dev/null

  if curl -fsSI --connect-timeout 3 --max-time 8 "$blocked_url" >/dev/null 2>&1; then
    echo "[$index] unexpectedly reached blocked URL: $blocked_url" >&2
    return 1
  fi

  curl -fsSI --connect-timeout 5 --max-time 20 "$allowed_url" >/dev/null
}

active=0
for index in $(seq 1 "$rounds"); do
  run_one "$index" &
  active=$((active + 1))

  if [ "$active" -ge "$concurrency" ]; then
    wait -n
    active=$((active - 1))
  fi
done

wait
echo "network-stress-ok rounds=$rounds concurrency=$concurrency"

