#!/usr/bin/env bash
set -euo pipefail

# VS Code's rich picker mixes Copilot's promoted models and control metadata into
# remote Agent Host model pools. Keep the grouped picker, but use only the
# Agent Host catalog for remote sessions. This patch is tied to the exact
# Insiders build used by AgentDP.
readonly expected_commit="780ea331b2861816fe6bb8215d812933c81df83b"
readonly expected_sha256="85fce2772e7a25234e30828ad6985910a6eb52122549fc11a7f6c0c1883a9c07"
readonly patched_sha256="b57f750d989f9521a028f612fe2633310f7b59c3472978051ec528cb9da8ff49"
readonly app_dir="${CODE_INSIDERS_APP_DIR:-/usr/share/code-insiders/resources/app}"
readonly bundle="$app_dir/out/vs/sessions/sessions.desktop.main.js"
readonly product="$app_dir/product.json"
readonly presentation_before='showUnavailableFeatured:t,showFeatured:t,showAutoModel'
readonly presentation_after='showUnavailableFeatured:!e||e===Hr,showFeatured:!e||e===Hr,showAutoModel'
readonly control_models_before='l=this._languageModelsService.getModelsControlManifest(),u=s2o(l,this._entitlementService.entitlement,a),m='
readonly control_models_after='l=this._languageModelsService.getModelsControlManifest(),u=c.showFeatured||c.showUnavailableFeatured?s2o(l,this._entitlementService.entitlement,a):{},m='

installed_commit="$(jq -r '.commit // empty' "$product")"
if [[ "$installed_commit" != "$expected_commit" ]]; then
  echo "unsupported VS Code Insiders commit: $installed_commit" >&2
  echo "expected: $expected_commit" >&2
  exit 1
fi

current_sha256="$(sha256sum "$bundle" | cut -d' ' -f1)"
case "$current_sha256" in
  "$patched_sha256")
    echo "VS Code Insiders Agent Host model picker is already patched"
    exit 0
    ;;
  "$expected_sha256") ;;
  *)
    echo "unexpected VS Code Insiders workbench checksum: $current_sha256" >&2
    exit 1
    ;;
esac

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

jq --raw-input --raw-output --slurp \
  --arg presentation_before "$presentation_before" \
  --arg presentation_after "$presentation_after" \
  --arg control_models_before "$control_models_before" \
  --arg control_models_after "$control_models_after" \
  '(split($presentation_before) | if length == 2 then join($presentation_after) else error("unexpected presentation patch count") end) |
   (split($control_models_before) | if length == 2 then join($control_models_after) else error("unexpected control-model patch count") end)' \
  "$bundle" >"$tmp"

actual_sha256="$(sha256sum "$tmp" | cut -d' ' -f1)"
if [[ "$actual_sha256" != "$patched_sha256" ]]; then
  echo "patched VS Code Insiders workbench checksum is unexpected: $actual_sha256" >&2
  exit 1
fi

if [[ -w "$bundle" ]]; then
  install -m 0644 "$tmp" "$bundle"
else
  sudo install -m 0644 "$tmp" "$bundle"
fi

echo "Patched VS Code Insiders Agent Host model picker; restart VS Code Insiders"
