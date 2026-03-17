#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pin_file="${KOMARI_FRONTEND_PIN_FILE:-${repo_root}/frontend-source.env}"

repo=""
ref=""
local_checkout=""

usage() {
  cat <<'EOF'
Usage:
  bash scripts/update-frontend-pin.sh --repo <repo-url> --ref <commit-or-tag>
  bash scripts/update-frontend-pin.sh --from-local <path-to-komari-web-checkout>

Options:
  --repo <repo-url>      Frontend repository URL to pin
  --ref <commit-or-tag>  Frontend commit, tag, or branch to pin
  --from-local <path>    Read repo URL and current HEAD commit from a local checkout
  --pin-file <path>      Override the output pin file path
EOF
}

normalize_repo_url() {
  local value="$1"
  if [[ "${value}" =~ ^git@github\.com:(.+)\.git$ ]]; then
    printf 'https://github.com/%s.git\n' "${BASH_REMATCH[1]}"
    return
  fi
  if [[ "${value}" =~ ^ssh://git@github\.com/(.+)\.git$ ]]; then
    printf 'https://github.com/%s.git\n' "${BASH_REMATCH[1]}"
    return
  fi
  printf '%s\n' "${value}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    --ref)
      ref="${2:-}"
      shift 2
      ;;
    --from-local)
      local_checkout="${2:-}"
      shift 2
      ;;
    --pin-file)
      pin_file="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "${local_checkout}" ]]; then
  local_checkout="$(cd "${local_checkout}" && pwd)"
  repo="$(git -C "${local_checkout}" config --get remote.origin.url)"
  ref="$(git -C "${local_checkout}" rev-parse HEAD)"
fi

if [[ -z "${repo}" || -z "${ref}" ]]; then
  usage >&2
  exit 1
fi

repo="$(normalize_repo_url "${repo}")"

mkdir -p "$(dirname "${pin_file}")"
{
  printf '# Pinned komari-web source used by GitHub builds and local prepare-frontend runs.\n'
  printf '# Update this file directly or use `bash scripts/update-frontend-pin.sh`.\n'
  printf 'KOMARI_PINNED_FRONTEND_REPO=%s\n' "${repo}"
  printf 'KOMARI_PINNED_FRONTEND_REF=%s\n' "${ref}"
} > "${pin_file}"

printf 'Updated %s\n' "${pin_file}"
printf 'Pinned frontend repo: %s\n' "${repo}"
printf 'Pinned frontend ref: %s\n' "${ref}"
