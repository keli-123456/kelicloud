#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
frontend_pin_file="${KOMARI_FRONTEND_PIN_FILE:-${repo_root}/frontend-source.env}"
pinned_frontend_repo="https://github.com/keli-123456/kelicloud-web.git"
pinned_frontend_ref=""
frontend_path="${KOMARI_FRONTEND_PATH:-}"
tmp_dir=""

if [[ -f "${frontend_pin_file}" ]]; then
  # shellcheck disable=SC1090
  source "${frontend_pin_file}"
  pinned_frontend_repo="${KOMARI_PINNED_FRONTEND_REPO:-$pinned_frontend_repo}"
  pinned_frontend_ref="${KOMARI_PINNED_FRONTEND_REF:-$pinned_frontend_ref}"
fi

frontend_repo_url="${KOMARI_FRONTEND_REPO:-$pinned_frontend_repo}"
frontend_ref="${KOMARI_FRONTEND_REF:-$pinned_frontend_ref}"

cleanup() {
  if [[ -n "${tmp_dir}" && -d "${tmp_dir}" ]]; then
    rm -rf "${tmp_dir}"
  fi
}
trap cleanup EXIT

if [[ -n "${frontend_path}" ]]; then
  frontend_dir="$(cd "${frontend_path}" && pwd)"
  printf 'Using local frontend checkout %s\n' "${frontend_dir}"
else
  tmp_dir="$(mktemp -d)"
  frontend_dir="${tmp_dir}/komari-web"
  printf 'Cloning frontend source %s' "${frontend_repo_url}"
  if [[ -n "${frontend_ref}" ]]; then
    printf ' @ %s' "${frontend_ref}"
  fi
  printf '\n'
  git clone "${frontend_repo_url}" "${frontend_dir}"
  if [[ -n "${frontend_ref}" ]]; then
    git -C "${frontend_dir}" checkout "${frontend_ref}"
  fi
fi

pushd "${frontend_dir}" >/dev/null
if [[ -f package-lock.json ]]; then
  npm ci
else
  npm install
fi
npm run build
popd >/dev/null

dest_dir="${repo_root}/public/frontend/dist"
mkdir -p "${dest_dir}"
rm -rf "${dest_dir:?}/"*
cp -R "${frontend_dir}/dist/." "${dest_dir}/"

printf 'Frontend bundle copied to %s\n' "${dest_dir}"
