#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container_name="${KOMARI_VERIFY_CONTAINER:-komari-mariadb-verify}"
mysql_image="${KOMARI_VERIFY_IMAGE:-mariadb:10.11}"
mysql_port="${KOMARI_VERIFY_PORT:-33306}"
mysql_root_password="${KOMARI_VERIFY_ROOT_PASSWORD:-komariroot}"
mysql_user="${KOMARI_VERIFY_DB_USER:-root}"
mysql_pass="${KOMARI_VERIFY_DB_PASS:-$mysql_root_password}"
admin_username="${KOMARI_VERIFY_ADMIN_USERNAME:-admin}"
admin_password="${KOMARI_VERIFY_ADMIN_PASSWORD:-adminpass}"
keep_container="${KOMARI_VERIFY_KEEP_CONTAINER:-0}"

work_dir="$(mktemp -d)"
binary_path="${work_dir}/komari-verify"
created_container=0

cleanup() {
  if [[ -d "${work_dir}" ]]; then
    rm -rf "${work_dir}"
  fi
  if [[ "${created_container}" == "1" && "${keep_container}" != "1" ]]; then
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

log() {
  printf '[verify-mysql] %s\n' "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

assert_eq() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    printf 'assertion failed: %s (expected %q, got %q)\n' "${message}" "${expected}" "${actual}" >&2
    exit 1
  fi
}

assert_contains() {
  local file="$1"
  local needle="$2"
  local message="$3"
  if ! grep -q -- "${needle}" "${file}"; then
    printf 'assertion failed: %s\n' "${message}" >&2
    printf '--- %s ---\n' "${file}" >&2
    sed -n '1,220p' "${file}" >&2
    exit 1
  fi
}

mysql_exec() {
  docker exec -i "${container_name}" mariadb -u"${mysql_user}" -p"${mysql_pass}" "$@"
}

mysql_query() {
  mysql_exec -N -e "$1"
}

ensure_container() {
  if docker ps -a --format '{{.Names}}' | grep -Fxq "${container_name}"; then
    if ! docker ps --format '{{.Names}}' | grep -Fxq "${container_name}"; then
      docker start "${container_name}" >/dev/null
    fi
    return
  fi

  log "starting MariaDB container ${container_name}"
  docker pull "${mysql_image}" >/dev/null
  docker run -d \
    --name "${container_name}" \
    -e "MARIADB_ROOT_PASSWORD=${mysql_root_password}" \
    -p "127.0.0.1:${mysql_port}:3306" \
    "${mysql_image}" \
    --character-set-server=utf8mb4 \
    --collation-server=utf8mb4_unicode_ci >/dev/null
  created_container=1
}

wait_for_mariadb() {
  for _ in $(seq 1 30); do
    if docker exec "${container_name}" mariadb-admin -u"${mysql_user}" -p"${mysql_pass}" ping --silent >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  printf 'MariaDB container did not become ready in time\n' >&2
  exit 1
}

build_binary() {
  log "building komari verification binary"
  (
    cd "${repo_root}"
    GOCACHE="${repo_root}/.gocache" GOMODCACHE="${repo_root}/.gomodcache" go build -o "${binary_path}" .
  )
}

run_komari() {
  local db_name="$1"
  local listen_port="$2"
  local run_dir="${work_dir}/${db_name}"
  local log_file="${run_dir}/server.log"
  mkdir -p "${run_dir}"

  (
    cd "${run_dir}"
    KOMARI_DB_HOST=127.0.0.1 \
    KOMARI_DB_PORT="${mysql_port}" \
    KOMARI_DB_USER="${mysql_user}" \
    KOMARI_DB_PASS="${mysql_pass}" \
    KOMARI_DB_NAME="${db_name}" \
    KOMARI_LISTEN="127.0.0.1:${listen_port}" \
    ADMIN_USERNAME="${admin_username}" \
    ADMIN_PASSWORD="${admin_password}" \
    "${binary_path}" >"${log_file}" 2>&1 &
    pid=$!
    sleep 6
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      wait "${pid}" || true
      printf 'komari exited early for %s\n' "${db_name}" >&2
      sed -n '1,220p' "${log_file}" >&2
      exit 1
    fi
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" >/dev/null 2>&1 || true
  )

  printf '%s\n' "${log_file}"
}

seed_empty_database() {
  mysql_exec -e "DROP DATABASE IF EXISTS komari_empty_verify; CREATE DATABASE komari_empty_verify CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
}

seed_legacy_database() {
  mysql_exec <<'SQL'
DROP DATABASE IF EXISTS komari_legacy_verify;
CREATE DATABASE komari_legacy_verify CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE komari_legacy_verify;

CREATE TABLE configs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  sitename VARCHAR(100) NOT NULL,
  description TEXT NULL,
  allow_cors TINYINT(1) DEFAULT 0,
  api_key VARCHAR(255) DEFAULT '',
  auto_discovery_key VARCHAR(255) DEFAULT '',
  script_domain VARCHAR(255) DEFAULT '',
  cn_connectivity_enabled TINYINT(1) DEFAULT 0,
  cn_connectivity_target VARCHAR(255) DEFAULT '',
  cn_connectivity_interval INT DEFAULT 60,
  cn_connectivity_retry_attempts INT DEFAULT 3,
  cn_connectivity_retry_delay_seconds INT DEFAULT 1,
  cn_connectivity_timeout_seconds INT DEFAULT 5,
  send_ip_addr_to_guest TINYINT(1) DEFAULT 0,
  eula_accepted TINYINT(1) DEFAULT 0,
  base_scripts_url VARCHAR(255) DEFAULT '',
  geo_ip_enabled TINYINT(1) DEFAULT 1,
  geo_ip_provider VARCHAR(20) DEFAULT 'ip-api',
  nezha_compat_enabled TINYINT(1) DEFAULT 0,
  nezha_compat_listen VARCHAR(100) DEFAULT '',
  o_auth_enabled TINYINT(1) DEFAULT 0,
  o_auth_provider VARCHAR(50) DEFAULT 'github',
  disable_password_login TINYINT(1) DEFAULT 0,
  custom_head LONGTEXT NULL,
  custom_body LONGTEXT NULL,
  notification_enabled TINYINT(1) DEFAULT 0,
  notification_method VARCHAR(64) DEFAULT 'none',
  notification_template LONGTEXT NULL,
  expire_notification_enabled TINYINT(1) DEFAULT 0,
  expire_notification_lead_days INT DEFAULT 7,
  login_notification TINYINT(1) DEFAULT 0,
  traffic_limit_percentage DECIMAL(5,2) DEFAULT 80.00,
  record_enabled TINYINT(1) DEFAULT 1,
  record_preserve_time INT DEFAULT 720,
  ping_record_preserve_time INT DEFAULT 24,
  theme VARCHAR(100) DEFAULT '',
  private_site TINYINT(1) DEFAULT 0,
  created_at DATETIME(3) NULL,
  updated_at DATETIME(3) NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO configs (
  sitename, description, allow_cors, api_key, auto_discovery_key, script_domain,
  cn_connectivity_enabled, cn_connectivity_target, cn_connectivity_interval,
  cn_connectivity_retry_attempts, cn_connectivity_retry_delay_seconds, cn_connectivity_timeout_seconds,
  send_ip_addr_to_guest, eula_accepted, base_scripts_url, geo_ip_enabled,
  geo_ip_provider, nezha_compat_enabled, nezha_compat_listen, o_auth_enabled,
  o_auth_provider, disable_password_login, custom_head, custom_body,
  notification_enabled, notification_method, notification_template,
  expire_notification_enabled, expire_notification_lead_days, login_notification,
  traffic_limit_percentage, record_enabled, record_preserve_time,
  ping_record_preserve_time, theme, private_site, created_at, updated_at
) VALUES (
  'Legacy Komari', 'legacy config migration check', 1, 'legacy-api-key', 'legacy-discovery-key', 'https://legacy-scripts.example.com',
  1, '223.5.5.5', 90, 4, 2, 8,
  1, 1, 'https://mirror.example.com/scripts', 1,
  'geojs', 1, '0.0.0.0:5555', 1,
  'github', 0, '<meta name="legacy" content="1">', '<script>window.__legacy=true</script>',
  1, 'telegram', 'Legacy template',
  1, 5, 1,
  87.50, 1, 480,
  12, 'legacy-theme', 1, NOW(3), NOW(3)
);

CREATE TABLE offline_notifications (
  client VARCHAR(36) NOT NULL,
  enable TINYINT(1) DEFAULT 0,
  grace_period INT NOT NULL DEFAULT 180,
  last_notified DATETIME(3) NULL,
  KEY idx_offline_notifications_client (client)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO offline_notifications (client, enable, grace_period, last_notified)
VALUES ('legacy-client-1', 1, 300, NOW(3));

CREATE TABLE logs (
  uuid VARCHAR(36) NOT NULL,
  ip VARCHAR(45) DEFAULT '',
  message TEXT NOT NULL,
  msg_type VARCHAR(20) NOT NULL,
  time DATETIME(3) NOT NULL,
  PRIMARY KEY (uuid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO logs (uuid, ip, message, msg_type, time)
VALUES ('legacy-log-user', '127.0.0.1', 'legacy log row', 'info', NOW(3));
SQL
}

verify_empty_database() {
  local log_file="$1"
  assert_contains "${log_file}" "Using MySQL database" "empty-db startup log should mention MySQL"
  local users_count
  users_count="$(mysql_query "USE komari_empty_verify; SELECT COUNT(*) FROM users;")"
  assert_eq "${users_count}" "1" "empty-db bootstrap should create one admin user"

  local session_columns
  session_columns="$(mysql_query "USE komari_empty_verify; SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',') FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'sessions';")"
  if [[ "${session_columns}" == *"current_tenant_id"* ]]; then
    printf 'assertion failed: sessions table should not contain current_tenant_id\n' >&2
    exit 1
  fi

  local expected_tables actual_tables
  expected_tables="clients,configs,offline_notifications,sessions,user_configs"
  actual_tables="$(mysql_query "USE komari_empty_verify; SELECT GROUP_CONCAT(table_name ORDER BY table_name SEPARATOR ',') FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('clients','configs','offline_notifications','sessions','user_configs');")"
  assert_eq "${actual_tables}" "${expected_tables}" "empty-db startup should create core tables"
}

verify_legacy_database() {
  local log_file="$1"
  assert_contains "${log_file}" "Using MySQL database" "legacy-db startup log should mention MySQL"
  assert_contains "${log_file}" "Moving legacy config data" "legacy-db startup should migrate legacy config rows"

  local config_columns
  config_columns="$(mysql_query "USE komari_legacy_verify; SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',') FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'configs';")"
  assert_eq "${config_columns}" "key,value" "legacy configs table should be migrated to key/value schema"

  local user_config_columns
  user_config_columns="$(mysql_query "USE komari_legacy_verify; SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',') FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'user_configs';")"
  assert_eq "${user_config_columns}" "user_uuid,key,value" "legacy-db startup should create user_configs"

  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT value FROM configs WHERE \`key\` = 'sitename';")" "\"Legacy Komari\"" "legacy sitename should be preserved"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT value FROM configs WHERE \`key\` = 'auto_discovery_key';")" "\"legacy-discovery-key\"" "legacy auto discovery key should be preserved"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT value FROM configs WHERE \`key\` = 'script_domain';")" "\"https://legacy-scripts.example.com\"" "legacy script domain should be preserved"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT value FROM configs WHERE \`key\` = 'base_scripts_url';")" "\"https://mirror.example.com/scripts\"" "legacy base scripts URL should be preserved"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT value FROM configs WHERE \`key\` = 'send_ip_addr_to_guest';")" "true" "legacy guest IP visibility flag should be preserved"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT value FROM configs WHERE \`key\` = 'notification_method';")" "\"telegram\"" "legacy notification method should be preserved"

  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = DATABASE() AND table_name = 'offline_notifications' AND constraint_type = 'PRIMARY KEY';")" "1" "offline_notifications should regain a primary key"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT COUNT(*) FROM offline_notifications;")" "0" "orphaned offline_notifications rows should be cleaned during upgrade"
  local log_columns
  log_columns="$(mysql_query "USE komari_legacy_verify; SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',') FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'logs';")"
  if [[ "${log_columns}" != *"id"* || "${log_columns}" != *"user_id"* ]]; then
    printf 'assertion failed: legacy logs table should include id and user_id columns (got %q)\n' "${log_columns}" >&2
    exit 1
  fi
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position SEPARATOR ',') FROM information_schema.key_column_usage WHERE table_schema = DATABASE() AND table_name = 'logs' AND constraint_name = 'PRIMARY';")" "id" "legacy logs table should restore id as the primary key"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT COUNT(*) FROM logs WHERE uuid = 'legacy-log-user' AND message = 'legacy log row';")" "1" "legacy logs rows should survive the primary key repair"
  assert_eq "$(mysql_query "USE komari_legacy_verify; SELECT COUNT(*) FROM users;")" "1" "legacy-db upgrade should still bootstrap one admin user"
}

main() {
  require_cmd docker
  require_cmd git
  require_cmd go

  ensure_container
  wait_for_mariadb
  build_binary

  log "verifying empty-database startup"
  seed_empty_database
  empty_log="$(run_komari "komari_empty_verify" "35878")"
  verify_empty_database "${empty_log}"

  log "verifying legacy-database upgrade"
  seed_legacy_database
  legacy_log="$(run_komari "komari_legacy_verify" "35879")"
  verify_legacy_database "${legacy_log}"

  log "MySQL runtime verification passed"
  printf 'empty-db log: %s\n' "${empty_log}"
  printf 'legacy-db log: %s\n' "${legacy_log}"
}

main "$@"
