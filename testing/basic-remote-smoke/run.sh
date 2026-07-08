#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/testing/basic-remote-smoke/docker-compose.yml"

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

extract_parent_version() {
  grep -A 3 '<parent>' "${repo_root}/pom.xml" |
    grep '<version>' |
    head -n 1 |
    sed -E 's/.*<version>([^<]+)<.*/\1/'
}

validate_version() {
  local label="$1"
  local version="$2"

  [[ "${version}" =~ ^[0-9]+$ ]] || fail "${label} must be an integer Trino version"
}

local_version="${LOCAL_TRINO_VERSION:-$(extract_parent_version)}"
validate_version "LOCAL_TRINO_VERSION" "${local_version}"

remote_versions=("$@")
if [[ "${#remote_versions[@]}" -eq 0 && -n "${REMOTE_TRINO_VERSION:-}" ]]; then
  remote_versions=("${REMOTE_TRINO_VERSION}")
fi
[[ "${#remote_versions[@]}" -gt 0 ]] || fail "usage: testing/basic-remote-smoke/run.sh REMOTE_VERSION [REMOTE_VERSION ...]"

plugin_dir="${repo_root}/target/trino-trino-${local_version}"
if [[ ! -d "${plugin_dir}" ]]; then
  echo "Missing ${plugin_dir}. Build the plugin first:"
  echo "  mvn -B clean verify"
  exit 1
fi

for remote_version in "${remote_versions[@]}"; do
  validate_version "remote version" "${remote_version}"
done

run_version() (
  set -euo pipefail

  local remote_version="$1"
  local project_name="${BASIC_REMOTE_SMOKE_PROJECT_NAME:-trino2trino-basic-remote-${local_version}-to-${remote_version}}"
  local log_base="${BASIC_REMOTE_SMOKE_LOG_BASE:-${repo_root}/target/basic-remote-smoke}"
  local log_dir="${log_base}/${local_version}-to-${remote_version}"
  local compose_touched=false

  compose() {
    LOCAL_TRINO_VERSION="${local_version}" REMOTE_TRINO_VERSION="${remote_version}" \
      docker compose -p "${project_name}" -f "${compose_file}" "$@"
  }

  reset_logs() {
    mkdir -p "${log_dir}"
    rm -f "${log_dir}"/docker-compose-ps-*.txt "${log_dir}"/docker-compose-logs-*.txt
  }

  capture_logs() {
    local label="$1"

    mkdir -p "${log_dir}"
    compose ps -a >"${log_dir}/docker-compose-ps-${label}.txt" 2>&1 || true
    compose logs --no-color >"${log_dir}/docker-compose-logs-${label}.txt" 2>&1 || true
    echo "Basic remote smoke diagnostics written to ${log_dir}"
  }

  finish() {
    local exit_code=$?

    if [[ "${compose_touched}" == "true" ]]; then
      if [[ "${exit_code}" -ne 0 ]]; then
        capture_logs failure
      elif [[ "${BASIC_REMOTE_SMOKE_ALWAYS_LOGS:-false}" == "true" ]]; then
        capture_logs success
      fi

      if [[ "${BASIC_REMOTE_SMOKE_KEEP_RUNNING:-false}" != "true" ]]; then
        compose down -v --remove-orphans >/dev/null 2>&1 || true
      fi
    fi

    exit "${exit_code}"
  }

  reset_logs
  trap finish EXIT

  trino_exec() {
    local service="$1"
    local sql="$2"

    compose exec -T "${service}" trino \
      --server localhost:8080 \
      --user trino \
      --output-format TSV \
      --execute "${sql}" | tr -d '\r'
  }

  wait_for_trino() {
    local service="$1"
    local label="$2"
    local attempts="${BASIC_REMOTE_SMOKE_WAIT_ATTEMPTS:-90}"

    for attempt in $(seq 1 "${attempts}"); do
      if trino_exec "${service}" "SELECT 1" >/dev/null 2>&1; then
        echo "${label} is ready"
        return 0
      fi
      echo "Waiting for ${label} (${attempt}/${attempts})"
      sleep 2
    done

    echo "${label} did not become ready"
    compose logs --tail=200 "${service}" >&2 || true
    return 1
  }

  assert_query_equals() {
    local expected="$1"
    local sql="$2"
    local actual

    actual="$(trino_exec trino-local-basic "${sql}")"
    if [[ "${actual}" != "${expected}" ]]; then
      echo "Assertion failed"
      echo "SQL: ${sql}"
      echo "Expected: ${expected}"
      echo "Actual: ${actual}"
      exit 1
    fi
  }

  assert_query_contains() {
    local needle="$1"
    local sql="$2"
    local actual

    actual="$(trino_exec trino-local-basic "${sql}")"
    if [[ "${actual}" != *"${needle}"* ]]; then
      echo "Assertion failed"
      echo "SQL: ${sql}"
      echo "Expected output to contain: ${needle}"
      echo "Actual output:"
      echo "${actual}"
      exit 1
    fi
  }

  echo "Starting basic remote smoke stack for local Trino ${local_version} -> remote Trino ${remote_version}"
  compose_touched=true
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d

  wait_for_trino trino-remote-basic "remote Trino ${remote_version}"
  wait_for_trino trino-local-basic "local Trino ${local_version}"

  echo "Running TPCH remote federation assertions"
  assert_query_contains "tiny" "SHOW SCHEMAS FROM remote_tpch"
  assert_query_contains "nation" "SHOW TABLES FROM remote_tpch.tiny"
  assert_query_contains "region" "SHOW TABLES FROM remote_tpch.tiny"
  assert_query_contains "orders" "SHOW TABLES FROM remote_tpch.tiny"
  assert_query_contains $'orderkey\tbigint' "DESCRIBE remote_tpch.tiny.orders"
  assert_query_contains $'orderdate\tdate' "DESCRIBE remote_tpch.tiny.orders"
  assert_query_equals "25" "SELECT count(*) FROM remote_tpch.tiny.nation"
  assert_query_equals "ARGENTINA" "SELECT name FROM remote_tpch.tiny.nation WHERE nationkey = 1"
  assert_query_equals $'1\tARGENTINA\n2\tBRAZIL\n3\tCANADA' "SELECT n.nationkey, n.name FROM remote_tpch.tiny.nation n JOIN remote_tpch.tiny.region r ON n.regionkey = r.regionkey WHERE r.name = 'AMERICA' ORDER BY n.nationkey LIMIT 3"
  assert_query_equals "0:5,1:5,2:5,3:5,4:5" "SELECT array_join(array_agg(CAST(regionkey AS VARCHAR) || ':' || CAST(nation_count AS VARCHAR) ORDER BY regionkey), ',') FROM (SELECT regionkey, count(*) AS nation_count FROM remote_tpch.tiny.nation GROUP BY regionkey)"
  assert_query_equals "25" "SELECT count(*) FROM remote_tpch.tiny.nation n JOIN remote_tpch.tiny.region r ON n.regionkey = r.regionkey"
  assert_query_equals "2" "SELECT count(*) FROM (VALUES BIGINT '1', BIGINT '3') AS local_keys(nationkey) JOIN remote_tpch.tiny.nation n ON n.nationkey = local_keys.nationkey"
  assert_query_equals "25" "SELECT * FROM TABLE(remote_tpch.system.query(query => 'SELECT count(*) FROM tpch.tiny.nation'))"

  echo "Running CHAR/VARCHAR remote delegation assertions"
  trino_exec trino-remote-basic "DROP TABLE IF EXISTS memory.default.basic_char_probe" >/dev/null
  trino_exec trino-remote-basic "CREATE TABLE memory.default.basic_char_probe AS SELECT * FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  ')) AS t(id, c, v)" >/dev/null
  assert_query_equals $'[a]\t1' "SELECT '[' || CAST(c AS VARCHAR) || ']', length(CAST(c AS VARCHAR)) FROM remote_memory.default.basic_char_probe WHERE id = 1"
  assert_query_equals "1" "SELECT id FROM remote_memory.default.basic_char_probe WHERE v = c ORDER BY id"
  assert_query_equals $'1\n2' "SELECT id FROM remote_memory.default.basic_char_probe WHERE c = CAST('a' AS CHAR(3)) ORDER BY id"
  assert_query_equals "1" "SELECT id FROM remote_memory.default.basic_char_probe WHERE v = CAST('a' AS CHAR(3)) ORDER BY id"
  if [[ "${remote_version}" -lt 482 ]]; then
    assert_query_contains "trim(TRAILING ' ' FROM" "EXPLAIN SELECT '[' || CAST(c AS VARCHAR) || ']', length(CAST(c AS VARCHAR)) FROM remote_memory.default.basic_char_probe WHERE id = 1"
    assert_query_contains "trim(TRAILING ' ' FROM" "EXPLAIN SELECT id FROM remote_memory.default.basic_char_probe WHERE v = c ORDER BY id"
  fi

  echo "Basic remote smoke test passed for local Trino ${local_version} -> remote Trino ${remote_version}"
)

failures=0
for remote_version in "${remote_versions[@]}"; do
  set +e
  run_version "${remote_version}"
  status=$?
  set -e

  if [[ "${status}" -ne 0 ]]; then
    failures=$((failures + 1))
  fi
done

if [[ "${failures}" -ne 0 ]]; then
  echo "${failures} basic remote smoke probe(s) failed"
  exit 1
fi
