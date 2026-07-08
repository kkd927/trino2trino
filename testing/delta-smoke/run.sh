#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/testing/delta-smoke/docker-compose.yml"
project_name="${DELTA_SMOKE_PROJECT_NAME:-trino2trino-delta-smoke}"
plugin_dir="${repo_root}/target/trino-trino-482"
log_dir="${DELTA_SMOKE_LOG_DIR:-${repo_root}/target/delta-smoke}"
compose_touched=false

compose() {
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
  echo "Delta smoke diagnostics written to ${log_dir}"
}

finish() {
  local exit_code=$?

  if [[ "${compose_touched}" == "true" ]]; then
    if [[ "${exit_code}" -ne 0 ]]; then
      capture_logs failure
    elif [[ "${DELTA_SMOKE_ALWAYS_LOGS:-false}" == "true" ]]; then
      capture_logs success
    fi

    if [[ "${DELTA_SMOKE_KEEP_RUNNING:-false}" != "true" ]]; then
      compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
  fi

  exit "${exit_code}"
}

if [[ ! -d "${plugin_dir}" ]]; then
  echo "Missing ${plugin_dir}. Build the plugin first:"
  echo "  mvn -B clean verify"
  exit 1
fi

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
  local attempts="${DELTA_SMOKE_WAIT_ATTEMPTS:-90}"

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
  actual="$(trino_exec trino-local-delta "${sql}")"
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
  actual="$(trino_exec trino-local-delta "${sql}")"
  if [[ "${actual}" != *"${needle}"* ]]; then
    echo "Assertion failed"
    echo "SQL: ${sql}"
    echo "Expected output to contain: ${needle}"
    echo "Actual output:"
    echo "${actual}"
    exit 1
  fi
}

assert_query_not_contains() {
  local needle="$1"
  local sql="$2"
  local actual
  actual="$(trino_exec trino-local-delta "${sql}")"
  if [[ "${actual}" == *"${needle}"* ]]; then
    echo "Assertion failed"
    echo "SQL: ${sql}"
    echo "Expected output not to contain: ${needle}"
    echo "Actual output:"
    echo "${actual}"
    exit 1
  fi
}

echo "Starting Delta smoke stack"
compose_touched=true
compose down -v --remove-orphans >/dev/null 2>&1 || true
compose up -d

wait_for_trino trino-remote-delta "remote Delta Trino"
wait_for_trino trino-local-delta "local federated Trino"

echo "Seeding Delta tables on the remote Trino cluster"
compose exec -T trino-remote-delta trino \
  --server localhost:8080 \
  --user trino \
  --file /delta-smoke/sql/seed-delta.sql \
  --output-format TSV

echo "Running local-to-remote Delta smoke assertions"
assert_query_contains "smoke" "SHOW SCHEMAS FROM remote_delta"
assert_query_contains "orders" "SHOW TABLES FROM remote_delta.smoke"
assert_query_contains "customers" "SHOW TABLES FROM remote_delta.smoke"
assert_query_contains "complex_types" "SHOW TABLES FROM remote_delta.smoke"
assert_query_contains $'totalprice\tdecimal(18,2)' "DESCRIBE remote_delta.smoke.orders"
assert_query_contains $'orderdate\tdate' "DESCRIBE remote_delta.smoke.orders"
assert_query_equals "3" "SELECT count(*) FROM remote_delta.smoke.customers"
assert_query_equals "4" "SELECT count(*) FROM remote_delta.smoke.orders"
assert_query_equals "131.75" "SELECT CAST(sum(totalprice) AS VARCHAR) FROM remote_delta.smoke.orders WHERE ds = '2026-01-01'"
assert_query_equals "2" "SELECT count(*) FROM remote_delta.smoke.orders WHERE ds = '2026-01-01'"
assert_query_equals "orderkey:bigint,custkey:bigint,totalprice:decimal(18,2),orderdate:date,status:varchar,ds:varchar" "SELECT array_join(array_agg(column_name || ':' || data_type ORDER BY ordinal_position), ',') FROM remote_delta.information_schema.columns WHERE table_schema = 'smoke' AND table_name = 'orders'"
assert_query_equals "2	gold	7	12345678901234567890.1234" "SELECT cardinality(tags), element_at(attrs, 'tier'), detail.qty, CAST(large_amount AS VARCHAR) FROM remote_delta.smoke.complex_types WHERE id = 1"
assert_query_equals "2026-01-01 10:11:12.123456" "SELECT CAST(created_at AS VARCHAR) FROM remote_delta.smoke.complex_types WHERE id = 1"
assert_query_equals "3" "SELECT count(*) FROM (VALUES BIGINT '1', BIGINT '3') AS local_keys(custkey) JOIN remote_delta.smoke.orders o ON o.custkey = local_keys.custkey"
assert_query_equals "3" "SELECT count(*) FROM remote_delta.smoke.orders o JOIN remote_delta.smoke.customers c ON o.custkey = c.custkey WHERE c.region IN ('AMERICA', 'ASIA')"
assert_query_equals "2" "SELECT count(*) FROM TABLE(remote_delta.system.query(query => 'SELECT orderkey FROM delta.smoke.orders WHERE ds = ''2026-01-01'''))"
# Delegated subtrees collapse into a query relation scan
regexp_filter_plan_sql="EXPLAIN SELECT regexp_extract(status, '^(O|C)', 1) FROM remote_delta.smoke.orders WHERE regexp_like(status, '^(OPEN|CLOSED)$') AND ds = '2026-01-01'"
assert_query_contains "remote_delta:Query[" "${regexp_filter_plan_sql}"
assert_query_not_contains "ScanFilterProject" "${regexp_filter_plan_sql}"
assert_query_contains "remote_delta:Query[" "EXPLAIN SELECT o.orderkey, c.name FROM remote_delta.smoke.orders o JOIN remote_delta.smoke.customers c ON o.custkey = c.custkey"

echo "Delta smoke test passed"
