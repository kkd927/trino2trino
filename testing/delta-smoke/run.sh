#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/testing/delta-smoke/docker-compose.yml"
project_name="${DELTA_SMOKE_PROJECT_NAME:-trino2trino-delta-smoke}"
plugin_dir="${repo_root}/target/trino-trino-481"

if [[ ! -d "${plugin_dir}" ]]; then
  echo "Missing ${plugin_dir}. Build the plugin first:"
  echo "  mvn -B -Dair.check.skip-all=true -DskipTests package"
  exit 1
fi

compose() {
  docker compose -p "${project_name}" -f "${compose_file}" "$@"
}

cleanup() {
  if [[ "${DELTA_SMOKE_KEEP_RUNNING:-false}" != "true" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

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

echo "Starting Delta smoke stack"
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
assert_query_equals "3" "SELECT count(*) FROM remote_delta.smoke.customers"
assert_query_equals "4" "SELECT count(*) FROM remote_delta.smoke.orders"
assert_query_equals "131.75" "SELECT CAST(sum(totalprice) AS VARCHAR) FROM remote_delta.smoke.orders WHERE ds = '2026-01-01'"
assert_query_equals "orderkey:bigint,custkey:bigint,totalprice:decimal(18,2),orderdate:date,status:varchar,ds:varchar" "SELECT array_join(array_agg(column_name || ':' || data_type ORDER BY ordinal_position), ',') FROM remote_delta.information_schema.columns WHERE table_schema = 'smoke' AND table_name = 'orders'"
assert_query_equals "2	gold	7	12345678901234567890.1234" "SELECT cardinality(tags), element_at(attrs, 'tier'), detail.qty, CAST(large_amount AS VARCHAR) FROM remote_delta.smoke.complex_types WHERE id = 1"
assert_query_equals "3" "SELECT count(*) FROM (VALUES BIGINT '1', BIGINT '3') AS local_keys(custkey) JOIN remote_delta.smoke.orders o ON o.custkey = local_keys.custkey"
assert_query_equals "3" "SELECT count(*) FROM remote_delta.smoke.orders o JOIN remote_delta.smoke.customers c ON o.custkey = c.custkey WHERE c.region IN ('AMERICA', 'ASIA')"
assert_query_equals "2" "SELECT count(*) FROM TABLE(remote_delta.system.query(query => 'SELECT orderkey FROM delta.smoke.orders WHERE ds = ''2026-01-01'''))"
assert_query_contains "RemoteTrinoQuery[catalog=delta, delegated=true]" "EXPLAIN SELECT regexp_extract(status, '^(O|C)', 1) FROM remote_delta.smoke.orders WHERE regexp_like(status, '^(OPEN|CLOSED)$') AND ds = '2026-01-01'"
assert_query_contains "RemoteTrinoQuery[catalog=delta" "EXPLAIN SELECT o.orderkey, c.name FROM remote_delta.smoke.orders o JOIN remote_delta.smoke.customers c ON o.custkey = c.custkey"

echo "Delta smoke test passed"
