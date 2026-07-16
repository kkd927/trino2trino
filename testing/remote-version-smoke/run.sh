#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/testing/remote-version-smoke/docker-compose.yml"

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
[[ "${#remote_versions[@]}" -gt 0 ]] || fail "usage: testing/remote-version-smoke/run.sh REMOTE_VERSION [REMOTE_VERSION ...]"

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
  local project_name="${REMOTE_VERSION_SMOKE_PROJECT_NAME:-trino2trino-remote-version-${local_version}-to-${remote_version}}"
  local log_base="${REMOTE_VERSION_SMOKE_LOG_BASE:-${repo_root}/target/remote-version-smoke}"
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
    echo "Remote version smoke diagnostics written to ${log_dir}"
  }

  finish() {
    local exit_code=$?

    if [[ "${compose_touched}" == "true" ]]; then
      if [[ "${exit_code}" -ne 0 ]]; then
        capture_logs failure
      elif [[ "${REMOTE_VERSION_SMOKE_ALWAYS_LOGS:-false}" == "true" ]]; then
        capture_logs success
      fi

      if [[ "${REMOTE_VERSION_SMOKE_KEEP_RUNNING:-false}" != "true" ]]; then
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
    local attempts="${REMOTE_VERSION_SMOKE_WAIT_ATTEMPTS:-90}"

    for attempt in $(seq 1 "${attempts}"); do
      if compose exec -T "${service}" \
          curl --fail --silent --show-error --max-time 2 http://localhost:8080/v1/info 2>/dev/null |
          grep -Eq '"starting"[[:space:]]*:[[:space:]]*false'; then
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

  assert_output_equals() {
    local expected="$1"
    local context="$2"
    local actual="$3"

    if [[ "${actual}" != "${expected}" ]]; then
      echo "Assertion failed"
      echo "Context: ${context}"
      echo "Expected: ${expected}"
      echo "Actual: ${actual}"
      exit 1
    fi
  }

  assert_output_contains() {
    local needle="$1"
    local context="$2"
    local actual="$3"

    if [[ "${actual}" != *"${needle}"* ]]; then
      echo "Assertion failed"
      echo "Context: ${context}"
      echo "Expected output to contain: ${needle}"
      echo "Actual output:"
      echo "${actual}"
      exit 1
    fi
  }

  assert_output_not_contains() {
    local needle="$1"
    local context="$2"
    local actual="$3"

    if [[ "${actual}" == *"${needle}"* ]]; then
      echo "Assertion failed"
      echo "Context: ${context}"
      echo "Expected output not to contain: ${needle}"
      echo "Actual output:"
      echo "${actual}"
      exit 1
    fi
  }

  assert_marked_output_equals() {
    local expected="$1"
    local marker="$2"
    local context="$3"
    local actual="$4"
    local marked_output

    marked_output="$(printf '%s\n' "${actual}" | awk -F '\t' -v marker="${marker}" '$1 == marker')"
    assert_output_equals "${expected}" "${context}" "${marked_output}"
  }

  output_between_markers() {
    local output="$1"
    local start_marker="$2"
    local end_marker="$3"

    [[ "${output}" == *"${start_marker}"* ]] || fail "missing output marker: ${start_marker}"
    output="${output#*"${start_marker}"}"
    [[ "${output}" == *"${end_marker}"* ]] || fail "missing output marker: ${end_marker}"
    printf '%s' "${output%%"${end_marker}"*}"
  }

  echo "Starting remote version smoke stack for local Trino ${local_version} -> remote Trino ${remote_version}"
  compose_touched=true
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d

  wait_for_trino trino-remote-version "remote Trino ${remote_version}"
  wait_for_trino trino-local-version "local Trino ${local_version}"

  echo "Running TPCH remote federation assertions"
  local tpch_output
  tpch_output="$(trino_exec trino-local-version "
    SHOW SCHEMAS FROM remote_tpch;
    SHOW TABLES FROM remote_tpch.tiny;
    DESCRIBE remote_tpch.tiny.orders;
    SELECT '__nation_count__', count(*) FROM remote_tpch.tiny.nation;
    SELECT '__nation_name__', name FROM remote_tpch.tiny.nation WHERE nationkey = 1;
    SELECT '__america_join__', n.nationkey, n.name FROM remote_tpch.tiny.nation n JOIN remote_tpch.tiny.region r ON n.regionkey = r.regionkey WHERE r.name = 'AMERICA' ORDER BY n.nationkey LIMIT 3;
    SELECT '__region_counts__', array_join(array_agg(CAST(regionkey AS VARCHAR) || ':' || CAST(nation_count AS VARCHAR) ORDER BY regionkey), ',') FROM (SELECT regionkey, count(*) AS nation_count FROM remote_tpch.tiny.nation GROUP BY regionkey);
    SELECT '__remote_join_count__', count(*) FROM remote_tpch.tiny.nation n JOIN remote_tpch.tiny.region r ON n.regionkey = r.regionkey;
    SELECT '__local_remote_join_count__', count(*) FROM (VALUES BIGINT '1', BIGINT '3') AS local_keys(nationkey) JOIN remote_tpch.tiny.nation n ON n.nationkey = local_keys.nationkey;
    SELECT '__passthrough__', result.* FROM TABLE(remote_tpch.system.query(query => 'SELECT count(*) FROM tpch.tiny.nation')) AS result;
  ")"
  # SHOW output has no labels in TSV mode. Assert the complete boundary from the
  # final schema through the full tiny table list and the first DESCRIBE row so
  # later query labels cannot accidentally satisfy the metadata check.
  assert_output_contains $'tiny\ncustomer\nlineitem\nnation\norders\npart\npartsupp\nregion\nsupplier\norderkey\tbigint' "TPCH schemas, tables, and orders metadata" "${tpch_output}"
  assert_output_contains $'orderdate\tdate' "TPCH orders metadata" "${tpch_output}"
  assert_marked_output_equals $'__nation_count__\t25' "__nation_count__" "TPCH nation count" "${tpch_output}"
  assert_marked_output_equals $'__nation_name__\tARGENTINA' "__nation_name__" "TPCH nation lookup" "${tpch_output}"
  assert_marked_output_equals $'__america_join__\t1\tARGENTINA\n__america_join__\t2\tBRAZIL\n__america_join__\t3\tCANADA' "__america_join__" "TPCH filtered join" "${tpch_output}"
  assert_marked_output_equals $'__region_counts__\t0:5,1:5,2:5,3:5,4:5' "__region_counts__" "TPCH grouped aggregation" "${tpch_output}"
  assert_marked_output_equals $'__remote_join_count__\t25' "__remote_join_count__" "TPCH remote join" "${tpch_output}"
  assert_marked_output_equals $'__local_remote_join_count__\t2' "__local_remote_join_count__" "TPCH local-to-remote join" "${tpch_output}"
  assert_marked_output_equals $'__passthrough__\t25' "__passthrough__" "TPCH passthrough query" "${tpch_output}"

  echo "Running timestamp with time zone transport assertions"
  local tstz_values_sql
  local tstz_expected
  tstz_values_sql="SELECT CAST(TIMESTAMP '2022-07-15 10:30:45.123 Europe/Warsaw' AS TIMESTAMP(3) WITH TIME ZONE) AS p3_region, CAST(TIMESTAMP '2022-07-15 10:30:45.123456789012 Europe/Warsaw' AS TIMESTAMP(12) WITH TIME ZONE) AS p12_region, CAST(TIMESTAMP '2022-07-15 10:30:45.123 +05:30' AS TIMESTAMP(3) WITH TIME ZONE) AS fixed_offset, CAST(at_timezone(TIMESTAMP '1890-01-01 00:00:00.000 UTC', 'Europe/Paris') AS TIMESTAMP(3) WITH TIME ZONE) AS historical_zone"
  trino_exec trino-remote-version "DROP TABLE IF EXISTS memory.default.remote_version_tstz_probe; CREATE TABLE memory.default.remote_version_tstz_probe AS ${tstz_values_sql}" >/dev/null
  tstz_expected="$(trino_exec trino-local-version "${tstz_values_sql}")"
  # Keep the remote columns raw so the connector's TSTZ transport projection is exercised.
  local tstz_actual
  tstz_actual="$(trino_exec trino-local-version "SELECT p3_region, p12_region, fixed_offset, historical_zone FROM remote_memory.default.remote_version_tstz_probe")"
  assert_output_equals "${tstz_expected}" "raw timestamp with time zone transport" "${tstz_actual}"
  # Exercise the predicate write mappings against the old remote, not only the read projection.
  local tstz_binding_sql
  tstz_binding_sql="SELECT '__binding__', date_diff('second', p12_region, CAST(TIMESTAMP '2022-07-15 10:30:45.123456789012 Europe/Warsaw' AS TIMESTAMP(12) WITH TIME ZONE)) FROM remote_memory.default.remote_version_tstz_probe WHERE p3_region = CAST(TIMESTAMP '2022-07-15 10:30:45.123 Europe/Warsaw' AS TIMESTAMP(3) WITH TIME ZONE) AND p12_region >= CAST(TIMESTAMP '2022-07-15 10:30:45.123456789012 Europe/Warsaw' AS TIMESTAMP(12) WITH TIME ZONE) AND p12_region < CAST(TIMESTAMP '2022-07-15 10:30:46.123456789012 Europe/Warsaw' AS TIMESTAMP(12) WITH TIME ZONE)"
  local tstz_binding_output
  tstz_binding_output="$(trino_exec trino-local-version "
    ${tstz_binding_sql};
    EXPLAIN ${tstz_binding_sql};
  ")"
  assert_marked_output_equals $'__binding__\t0' "__binding__" "TSTZ predicate and two-parameter constant binding" "${tstz_binding_output}"
  assert_output_contains "remote_memory:Query[" "TSTZ delegated projection plan" "${tstz_binding_output}"
  assert_output_not_contains "ScanFilterProject" "TSTZ delegated predicate and projection plan" "${tstz_binding_output}"

  echo "Running CHAR/VARCHAR remote delegation assertions"
  trino_exec trino-remote-version "DROP TABLE IF EXISTS memory.default.remote_version_char_probe; CREATE TABLE memory.default.remote_version_char_probe AS SELECT * FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  ')) AS t(id, c, v)" >/dev/null
  local char_expected
  local char_actual
  char_expected="$(trino_exec trino-local-version "
    SELECT '__char_cast__';
    SELECT '[' || CAST(CAST('a' AS CHAR(3)) AS VARCHAR) || ']', length(CAST(CAST('a' AS CHAR(3)) AS VARCHAR));
    SELECT '__char_varchar_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  ')) AS t(id, c, v) WHERE v = c ORDER BY id;
    SELECT '__char_constant_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  ')) AS t(id, c, v) WHERE c = CAST('a' AS CHAR(3)) ORDER BY id;
    SELECT '__varchar_char_constant_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  ')) AS t(id, c, v) WHERE v = CAST('a' AS CHAR(3)) ORDER BY id;
  ")"
  char_actual="$(trino_exec trino-local-version "
    SELECT '__char_cast__';
    SELECT '[' || CAST(c AS VARCHAR) || ']', length(CAST(c AS VARCHAR)) FROM remote_memory.default.remote_version_char_probe WHERE id = 1;
    SELECT '__char_varchar_predicate__';
    SELECT id FROM remote_memory.default.remote_version_char_probe WHERE v = c ORDER BY id;
    SELECT '__char_constant_predicate__';
    SELECT id FROM remote_memory.default.remote_version_char_probe WHERE c = CAST('a' AS CHAR(3)) ORDER BY id;
    SELECT '__varchar_char_constant_predicate__';
    SELECT id FROM remote_memory.default.remote_version_char_probe WHERE v = CAST('a' AS CHAR(3)) ORDER BY id;
  ")"
  assert_output_equals "${char_expected}" "CHAR/VARCHAR local-versus-remote semantics" "${char_actual}"
  if [[ "${local_version}" -ge 482 && "${remote_version}" -lt 482 ]]; then
    local char_explain_output
    char_explain_output="$(trino_exec trino-local-version "
      SELECT '__char_cast_plan__';
      EXPLAIN SELECT '[' || CAST(c AS VARCHAR) || ']', length(CAST(c AS VARCHAR)) FROM remote_memory.default.remote_version_char_probe WHERE id = 1;
      SELECT '__char_predicate_plan__';
      EXPLAIN SELECT id FROM remote_memory.default.remote_version_char_probe WHERE v = c ORDER BY id;
      SELECT '__char_plan_end__';
    ")"
    local char_cast_plan
    local char_predicate_plan
    char_cast_plan="$(output_between_markers "${char_explain_output}" "__char_cast_plan__" "__char_predicate_plan__")"
    char_predicate_plan="$(output_between_markers "${char_explain_output}" "__char_predicate_plan__" "__char_plan_end__")"
    assert_output_contains "trim(TRAILING ' ' FROM" "CHAR cast plan" "${char_cast_plan}"
    assert_output_contains "trim(TRAILING ' ' FROM" "CHAR/VARCHAR predicate plan" "${char_predicate_plan}"
  fi

  echo "Remote version smoke test passed for local Trino ${local_version} -> remote Trino ${remote_version}"
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
  echo "${failures} remote version smoke probe(s) failed"
  exit 1
fi
