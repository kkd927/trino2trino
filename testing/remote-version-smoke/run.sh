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
[[ "${#remote_versions[@]}" -gt 0 ]] || fail "usage: testing/remote-version-smoke/run.sh REMOTE_VERSION[-legacy-settings] [REMOTE_VERSION ...]"

plugin_dir="${repo_root}/target/trino-trino-${local_version}"
if [[ -f "${plugin_dir}.zip" ]]; then
  echo "Preparing smoke-test plugin from ${plugin_dir}.zip"
  rm -rf "${plugin_dir}"
  unzip -q "${plugin_dir}.zip" -d "${repo_root}/target"
fi

if [[ ! -d "${plugin_dir}" ]]; then
  echo "Missing ${plugin_dir}.zip or ${plugin_dir}. Build the plugin first:"
  echo "  mvn -B clean verify"
  exit 1
fi

# A remote version with the suffix -legacy-settings stands for a remote server of the
# local version that evaluates decimal arithmetic, regular expressions, and casts from
# char to varchar as an earlier release does. Only the local version has all three settings.
for remote_target in "${remote_versions[@]}"; do
  validate_version "remote version" "${remote_target%-legacy-settings}"
  if [[ "${remote_target}" == *-legacy-settings && "${remote_target%-legacy-settings}" != "${local_version}" ]]; then
    fail "the suffix -legacy-settings requires the local version ${local_version}: ${remote_target}"
  fi
done

run_version() (
  set -euo pipefail

  local remote_target="$1"
  local remote_version="${remote_target%-legacy-settings}"
  local project_name="${REMOTE_VERSION_SMOKE_PROJECT_NAME:-trino2trino-remote-version-${local_version}-to-${remote_target}}"
  local log_base="${REMOTE_VERSION_SMOKE_LOG_BASE:-${repo_root}/target/remote-version-smoke}"
  local log_dir="${log_base}/${local_version}-to-${remote_target}"
  local compose_touched=false
  # The connector sends functions, casts, and arithmetic only to a remote server that
  # reports the version of the local server and evaluates them with default settings.
  # Every other server receives the basic list, and this script then expects what it
  # expects for a remote server of another version.
  local same_version=false
  local remote_config=common
  if [[ "${remote_target}" == *-legacy-settings ]]; then
    remote_config=legacy-settings
  elif [[ "${remote_version}" == "${local_version}" ]]; then
    same_version=true
  fi

  compose() {
    LOCAL_TRINO_VERSION="${local_version}" REMOTE_TRINO_VERSION="${remote_version}" REMOTE_TRINO_CONFIG="${remote_config}" \
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

  # Runs the statements from standard input. A command argument cannot hold more than
  # 128 KiB on Linux.
  trino_exec_stdin() {
    local service="$1"

    compose exec -T "${service}" trino \
      --server localhost:8080 \
      --user trino \
      --output-format TSV \
      --file /dev/stdin | tr -d '\r'
  }

  # Runs every statement, prints the rows of the statements that succeed, and drops the
  # error messages, which carry query IDs
  trino_exec_ignoring_errors() {
    local service="$1"
    local sql="$2"

    compose exec -T "${service}" trino \
      --server localhost:8080 \
      --user trino \
      --output-format TSV \
      --ignore-errors \
      --execute "${sql}" 2>/dev/null | tr -d '\r' || true
  }

  assert_query_fails() {
    local service="$1"
    local sql="$2"
    local expected_message="$3"
    local context="$4"
    local output

    if output="$(trino_exec "${service}" "${sql}" 2>&1)"; then
      echo "Assertion failed"
      echo "Context: ${context}"
      echo "Expected the query to fail with: ${expected_message}"
      echo "Actual output:"
      echo "${output}"
      exit 1
    fi
    assert_output_contains "${expected_message}" "${context}" "${output}"
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

  # Runs the statements through the connector, with delegation enabled and disabled, and
  # requires the rows of the same statements on the tables of the local memory catalog.
  # The third argument holds the local statements when they differ in more than the
  # catalog.
  assert_local_evaluation() {
    local context="$1"
    local sql="$2"
    local local_sql="${3:-$2}"
    local expected actual delegation

    expected="$(trino_exec_ignoring_errors trino-local-version "${local_sql//remote_memory.default./memory.default.}")"
    [[ -n "${expected}" ]] || fail "local evaluation produced no rows: ${context}"
    for delegation in true false; do
      actual="$(trino_exec_ignoring_errors trino-local-version "SET SESSION remote_memory.remote_delegation_enabled = ${delegation}; ${sql}" | grep -v '^SET SESSION$' || true)"
      if [[ "${actual}" != "${expected}" ]]; then
        echo "Assertion failed"
        echo "Context: ${context} must return the rows of local evaluation, delegation=${delegation}"
        diff <(printf '%s\n' "${expected}") <(printf '%s\n' "${actual}") || true
        exit 1
      fi
    done
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

  echo "Starting remote version smoke stack for local Trino ${local_version} -> remote Trino ${remote_target}"
  compose_touched=true
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  compose up -d

  wait_for_trino trino-remote-version "remote Trino ${remote_target}"
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

  echo "Running basic list pushdown assertions"
  # The basic list is what the connector pushes down to a remote server of any version.
  # Every statement runs through the connector and on a copy of the table in the local
  # memory catalog, and must return the same rows. The plan of every statement must
  # show that the remote server evaluates it.
  local basic_name basic_type basic_operations basic_values basic_table basic_value basic_left basic_right
  local basic_rows basic_id basic_first basic_second basic_third basic_filters basic_aggregates
  local basic_create_sql="" basic_local_sql="" basic_connector_sql="" basic_explain_sql=""
  local -a basic_kinds=() basic_statements=() basic_raw_values=() basic_typed_values=()
  add_basic_statement() {
    basic_kinds+=("$1")
    basic_statements+=("$2")
  }
  while IFS='|' read -r basic_name basic_type basic_operations basic_values; do
    [[ -n "${basic_name}" && "${basic_name}" != \#* ]] || continue
    IFS=';' read -ra basic_raw_values <<< "${basic_values}"
    basic_typed_values=("CAST(NULL AS ${basic_type})")
    for basic_value in "${basic_raw_values[@]}"; do
      basic_value="${basic_value#"${basic_value%%[![:space:]]*}"}"
      basic_typed_values+=("CAST(${basic_value} AS ${basic_type})")
    done
    basic_rows=""
    basic_id=0
    for basic_left in "${basic_typed_values[@]}"; do
      for basic_right in "${basic_typed_values[@]}"; do
        basic_id=$((basic_id + 1))
        basic_rows+="${basic_rows:+, }(${basic_id}, ${basic_left}, ${basic_right})"
      done
    done
    basic_create_sql+="CREATE TABLE memory.default.basic_${basic_name} AS SELECT * FROM (VALUES ${basic_rows}) AS t(id, a, b);"$'\n'

    # @@ stands for the catalog: remote_memory through the connector, memory locally
    basic_table="@@.default.basic_${basic_name}"
    basic_first="${basic_typed_values[1]}"
    basic_second="${basic_typed_values[2]}"
    basic_third="${basic_typed_values[3]:-${basic_first}}"
    basic_filters="SELECT 'not_equal' AS op, id FROM ${basic_table} WHERE a <> b
      UNION ALL SELECT 'unknown', id FROM ${basic_table} WHERE (a <> b) IS NULL"
    if [[ ",${basic_operations}," == *,equal,* ]]; then
      basic_filters+="
      UNION ALL SELECT 'equal', id FROM ${basic_table} WHERE a = b
      UNION ALL SELECT 'distinct', id FROM ${basic_table} WHERE a IS DISTINCT FROM b
      UNION ALL SELECT 'not_distinct', id FROM ${basic_table} WHERE a IS NOT DISTINCT FROM b"
    fi
    if [[ ",${basic_operations}," == *,equal,* && ",${basic_operations}," == *,filter,* ]]; then
      basic_filters+="
      UNION ALL SELECT 'not', id FROM ${basic_table} WHERE NOT (a = b OR b IS NULL)"
    fi
    if [[ ",${basic_operations}," == *,equal,* && ",${basic_operations}," == *,const,* ]]; then
      basic_filters+="
      UNION ALL SELECT 'constant_equal', id FROM ${basic_table} WHERE a = ${basic_first} OR b IS NULL"
    fi
    if [[ ",${basic_operations}," == *,order,* ]]; then
      basic_filters+="
      UNION ALL SELECT 'less', id FROM ${basic_table} WHERE a < b
      UNION ALL SELECT 'less_or_equal', id FROM ${basic_table} WHERE a <= b
      UNION ALL SELECT 'greater', id FROM ${basic_table} WHERE a > b
      UNION ALL SELECT 'greater_or_equal', id FROM ${basic_table} WHERE a >= b"
    fi
    if [[ ",${basic_operations}," == *,order,* && ",${basic_operations}," == *,const,* ]]; then
      basic_filters+="
      UNION ALL SELECT 'constant_less', id FROM ${basic_table} WHERE a < ${basic_second} OR b IS NULL"
    fi
    if [[ ",${basic_operations}," == *,in,* ]]; then
      basic_filters+="
      UNION ALL SELECT 'in', id FROM ${basic_table} WHERE a IN (b, ${basic_first})
      UNION ALL SELECT 'constant_in', id FROM ${basic_table} WHERE a IN (${basic_first}, ${basic_second}, ${basic_third}) OR b IS NULL"
    fi
    if [[ ",${basic_operations}," == *,in,* && ",${basic_operations}," == *,filter,* ]]; then
      basic_filters+="
      UNION ALL SELECT 'not_in', id FROM ${basic_table} WHERE a NOT IN (b, ${basic_first})"
    fi
    add_basic_statement filter "SELECT '__basic_${basic_name}_filter__', op, id FROM (${basic_filters}) ORDER BY op, id"

    # A filter that compares one column with constants is a column filter. The base JDBC
    # query builder renders it, and the checks of the basic list do not see it.
    if [[ ",${basic_operations}," == *,filter,* ]]; then
      basic_filters="SELECT 'column_equal' AS op, id FROM ${basic_table} WHERE a = ${basic_first}
      UNION ALL SELECT 'column_less', id FROM ${basic_table} WHERE a < ${basic_second}"
      if [[ ",${basic_operations}," == *,in,* ]]; then
        basic_filters+="
      UNION ALL SELECT 'column_in', id FROM ${basic_table} WHERE a IN (${basic_first}, ${basic_second}, ${basic_third})"
      fi
      add_basic_statement filter "SELECT '__basic_${basic_name}_column_filter__', op, id FROM (${basic_filters}) ORDER BY op, id"
    fi

    basic_aggregates="count(*), count(a), count_if(a <> b)"
    if [[ ",${basic_operations}," == *,equal,* ]]; then
      basic_aggregates+=", count_if(a = b)"
    fi
    if [[ ",${basic_operations}," == *,order,* ]]; then
      basic_aggregates+=", min(a), max(a)"
    fi
    if [[ ",${basic_operations}," == *,sum,* ]]; then
      basic_aggregates+=", sum(a)"
    fi
    if [[ ",${basic_operations}," == *,avg,* ]]; then
      basic_aggregates+=", avg(a)"
    fi
    add_basic_statement aggregate "SELECT '__basic_${basic_name}_aggregate__', ${basic_aggregates}, checksum(a) FROM ${basic_table}"
    add_basic_statement aggregate "SELECT '__basic_${basic_name}_distinct__', count(DISTINCT a) FROM ${basic_table}"
    # The key of a group is one of its equal values, such as 0.0 or -0.0, so it is not selected
    add_basic_statement aggregate "SELECT '__basic_${basic_name}_group__', count(*), count(b) FROM ${basic_table} GROUP BY a ORDER BY 2, 3"
    add_basic_statement aggregate "SELECT '__basic_${basic_name}_group_distinct__', count(DISTINCT b) FROM ${basic_table} GROUP BY a ORDER BY 2"
    if [[ ",${basic_operations}," == *,join,* ]]; then
      add_basic_statement join "SELECT '__basic_${basic_name}_join__', count(*), sum(l.id), sum(r.id) FROM ${basic_table} l JOIN ${basic_table} r ON l.a = r.b"
      add_basic_statement join "SELECT '__basic_${basic_name}_left_join__', count(*), sum(l.id), sum(r.id) FROM ${basic_table} l LEFT JOIN ${basic_table} r ON l.a = r.b"
      add_basic_statement join "SELECT '__basic_${basic_name}_right_join__', count(*), sum(l.id), sum(r.id) FROM ${basic_table} l RIGHT JOIN ${basic_table} r ON l.a = r.b"
    fi
    if [[ ",${basic_operations}," == *,order,* ]]; then
      add_basic_statement topn "SELECT '__basic_${basic_name}_topn__', id FROM ${basic_table} ORDER BY a DESC NULLS LAST, id LIMIT 9"
    fi
    if [[ ",${basic_operations}," == *,order,* && ",${basic_operations}," == *,join,* ]]; then
      # The engine pushes down a join only when it has an equality condition
      add_basic_statement join "SELECT '__basic_${basic_name}_ordering_join__', count(*), sum(l.id), sum(r.id) FROM ${basic_table} l JOIN ${basic_table} r ON l.a = r.a AND l.b < r.b"
    fi
  done < "${repo_root}/testing/remote-version-smoke/basic-pushdown-types.txt"
  [[ "${#basic_statements[@]}" -gt 0 ]] || fail "no basic list statements"

  trino_exec_stdin trino-remote-version <<< "${basic_create_sql}" >/dev/null
  trino_exec_stdin trino-local-version <<< "${basic_create_sql}" >/dev/null

  # Replace the catalog in each statement: bash 3.2 takes minutes to replace it in the
  # joined statements
  local basic_index basic_statement
  for basic_index in "${!basic_statements[@]}"; do
    basic_statement="${basic_statements[basic_index]}"
    basic_local_sql+="${basic_statement//@@/memory};"$'\n'
    basic_connector_sql+="${basic_statement//@@/remote_memory};"$'\n'
    basic_explain_sql+="SELECT '__basic_plan_${basic_index}__'; EXPLAIN ${basic_statement//@@/remote_memory};"$'\n'
  done
  basic_explain_sql+="SELECT '__basic_plan_${#basic_statements[@]}__';"

  # Without delegation the baseline JDBC rewriter renders the statements, in another form
  local basic_expected basic_actual basic_delegation
  basic_expected="$(trino_exec_stdin trino-local-version <<< "${basic_local_sql}")"
  [[ -n "${basic_expected}" ]] || fail "basic list statements produced no rows"
  for basic_delegation in true false; do
    basic_actual="$(trino_exec_stdin trino-local-version <<< "SET SESSION remote_memory.remote_delegation_enabled = ${basic_delegation};"$'\n'"${basic_connector_sql}" | grep -v '^SET SESSION$' || true)"
    if [[ "${basic_actual}" != "${basic_expected}" ]]; then
      echo "Assertion failed"
      echo "Context: the basic list must return the rows of local evaluation, delegation=${basic_delegation}"
      diff <(printf '%s\n' "${basic_expected}") <(printf '%s\n' "${basic_actual}") || true
      exit 1
    fi
  done

  local basic_plans basic_plan
  basic_plans="$(trino_exec_stdin trino-local-version <<< "${basic_explain_sql}")"
  # Bash 3.2 searches a long string much faster by bytes than by characters
  local basic_locale="${LC_ALL:-}"
  LC_ALL=C
  basic_plans="${basic_plans#*"__basic_plan_0__"}"
  for basic_index in "${!basic_statements[@]}"; do
    [[ "${basic_plans}" == *"__basic_plan_$((basic_index + 1))__"* ]] || fail "missing plan of basic list statement ${basic_index}"
    basic_plan="${basic_plans%%"__basic_plan_$((basic_index + 1))__"*}"
    basic_plans="${basic_plans#*"__basic_plan_$((basic_index + 1))__"}"
    case "${basic_kinds[basic_index]}" in
      filter)
        assert_output_not_contains "filterPredicate" "basic list filter pushdown: ${basic_statements[basic_index]}" "${basic_plan}"
        ;;
      aggregate)
        assert_output_not_contains "Aggregate" "basic list aggregation pushdown: ${basic_statements[basic_index]}" "${basic_plan}"
        ;;
      topn)
        assert_output_contains "sortOrder=" "basic list TopN pushdown: ${basic_statements[basic_index]}" "${basic_plan}"
        ;;
      join)
        # The engine does not push an aggregation into a join that it pushed down
        assert_output_not_contains "Join[" "basic list join pushdown: ${basic_statements[basic_index]}" "${basic_plan}"
        ;;
    esac
  done
  LC_ALL="${basic_locale}"

  # Servers before 484 compare the values of a dynamic filter on a real column by their
  # raw form, so an equality join there misses the rows whose zero has the other sign.
  # Only a remote server of the local version evaluates such a join. A join that the
  # local server evaluates sends its dynamic filter to the remote server as a list of
  # values, which a server before 484 compares in the same way. The connector does not
  # work around bugs of a remote release, so the rows are compared only for a remote
  # server of the local release.
  local real_join_sql="SELECT '__real_join__', count(*) FROM remote_memory.default.basic_real l JOIN remote_memory.default.basic_real r ON l.a = r.b"
  if [[ "${remote_version}" == "${local_version}" ]]; then
    assert_local_evaluation "equality join on real values with zeros of both signs" "${real_join_sql};"
  fi
  local real_join_plan
  real_join_plan="$(trino_exec trino-local-version "EXPLAIN ${real_join_sql}")"
  if [[ "${same_version}" == "true" ]]; then
    assert_output_not_contains "Join[" "equality join on real values, pushed down to a remote of the local version" "${real_join_plan}"
  else
    assert_output_contains "Join[" "equality join on real values stays local for another remote version" "${real_join_plan}"
  fi

  # The base JDBC query builder renders the values other than a few listed ones as
  # NOT (x = ?) or NOT (x IN (...)), which NaN passes on every version. Such a filter on a
  # real or double column must stay local. Ranges whose complement is a range stay remote.
  local nan_table_sql="CREATE TABLE memory.default.remote_version_nan_probe AS SELECT * FROM (VALUES (1, 5e0, REAL '5'), (2, nan(), CAST(nan() AS real)), (3, 1e0, REAL '1'), (4, 0e0, REAL '0')) AS t(id, d, r)"
  trino_exec trino-remote-version "${nan_table_sql}" >/dev/null
  trino_exec trino-local-version "${nan_table_sql}" >/dev/null
  assert_local_evaluation "filters that NaN passes" "
    SELECT '__double_other_than_one__', id FROM remote_memory.default.remote_version_nan_probe WHERE d < 1 OR d > 1 ORDER BY id;
    SELECT '__real_other_than_one__', id FROM remote_memory.default.remote_version_nan_probe WHERE r < REAL '1' OR r > REAL '1' ORDER BY id;
    SELECT '__double_other_than_two_values__', id FROM remote_memory.default.remote_version_nan_probe WHERE d < 1 OR (d > 1 AND d < 5) OR d > 5 ORDER BY id;
    SELECT '__double_outside_a_range__', id FROM remote_memory.default.remote_version_nan_probe WHERE d < 1 OR d > 2 ORDER BY id;
  "
  local nan_plan
  nan_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_nan_probe WHERE d < 1 OR d > 1")"
  assert_output_contains "filterPredicate" "a filter on a double column that NaN passes stays local" "${nan_plan}"
  nan_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_nan_probe WHERE d < 1 OR d > 2")"
  assert_output_not_contains "filterPredicate" "range filters on a double column stay on the remote server" "${nan_plan}"

  # Servers before 484 drop the unmatched row of an input with exactly one row when the
  # condition of a FULL JOIN refers only to the other input. Only a remote server of the
  # local version evaluates a FULL JOIN. The catalog uses EAGER join pushdown.
  local full_join_sql="SELECT '__full_join__', r.b FROM TABLE(remote_memory.system.query(query => 'SELECT 1 AS a')) l FULL JOIN TABLE(remote_memory.system.query(query => 'SELECT 2 AS b')) r ON r.b < 0 ORDER BY 2"
  assert_local_evaluation "FULL JOIN with an input of one row" "${full_join_sql};" "SELECT '__full_join__', r.b FROM (SELECT 1 AS a) l FULL JOIN (SELECT 2 AS b) r ON r.b < 0 ORDER BY 2;"
  local full_join_plan
  full_join_plan="$(trino_exec trino-local-version "EXPLAIN ${full_join_sql}")"
  if [[ "${same_version}" == "true" ]]; then
    assert_output_not_contains "Join[" "FULL JOIN, pushed down to a remote of the local version" "${full_join_plan}"
  else
    assert_output_contains "Join[" "FULL JOIN stays local for another remote version" "${full_join_plan}"
  fi

  echo "Running geospatial remote SQL passthrough assertions"
  local geospatial_passthrough
  geospatial_passthrough="$(trino_exec trino-local-version "
    SELECT '__geospatial_passthrough__', wkt, spherical_wkt
    FROM TABLE(remote_memory.system.query(query =>
      'SELECT ST_AsText(ST_Point(1.0, 2.0)) AS wkt, ST_AsText(to_geometry(to_spherical_geography(ST_Point(1.0, 2.0)))) AS spherical_wkt'))
  ")"
  assert_output_equals $'__geospatial_passthrough__\tPOINT (1 2)\tPOINT (1 2)' "remote geospatial SQL passthrough" "${geospatial_passthrough}"

  if (( 10#${local_version} >= 481 && 10#${remote_version} >= 481 )); then
    echo "Running native geospatial EWKB transport assertions"
    # Exercise the 2D EWKB format shared by 481+; 483's Z preservation is
    # covered by the same-version integration tests.
    local point_srid_ewkb="0101000020E6100000000000000000F03F0000000000000040"
    trino_exec trino-remote-version "
      DROP TABLE IF EXISTS memory.default.remote_version_geospatial_probe;
      CREATE TABLE memory.default.remote_version_geospatial_probe AS
      SELECT ST_GeomFromBinary(X'${point_srid_ewkb}') AS g,
             to_spherical_geography(ST_Point(1.0, 2.0)) AS s,
             ARRAY[ST_GeomFromBinary(X'${point_srid_ewkb}'), CAST(NULL AS Geometry)] AS ga
    " >/dev/null

    local geospatial_native
    geospatial_native="$(trino_exec trino-local-version "
      SELECT '__geospatial_native__', typeof(g), typeof(s), ST_SRID(g), ST_AsText(g),
             ST_AsText(to_geometry(s)), ST_SRID(ga[1]), ga[2] IS NULL
      FROM remote_memory.default.remote_version_geospatial_probe
    ")"
    assert_output_equals $'__geospatial_native__\tGeometry\tSphericalGeography\t4326\tPOINT (1 2)\tPOINT (1 2)\t4326\ttrue' "native and nested geospatial transport" "${geospatial_native}"

    local geospatial_system_query
    geospatial_system_query="$(trino_exec trino-local-version "
      SELECT '__geospatial_system_query__', typeof(g), ST_SRID(g), ST_AsText(g)
      FROM TABLE(remote_memory.system.query(query =>
        'SELECT g FROM memory.default.remote_version_geospatial_probe'))
    ")"
    assert_output_equals $'__geospatial_system_query__\tGeometry\t4326\tPOINT (1 2)' "native geospatial system.query transport" "${geospatial_system_query}"
  else
    echo "Skipping native geospatial transport: EWKB support starts with Trino 481"
  fi

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
  # Functions are delegated only to a server of the local version. Other servers still
  # receive the column filters.
  if [[ "${same_version}" == "true" ]]; then
    assert_output_contains "remote_memory:Query[" "TSTZ delegated projection plan" "${tstz_binding_output}"
  else
    assert_output_not_contains "remote_memory:Query[" "TSTZ projection stays local for another remote version" "${tstz_binding_output}"
  fi
  assert_output_not_contains "ScanFilterProject" "TSTZ delegated predicate plan" "${tstz_binding_output}"

  if (( local_version >= 484 )); then
    echo "Running interval compatibility assertions"
    trino_exec trino-remote-version "
      CREATE TABLE memory.default.remote_version_interval_probe AS
      SELECT * FROM (VALUES
        (1, INTERVAL '1' DAY, INTERVAL '1' YEAR),
        (2, INTERVAL '0' DAY, INTERVAL '0' YEAR),
        (3, INTERVAL '1000' DAY, INTERVAL '1000' YEAR),
        (4, INTERVAL '-1' DAY, INTERVAL '-1' YEAR),
        (5, CAST(NULL AS INTERVAL DAY TO SECOND), CAST(NULL AS INTERVAL YEAR TO MONTH))
      ) AS t(id, ds, ym);
      CREATE TABLE memory.default.remote_version_difference_probe AS
      SELECT TIMESTAMP '2026-01-02 00:00:00.123456789' AS a,
             TIMESTAMP '2026-01-01 00:00:00.000000001' AS b,
             TIME '12:00:00.123456789' AS t, TIME '01:00:00.000000001' AS u;
      CREATE TABLE memory.default.remote_version_subms_probe AS
      SELECT TIMESTAMP '2026-01-01 00:00:00.000001000' AS a,
             TIMESTAMP '2026-01-01 00:00:00.000000000' AS b
    " >/dev/null

    local interval_plan
    interval_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_interval_probe WHERE ds = INTERVAL '1' DAY")"
    if (( remote_version >= 484 )); then
      assert_output_contains "constraint on [ds]" "same-version interval tuple-domain pushdown" "${interval_plan}"
      interval_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_interval_probe WHERE ym > INTERVAL '12' MONTH")"
      assert_output_contains "constraint on [ym]" "same-version year-month tuple-domain pushdown" "${interval_plan}"
    else
      assert_output_contains "ScanFilter" "legacy interval filter stays local" "${interval_plan}"
      local interval_metadata
      interval_metadata="$(trino_exec trino-local-version "DESCRIBE remote_memory.default.remote_version_interval_probe")"
      assert_output_contains $'ds\tinterval day(9) to second(3)' "legacy day-time interval metadata" "${interval_metadata}"
      assert_output_contains $'ym\tinterval year(9) to month' "legacy year-month interval metadata" "${interval_metadata}"
    fi

    local delegation handling interval_output difference_output
    for delegation in true false; do
      interval_output="$(trino_exec trino-local-version "
        SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
        SELECT '__interval_eq__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds = INTERVAL '1' DAY;
        SELECT '__interval_lt__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds < INTERVAL '2' DAY ORDER BY id;
        SELECT '__interval_between__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds BETWEEN INTERVAL '1' DAY AND INTERVAL '2' DAY;
        SELECT '__interval_large__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds > INTERVAL '500' DAY;
        SELECT '__interval_arithmetic__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds + INTERVAL '1' DAY = INTERVAL '2' DAY;
        SELECT '__interval_negative__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds = INTERVAL '-1' DAY;
        SELECT '__interval_null__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds IS NULL;
        SELECT '__interval_in__', id FROM remote_memory.default.remote_version_interval_probe WHERE ds IN (INTERVAL '0' DAY, INTERVAL '1' DAY) ORDER BY id;
        SELECT '__year_month_large__', id FROM remote_memory.default.remote_version_interval_probe WHERE ym > INTERVAL '500' YEAR;
        SELECT '__year_month_arithmetic__', id FROM remote_memory.default.remote_version_interval_probe WHERE ym + INTERVAL '1' YEAR = INTERVAL '2' YEAR;
        SELECT '__subms_predicate__', count(*) FROM remote_memory.default.remote_version_subms_probe WHERE a - b > b - a;
        SELECT '__subms_cast__', CAST(a - b AS VARCHAR) FROM remote_memory.default.remote_version_subms_probe;
      ")"
      assert_marked_output_equals $'__interval_eq__\t1' "__interval_eq__" "interval equality, delegation=${delegation}" "${interval_output}"
      assert_marked_output_equals $'__interval_lt__\t1\n__interval_lt__\t2\n__interval_lt__\t4' "__interval_lt__" "interval range" "${interval_output}"
      assert_marked_output_equals $'__interval_between__\t1' "__interval_between__" "interval BETWEEN" "${interval_output}"
      assert_marked_output_equals $'__interval_large__\t3' "__interval_large__" "interval leading precision" "${interval_output}"
      assert_marked_output_equals $'__interval_arithmetic__\t1' "__interval_arithmetic__" "interval arithmetic" "${interval_output}"
      assert_marked_output_equals $'__interval_negative__\t4' "__interval_negative__" "negative interval" "${interval_output}"
      assert_marked_output_equals $'__interval_null__\t5' "__interval_null__" "NULL interval" "${interval_output}"
      assert_marked_output_equals $'__interval_in__\t1\n__interval_in__\t2' "__interval_in__" "interval IN" "${interval_output}"
      assert_marked_output_equals $'__year_month_large__\t3' "__year_month_large__" "year-month leading precision" "${interval_output}"
      assert_marked_output_equals $'__year_month_arithmetic__\t1' "__year_month_arithmetic__" "year-month arithmetic" "${interval_output}"
      assert_marked_output_equals $'__subms_predicate__\t1' "__subms_predicate__" "timestamp arithmetic without constants, delegation=${delegation}" "${interval_output}"
      assert_marked_output_equals $'__subms_cast__\t0 00:00:00.000001000' "__subms_cast__" "timestamp difference precision" "${interval_output}"

      for handling in IGNORE CONVERT_TO_VARCHAR; do
        difference_output="$(trino_exec trino-local-version "
          SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
          SET SESSION remote_memory.unsupported_type_handling = '${handling}';
          SELECT '__timestamp_difference__', a - b FROM remote_memory.default.remote_version_difference_probe;
          SELECT '__time_difference__', t - u FROM remote_memory.default.remote_version_difference_probe;
          SELECT '__difference_max__', max(a - b) FROM remote_memory.default.remote_version_difference_probe;
          SELECT '__difference_order__', CAST(a - b AS VARCHAR) FROM remote_memory.default.remote_version_difference_probe ORDER BY a - b;
          SELECT '__difference_cast__', CAST(a - b AS VARCHAR) FROM remote_memory.default.remote_version_difference_probe;
        ")"
        assert_marked_output_equals $'__timestamp_difference__\t1 00:00:00.123456788' "__timestamp_difference__" "timestamp(9) difference, ${handling}, delegation=${delegation}" "${difference_output}"
        assert_marked_output_equals $'__time_difference__\t0 11:00:00.123456788' "__time_difference__" "time(9) difference" "${difference_output}"
        assert_marked_output_equals $'__difference_max__\t1 00:00:00.123456788' "__difference_max__" "high-precision interval aggregation" "${difference_output}"
        assert_marked_output_equals $'__difference_order__\t1 00:00:00.123456788' "__difference_order__" "high-precision interval ordering" "${difference_output}"
        assert_marked_output_equals $'__difference_cast__\t1 00:00:00.123456788' "__difference_cast__" "high-precision interval cast" "${difference_output}"
      done
    done

    echo "Running remote semantics delegation assertions"
    # Each query depends on behavior that changed in a Trino release up to 484.
    # Results must follow local 484 semantics whether or not the remote evaluates them.
    # These are the differences that keep functions, casts, and arithmetic out of the
    # basic list.
    trino_exec trino-remote-version "
      CREATE TABLE memory.default.remote_version_semantics_probe AS
      SELECT '{\"b\":1,\"a\":2}' AS js,
             CAST(TIMESTAMP '2024-03-10 00:00:00.000000 America/New_York' AS TIMESTAMP(6) WITH TIME ZONE) AS dst_start,
             CAST(TIMESTAMP '2024-03-11 00:00:00.000000 America/New_York' AS TIMESTAMP(6) WITH TIME ZONE) AS dst_end,
             CAST(TIMESTAMP '1975-05-10 12:34:56.001002 UTC' AS TIMESTAMP(6) WITH TIME ZONE) AS unix_timestamp,
             DOUBLE '0.5005' AS unix_seconds,
             DOUBLE '123.456' AS round_value,
             DECIMAL '912057769880088.000' AS double_decimal,
             DECIMAL '0.00391462049447' AS real_decimal,
             CAST('abc def' AS CHAR(7)) AS char_value,
             TIMESTAMP '1969-12-31 00:00:00.5' AS pre_epoch,
             TIMESTAMP '1970-01-01 00:00:00 UTC' AS zoned_value,
             REAL 'NaN' AS real_nan,
             DECIMAL '1.00' AS one,
             DECIMAL '3.00' AS three,
             CAST(DECIMAL '1.23456789' AS DECIMAL(38,10)) AS long_factor;
      CREATE TABLE memory.default.remote_version_average_probe AS
      SELECT x FROM UNNEST(concat(
        ARRAY[DECIMAL '9000000000000000000000000000000000000.0', DECIMAL '9000000000000000000000000000000000000.0'],
        repeat(CAST(0 AS DECIMAL(38,1)), 98))) t(x)
    " >/dev/null

    local semantics_output semantics_plan
    for delegation in true false; do
      semantics_output="$(trino_exec trino-local-version "
        SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
        SELECT '__json_order__', json_format(json_parse(js)) FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__date_diff_dst__', date_diff('day', dst_start, dst_end) FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__to_unixtime__', to_unixtime(unix_timestamp) = 168957296.001002e0 FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__from_unixtime__', from_unixtime(unix_seconds, 'UTC') = TIMESTAMP '1970-01-01 00:00:00.501 UTC' FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__round_underflow__', round(round_value, -324) = 0.0e0 FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__decimal_double__', CAST(double_decimal AS DOUBLE) = 912057769880088.0e0 FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__decimal_real__', CAST(real_decimal AS REAL) = REAL '0.0039146203' FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__char_trim__', trim(char_value, 'def') = 'abc ' FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__timestamp_time__', CAST(pre_epoch AS TIME(1)) = TIME '00:00:00.5' FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__in_time_zones__', at_timezone(zoned_value, 'Asia/Seoul') IN (TIMESTAMP '1970-01-01 01:00:00 Europe/Warsaw', TIMESTAMP '1970-01-01 00:00:02 UTC') FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__in_real_nan__', NOT (real_nan IN (REAL '12.34', REAL 'NaN', REAL '23.45')) FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__decimal_divide__', one / three FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__decimal_multiply__', long_factor * CAST(one AS DECIMAL(38,10)) FROM remote_memory.default.remote_version_semantics_probe;
        SELECT '__decimal_average__', avg(x) FROM remote_memory.default.remote_version_average_probe;
      ")"
      assert_marked_output_equals $'__json_order__\t{"b":1,"a":2}' "__json_order__" "JSON member order, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__date_diff_dst__\t1' "__date_diff_dst__" "timestamp(6) with time zone date_diff, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__to_unixtime__\ttrue' "__to_unixtime__" "to_unixtime rounding, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__from_unixtime__\ttrue' "__from_unixtime__" "from_unixtime rounding, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__round_underflow__\ttrue' "__round_underflow__" "round underflow, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__decimal_double__\ttrue' "__decimal_double__" "short decimal to double cast, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__decimal_real__\ttrue' "__decimal_real__" "short decimal to real cast, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__char_trim__\ttrue' "__char_trim__" "char trim with characters, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__timestamp_time__\ttrue' "__timestamp_time__" "pre-1970 timestamp to time cast, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__in_time_zones__\ttrue' "__in_time_zones__" "IN with time zones, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__in_real_nan__\ttrue' "__in_real_nan__" "IN with real NaN, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__decimal_divide__\t0.333333' "__decimal_divide__" "decimal division scale, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__decimal_multiply__\t1.234568' "__decimal_multiply__" "decimal multiplication scale, delegation=${delegation}" "${semantics_output}"
      assert_marked_output_equals $'__decimal_average__\t180000000000000000000000000000000000.0' "__decimal_average__" "long decimal average, delegation=${delegation}" "${semantics_output}"
    done

    # Only a remote server of the local version evaluates functions and arithmetic
    semantics_plan="$(trino_exec trino-local-version "EXPLAIN SELECT json_format(json_parse(js)) FROM remote_memory.default.remote_version_semantics_probe")"
    if [[ "${same_version}" == "true" ]]; then
      assert_output_contains "remote_memory:Query[" "JSON delegation to a remote of the local version" "${semantics_plan}"
    else
      assert_output_not_contains "remote_memory:Query[" "JSON stays local for another remote version" "${semantics_plan}"
    fi
    semantics_plan="$(trino_exec trino-local-version "EXPLAIN SELECT one / three FROM remote_memory.default.remote_version_semantics_probe")"
    if [[ "${same_version}" == "true" ]]; then
      assert_output_contains "remote_memory:Query[" "decimal division delegation to a remote of the local version" "${semantics_plan}"
    else
      assert_output_not_contains "remote_memory:Query[" "decimal division stays local for another remote version" "${semantics_plan}"
    fi

    echo "Running cast, aggregation, and join assertions"
    # Each case depends on a fix up to Trino 484 that older remote servers lack: casts to a
    # shorter char, casts from timestamp to a lower precision, casts to varchar(n) that do
    # not fit, date_add overflow, multiple DISTINCT aggregations with a NULL group, and
    # dynamic filters on a char to varchar cast join key. The catalog uses EAGER join pushdown.
    trino_exec trino-remote-version "
      CREATE TABLE memory.default.remote_version_cast_probe AS
      SELECT CAST('a b' AS CHAR(3)) AS char_value,
             CAST(TIMESTAMP '2026-07-26 03:00:00' AS TIMESTAMP(9)) AS ts9,
             CAST(TIMESTAMP '294247-01-10 04:00:54.775807' AS TIMESTAMP(6)) AS ts_max,
             CAST(TIMESTAMP '294246-12-10 04:00:54.775999' AS TIMESTAMP(6)) AS ts_near_max,
             CAST(TIME '12:34:56' AS TIME(0)) AS time_value,
             true AS boolean_value;
      CREATE TABLE memory.default.remote_version_distinct_probe AS
      SELECT * FROM (VALUES ('US', 1, 100), ('EU', 2, 200), (NULL, 3, 300), (NULL, 4, 400), (NULL, 3, 500)) AS t(region, customer_id, product_id);
      CREATE TABLE memory.default.remote_version_char_join_left AS
      SELECT * FROM (VALUES (1, CAST('a' AS CHAR(10))), (2, CAST('b' AS CHAR(10))), (3, CAST('abc' AS CHAR(10)))) AS t(id, c);
      CREATE TABLE memory.default.remote_version_char_join_right AS
      SELECT * FROM (VALUES CAST('a' AS VARCHAR(10)), CAST('abc' AS VARCHAR(10))) AS t(v);
      CREATE TABLE memory.default.remote_version_array_probe AS
      SELECT * FROM (VALUES (1, ARRAY[1, NULL]), (2, ARRAY[1, 2]), (3, ARRAY[0, 5])) AS t(id, a)
    " >/dev/null

    # Before 482, ordering arrays failed when a null element decided the order. TopN,
    # max, and ordering comparisons must follow local evaluation.
    local array_expected array_actual
    array_expected="$(trino_exec trino-local-version "
      SELECT '__array_topn__', id FROM (VALUES (1, ARRAY[1, NULL]), (2, ARRAY[1, 2]), (3, ARRAY[0, 5])) AS t(id, a) ORDER BY a LIMIT 2;
      SELECT '__array_max__', json_format(CAST(max(a) AS JSON)) FROM (VALUES (1, ARRAY[1, NULL]), (2, ARRAY[1, 2]), (3, ARRAY[0, 5])) AS t(id, a);
      SELECT '__array_less__', id FROM (VALUES (1, ARRAY[1, NULL]), (2, ARRAY[1, 2]), (3, ARRAY[0, 5])) AS t(id, a) WHERE a < ARRAY[1, 3] ORDER BY id;
    ")"
    for delegation in true false; do
      array_actual="$(trino_exec trino-local-version "
        SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
        SELECT '__array_topn__', id FROM remote_memory.default.remote_version_array_probe ORDER BY a LIMIT 2;
        SELECT '__array_max__', json_format(CAST(max(a) AS JSON)) FROM remote_memory.default.remote_version_array_probe;
        SELECT '__array_less__', id FROM remote_memory.default.remote_version_array_probe WHERE a < ARRAY[1, 3] ORDER BY id;
      ")"
      assert_output_equals "${array_expected}" "array ordering with null elements, delegation=${delegation}" "$(printf '%s\n' "${array_actual}" | grep -v '^SET SESSION$')"
    done

    local cast_output
    for delegation in true false; do
      cast_output="$(trino_exec trino-local-version "
        SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
        SELECT '__char_narrowing__', CAST(char_value AS CHAR(2)) = CAST('a' AS CHAR(2)) FROM remote_memory.default.remote_version_cast_probe;
        SELECT '__timestamp_tz_lower_precision__', count(*) FROM remote_memory.default.remote_version_cast_probe WHERE CAST(ts9 AS TIMESTAMP(6) WITH TIME ZONE) > TIMESTAMP '2026-07-25 00:00:00 UTC';
        SELECT '__timestamp_lower_precision__', year(CAST(ts_max AS TIMESTAMP(4))) FROM remote_memory.default.remote_version_cast_probe;
        SELECT '__distinct_null_group__', coalesce(region, 'NULL'), count(DISTINCT customer_id), count(DISTINCT product_id) FROM remote_memory.default.remote_version_distinct_probe GROUP BY region ORDER BY 2;
        SELECT '__char_join__', l.id FROM remote_memory.default.remote_version_char_join_left l JOIN remote_memory.default.remote_version_char_join_right r ON CAST(l.c AS VARCHAR(10)) = r.v ORDER BY l.id;
      ")"
      assert_marked_output_equals $'__char_narrowing__\ttrue' "__char_narrowing__" "cast to a shorter char, delegation=${delegation}" "${cast_output}"
      assert_marked_output_equals $'__timestamp_tz_lower_precision__\t1' "__timestamp_tz_lower_precision__" "timestamp to lower precision timestamp with time zone, delegation=${delegation}" "${cast_output}"
      assert_marked_output_equals $'__timestamp_lower_precision__\t294247' "__timestamp_lower_precision__" "lower precision cast near the maximum timestamp, delegation=${delegation}" "${cast_output}"
      assert_marked_output_equals $'__distinct_null_group__\tEU\t1\t1\n__distinct_null_group__\tNULL\t2\t3\n__distinct_null_group__\tUS\t1\t1' "__distinct_null_group__" "multiple DISTINCT aggregations with a NULL group, delegation=${delegation}" "${cast_output}"
      assert_marked_output_equals $'__char_join__\t1\n__char_join__\t3' "__char_join__" "char to varchar cast join key, delegation=${delegation}" "${cast_output}"
      assert_query_fails trino-local-version "
        SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
        SELECT time_value FROM remote_memory.default.remote_version_cast_probe WHERE CAST(time_value AS VARCHAR(5)) = '12:34'
      " "Cannot cast" "time to varchar(5) cast that does not fit, delegation=${delegation}"
      assert_query_fails trino-local-version "
        SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
        SELECT boolean_value FROM remote_memory.default.remote_version_cast_probe WHERE CAST(boolean_value AS VARCHAR(1)) = 't'
      " "Cannot cast" "boolean to varchar(1) cast that does not fit, delegation=${delegation}"
      assert_query_fails trino-local-version "
        SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
        SELECT ts_near_max FROM remote_memory.default.remote_version_cast_probe WHERE date_add('month', 1, ts_near_max) > TIMESTAMP '2000-01-01 00:00:00'
      " "out of range" "date_add beyond the maximum timestamp, delegation=${delegation}"
    done

    # Servers before 484 can drop the NULL group of this aggregation, so it is not in the
    # basic list
    local aggregation_plan
    aggregation_plan="$(trino_exec trino-local-version "EXPLAIN SELECT region, count(DISTINCT customer_id), count(DISTINCT product_id) FROM remote_memory.default.remote_version_distinct_probe GROUP BY region")"
    if [[ "${same_version}" == "true" ]]; then
      assert_output_not_contains "Aggregate" "multiple DISTINCT aggregation pushdown to a remote of the local version" "${aggregation_plan}"
    else
      assert_output_contains "Aggregate" "multiple DISTINCT aggregation stays local for another remote version" "${aggregation_plan}"
    fi

    # Servers before 484 push an aggregation through an outer join without a join
    # condition, and then count no row for an outer row without a match. The connector
    # must not send that shape. The catalog uses EAGER join pushdown.
    trino_exec trino-remote-version "
      CREATE TABLE memory.default.remote_version_outer_keys AS SELECT * FROM (VALUES 1, 2) AS t(a);
      CREATE TABLE memory.default.remote_version_outer_none AS SELECT * FROM (VALUES 1) AS t(b) WHERE false
    " >/dev/null
    local outer_join_output
    outer_join_output="$(trino_exec trino-local-version "
      SELECT '__outer_join_count__', a, count(*) FROM (SELECT DISTINCT a FROM remote_memory.default.remote_version_outer_keys) l LEFT JOIN remote_memory.default.remote_version_outer_none r ON true GROUP BY a ORDER BY a
    ")"
    assert_marked_output_equals $'__outer_join_count__\t1\t1\n__outer_join_count__\t2\t1' "__outer_join_count__" "count over an outer join without a join condition" "${outer_join_output}"

    # Trino 482 fixed number comparisons, DISTINCT, and GROUP BY with NaN values and with
    # values beyond the supported precision. Servers before 480 have no number columns to test.
    if (( remote_version >= 480 )); then
      trino_exec trino-remote-version "
        CREATE TABLE memory.default.remote_version_number_probe AS
        SELECT * FROM (VALUES (1, NUMBER '5'), (2, CAST(nan() AS NUMBER)), (3, NUMBER '1e200' + NUMBER '3'), (4, NUMBER '1e200'), (5, CAST(nan() AS NUMBER))) AS t(id, n)
      " >/dev/null
      local number_output
      for delegation in true false; do
        number_output="$(trino_exec trino-local-version "
          SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
          SELECT '__number_distinct__', count(*) FROM (SELECT DISTINCT n FROM remote_memory.default.remote_version_number_probe);
          SELECT '__number_group__', count(*) FROM (SELECT n FROM remote_memory.default.remote_version_number_probe GROUP BY n);
          SELECT '__number_equal__', id FROM remote_memory.default.remote_version_number_probe WHERE n = NUMBER '1e200' ORDER BY id;
          SELECT '__number_not_equal__', id FROM remote_memory.default.remote_version_number_probe WHERE n <> NUMBER '5' ORDER BY id;
          SELECT '__number_other_than__', id FROM remote_memory.default.remote_version_number_probe WHERE n < NUMBER '1e200' OR n > NUMBER '1e200' ORDER BY id;
        ")"
        assert_marked_output_equals $'__number_distinct__\t3' "__number_distinct__" "number DISTINCT with NaN and rounded values, delegation=${delegation}" "${number_output}"
        assert_marked_output_equals $'__number_group__\t3' "__number_group__" "number GROUP BY with NaN and rounded values, delegation=${delegation}" "${number_output}"
        assert_marked_output_equals $'__number_equal__\t3\n__number_equal__\t4' "__number_equal__" "number equality with a rounded value, delegation=${delegation}" "${number_output}"
        assert_marked_output_equals $'__number_not_equal__\t2\n__number_not_equal__\t3\n__number_not_equal__\t4\n__number_not_equal__\t5' "__number_not_equal__" "number inequality with NaN, delegation=${delegation}" "${number_output}"
        # The query builder would send NOT (n = ?), which NaN passes and which servers
        # before 482 evaluate by the bytes of the rounded value
        assert_marked_output_equals $'__number_other_than__\t1' "__number_other_than__" "number values other than a rounded value, delegation=${delegation}" "${number_output}"
      done

      # Without complex join pushdown, base-jdbc checks join conditions only through
      # isSupportedJoinCondition. Equal number values 3 and 4 must still match.
      number_output="$(trino_exec trino-local-version "
        SET SESSION remote_memory.complex_join_pushdown_enabled = false;
        SELECT '__number_legacy_join__', count(*) FROM remote_memory.default.remote_version_number_probe a JOIN remote_memory.default.remote_version_number_probe b ON a.n = b.n;
      ")"
      assert_marked_output_equals $'__number_legacy_join__\t5' "__number_legacy_join__" "number join without complex join pushdown" "${number_output}"
    fi

    # Servers before 484 accepted JSON values that the 484 parser rejects. Top-level JSON
    # columns must read them as nested JSON values are read.
    if (( remote_version < 484 )); then
      trino_exec trino-remote-version "
        CREATE TABLE memory.default.remote_version_json_probe AS
        SELECT id, j, ARRAY[j] AS ja FROM (VALUES (1, json_parse('{\"x\": 1e400}')), (2, json_parse('[\"\\ud800\"]')), (3, json_parse('{\"ok\": 1}'))) AS t(id, j)
      " >/dev/null
      local json_output json_top_level json_nested
      json_output="$(trino_exec trino-local-version "
        SELECT '__json_top_level__', id, json_format(j) FROM remote_memory.default.remote_version_json_probe ORDER BY id;
        SELECT '__json_nested__', id, json_format(ja[1]) FROM remote_memory.default.remote_version_json_probe ORDER BY id;
      ")"
      json_top_level="$(printf '%s\n' "${json_output}" | awk -F '\t' '$1 == "__json_top_level__" {print $2 "\t" $3}')"
      json_nested="$(printf '%s\n' "${json_output}" | awk -F '\t' '$1 == "__json_nested__" {print $2 "\t" $3}')"
      [[ "$(printf '%s\n' "${json_top_level}" | grep -c .)" == "3" ]] || fail "expected three top-level JSON rows: ${json_top_level}"
      assert_output_equals "${json_nested}" "top-level JSON values read like nested JSON values" "${json_top_level}"
    fi

    # A JSON value of Trino 484 can hold a date or time, which the remote server sends as a
    # JSON string. The remote server tells such a value from the string, and the local
    # server cannot. Operations that compare or hash JSON values must therefore run
    # locally. Operations that read a JSON value as text stay on the remote server.
    if [[ "${same_version}" == "true" ]]; then
      trino_exec trino-remote-version "
        CREATE TABLE memory.default.remote_version_json_datetime_probe AS
        SELECT * FROM (VALUES
          (1, json_scalar(DATE '1970-01-01'), json_scalar(VARCHAR '1970-01-01')),
          (2, json_scalar(VARCHAR '1970-01-01'), json_scalar(VARCHAR '1970-01-01')),
          (3, JSON '\"x\"', JSON '\"x\"')) AS t(id, j, k)
      " >/dev/null
      local json_datetime_output json_datetime_plan
      json_datetime_output="$(trino_exec trino-remote-version "SELECT '__json_remote_distinct__', count(DISTINCT j) FROM memory.default.remote_version_json_datetime_probe")"
      assert_marked_output_equals $'__json_remote_distinct__\t3' "__json_remote_distinct__" "the remote server tells a date in a JSON value from a string" "${json_datetime_output}"
      for delegation in true false; do
        json_datetime_output="$(trino_exec trino-local-version "
          SET SESSION remote_memory.remote_delegation_enabled = ${delegation};
          SELECT '__json_distinct__', count(DISTINCT j) FROM remote_memory.default.remote_version_json_datetime_probe;
          SELECT '__json_group__', count(*) FROM (SELECT j FROM remote_memory.default.remote_version_json_datetime_probe GROUP BY j);
          SELECT '__json_equal__', id FROM remote_memory.default.remote_version_json_datetime_probe WHERE j = k ORDER BY id;
          SELECT '__json_checksum__', checksum(j) = checksum(k) FROM remote_memory.default.remote_version_json_datetime_probe;
          SELECT '__json_read__', id FROM remote_memory.default.remote_version_json_datetime_probe WHERE json_extract_scalar(j, '\$') = '1970-01-01' ORDER BY id;
        ")"
        assert_marked_output_equals $'__json_distinct__\t2' "__json_distinct__" "DISTINCT on JSON values with a date, delegation=${delegation}" "${json_datetime_output}"
        assert_marked_output_equals $'__json_group__\t2' "__json_group__" "GROUP BY on JSON values with a date, delegation=${delegation}" "${json_datetime_output}"
        assert_marked_output_equals $'__json_equal__\t1\n__json_equal__\t2\n__json_equal__\t3' "__json_equal__" "equality of JSON values with a date, delegation=${delegation}" "${json_datetime_output}"
        assert_marked_output_equals $'__json_checksum__\ttrue' "__json_checksum__" "checksum of JSON values with a date, delegation=${delegation}" "${json_datetime_output}"
        assert_marked_output_equals $'__json_read__\t1\n__json_read__\t2' "__json_read__" "text of JSON values with a date, delegation=${delegation}" "${json_datetime_output}"
      done
      json_datetime_plan="$(trino_exec trino-local-version "EXPLAIN SELECT count(DISTINCT j) FROM remote_memory.default.remote_version_json_datetime_probe")"
      assert_output_contains "Aggregate" "DISTINCT on JSON values stays local" "${json_datetime_plan}"
      json_datetime_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_json_datetime_probe WHERE j = k")"
      assert_output_contains "filterPredicate" "equality of JSON values stays local" "${json_datetime_plan}"
      json_datetime_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_json_datetime_probe WHERE json_extract_scalar(j, '\$') = '1970-01-01'")"
      assert_output_not_contains "filterPredicate" "reading a JSON value as text stays on the remote server" "${json_datetime_plan}"
    fi

    echo "Running delegation differential assertions"
    # Every expression must return the rows that the local server returns for a copy of
    # the table in its memory catalog, with delegation enabled and disabled. An
    # expression that fails locally must fail through the connector too.
    local differential_table_sql
    differential_table_sql="
      CREATE TABLE memory.default.remote_version_differential_probe AS SELECT * FROM (VALUES
        (1, CAST('a b' AS CHAR(3)), CAST('a ' AS VARCHAR(4)), CAST(TIMESTAMP '1969-12-31 23:59:59.999999999' AS TIMESTAMP(9)), CAST(TIMESTAMP '1969-12-31 00:00:00.500' AS TIMESTAMP(3)),
         CAST(TIMESTAMP '2024-03-10 01:30:00.123456 America/New_York' AS TIMESTAMP(6) WITH TIME ZONE), CAST(TIMESTAMP '1969-12-31 23:59:59.500 UTC' AS TIMESTAMP(3) WITH TIME ZONE),
         DOUBLE '-0.0', REAL '-0.0', CAST(12345678.95 AS DECIMAL(10,2)), CAST(1.2345678901 AS DECIMAL(38,10)), BIGINT '12345678', CAST(TIME '12:34:56.789' AS TIME(3)), true,
         CAST(TIME '00:00:00.000 +05:00' AS TIME(3) WITH TIME ZONE), CAST(TIMESTAMP '294247-01-10 04:00:54.775807' AS TIMESTAMP(6)), CAST(TIMESTAMP '294246-12-10 04:00:54.775999' AS TIMESTAMP(6)),
         CAST(' 1337 ' AS VARCHAR), CAST(1.0000000003 AS DECIMAL(19,10)), DATE '300000-01-01', INTEGER '-2147483648',
         CAST(TIMESTAMP '73326-08-22 20:14:45.000000 UTC' AS TIMESTAMP(6) WITH TIME ZONE), CAST(TIMESTAMP '73326-09-11 20:14:45.247999 UTC' AS TIMESTAMP(6) WITH TIME ZONE),
         CAST(TIMESTAMP '-69387-04-22 12:00:00.000000 UTC' AS TIMESTAMP(6) WITH TIME ZONE), CAST('100000-01-01 00:00:00.000000 UTC' AS VARCHAR), CAST(TIMESTAMP '100000-01-01 00:00:00.000000' AS TIMESTAMP(6))),
        (2, CAST('ab' AS CHAR(3)), CAST(' a' AS VARCHAR(4)), CAST(TIMESTAMP '2026-07-26 03:00:00.123456789' AS TIMESTAMP(9)), CAST(TIMESTAMP '2026-01-01 12:00:00.123' AS TIMESTAMP(3)),
         CAST(TIMESTAMP '2024-11-03 01:30:00.999999 America/New_York' AS TIMESTAMP(6) WITH TIME ZONE), CAST(TIMESTAMP '2026-01-31 08:00:00.000 Asia/Tokyo' AS TIMESTAMP(3) WITH TIME ZONE),
         nan(), nan(), CAST(-0.05 AS DECIMAL(10,2)), CAST(-99999999.9999999999 AS DECIMAL(38,10)), BIGINT '-1', CAST(TIME '23:59:59.999' AS TIME(3)), false,
         CAST(TIME '23:59:59.999 -08:00' AS TIME(3) WITH TIME ZONE), CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP(6)), CAST(TIMESTAMP '2026-01-01 00:00:00' AS TIMESTAMP(6)),
         CAST('42' AS VARCHAR), CAST(-2.5 AS DECIMAL(19,10)), DATE '2026-01-01', INTEGER '7',
         CAST(TIMESTAMP '2026-01-01 00:00:00.000000 UTC' AS TIMESTAMP(6) WITH TIME ZONE), CAST(TIMESTAMP '2026-01-01 00:00:00.000000 UTC' AS TIMESTAMP(6) WITH TIME ZONE),
         CAST(TIMESTAMP '2026-01-01 00:00:00.000000 UTC' AS TIMESTAMP(6) WITH TIME ZONE), CAST('2026-01-01 00:00:00.000000 UTC' AS VARCHAR), CAST(TIMESTAMP '2026-01-02 00:00:00' AS TIMESTAMP(6))),
        (3, CAST(' a' AS CHAR(3)), CAST('abc' AS VARCHAR(4)), CAST(TIMESTAMP '1900-01-01 00:00:00.000000001' AS TIMESTAMP(9)), CAST(TIMESTAMP '1969-12-31 23:59:59.999' AS TIMESTAMP(3)),
         CAST(TIMESTAMP '1970-01-01 00:00:00.000001 UTC' AS TIMESTAMP(6) WITH TIME ZONE), CAST(TIMESTAMP '2024-02-29 12:00:00.000 Europe/Warsaw' AS TIMESTAMP(3) WITH TIME ZONE),
         DOUBLE '2.5', REAL '1.5', CAST(0.00 AS DECIMAL(10,2)), CAST(0 AS DECIMAL(38,10)), BIGINT '0', CAST(TIME '00:00:00.001' AS TIME(3)), CAST(NULL AS BOOLEAN),
         CAST(TIME '12:00:00.000 +14:00' AS TIME(3) WITH TIME ZONE), CAST(NULL AS TIMESTAMP(6)), CAST(NULL AS TIMESTAMP(6)),
         CAST('x' AS VARCHAR), CAST(NULL AS DECIMAL(19,10)), CAST(NULL AS DATE), INTEGER '0',
         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE),
         CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE), CAST(NULL AS VARCHAR), CAST(NULL AS TIMESTAMP(6)))
      ) AS t(id, c3, v, ts9, ts3, tstz6, tstz3, d, r, dec, dl, i, t, b, ttz, ts_max, ts_near_max, vi, dm, dt, i4, tz_max, tz_edge, tz_min, vt, ts_far)
    "
    trino_exec trino-remote-version "${differential_table_sql}" >/dev/null
    trino_exec trino-local-version "${differential_table_sql}" >/dev/null

    # Functions, casts, and arithmetic are delegated only to a server of the local version.
    # Comparisons between columns are delegated to every server, as column filters and
    # joins are.
    local delegation_plan
    delegation_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_differential_probe WHERE length(v) = 2")"
    if [[ "${same_version}" == "true" ]]; then
      assert_output_not_contains "ScanFilter" "function filter delegation to a remote of the local version" "${delegation_plan}"
    else
      assert_output_contains "ScanFilter" "function filter stays local for another remote version" "${delegation_plan}"
    fi
    delegation_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_differential_probe WHERE ts_max > ts_near_max OR b IS NULL")"
    assert_output_not_contains "ScanFilter" "column comparison delegation to every remote version" "${delegation_plan}"

    local differential_sql="" expression expression_index=0
    while IFS= read -r expression; do
      [[ -n "${expression}" ]] || continue
      expression_index=$((expression_index + 1))
      differential_sql+="SELECT '__expression_${expression_index}__', id, CAST((${expression}) AS VARCHAR) FROM remote_memory.default.remote_version_differential_probe ORDER BY id;"$'\n'
    done < "${repo_root}/testing/remote-version-smoke/delegation-expressions.txt"

    assert_local_evaluation "expressions" "${differential_sql}"

    # Filters reach the remote optimizer, which the projections above do not. Trino 482
    # drops the negation of a negated NULLIF and times out on a cast compared with
    # date_trunc, so it receives no functions or casts. Trino 482 and 483 fail on a cast
    # from varchar to char compared with a constant.
    local filter filter_sql="" filter_index=0
    while IFS= read -r filter; do
      [[ -n "${filter}" ]] || continue
      filter_index=$((filter_index + 1))
      filter_sql+="SELECT '__filter_${filter_index}__', id FROM remote_memory.default.remote_version_differential_probe WHERE ${filter} ORDER BY id;"$'\n'
    done <<'FILTERS'
NOT NULLIF(CAST(i AS integer) = 12345678, true)
NOT (CAST(i AS integer) BETWEEN 0 AND 100)
CAST(CAST(ts9 AS date) AS timestamp(3)) = date_trunc('day', ts3)
year(ts3) = i4 + 2019
TRY_CAST(vi AS bigint) > 0
dm * dm = CAST('1.00000000060000000009' AS decimal(38,20))
TRY_CAST(dt AS timestamp(6)) IS NULL
TRY_CAST(tz_edge AS timestamp(4) with time zone) IS NULL
CAST(v AS char(4)) = CAST('abc' AS char(4))
ts_max > ts_near_max OR b IS NULL
FILTERS

    assert_local_evaluation "filters" "${filter_sql}"

    # A passthrough query runs on the remote server as written. A filter or a join on
    # its result follows the rules of a table: only a remote server of the local
    # version evaluates the function.
    local passthrough_query="SELECT id, v, i FROM memory.default.remote_version_differential_probe"
    local passthrough_sql="
      SELECT '__passthrough_filter__', id FROM @@ WHERE length(v) = 2 OR i < 0 ORDER BY id;
      SELECT '__passthrough_join__', p.id, t.i4 FROM @@ p JOIN remote_memory.default.remote_version_differential_probe t ON p.id = t.id WHERE length(p.v) = 2 ORDER BY p.id;
    "
    # The replacement is a variable: bash 4.3 and later remove single quotes that are
    # written in the replacement of a substitution and do not expand what they enclose
    local passthrough_table="TABLE(remote_memory.system.query(query => '${passthrough_query}'))"
    assert_local_evaluation "filters and joins on a passthrough query" \
      "${passthrough_sql//@@/${passthrough_table}}" \
      "${passthrough_sql//@@/(${passthrough_query})}"
    delegation_plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM TABLE(remote_memory.system.query(query => '${passthrough_query}')) WHERE length(v) = 2 OR i < 0")"
    if [[ "${same_version}" == "true" ]]; then
      assert_output_not_contains "filterPredicate" "function filter on a passthrough query, delegated to a remote of the local version" "${delegation_plan}"
    else
      assert_output_contains "filterPredicate" "function filter on a passthrough query stays local for another remote version" "${delegation_plan}"
    fi
  fi

  echo "Running CHAR/VARCHAR remote delegation assertions"
  trino_exec trino-remote-version "DROP TABLE IF EXISTS memory.default.remote_version_char_probe; CREATE TABLE memory.default.remote_version_char_probe AS SELECT * FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  '), (4, CAST('a' || chr(9) AS CHAR(3)), VARCHAR 'c')) AS t(id, c, v)" >/dev/null
  local char_expected
  local char_actual
  char_expected="$(trino_exec trino-local-version "
    SELECT '__char_cast__';
    SELECT '[' || CAST(CAST('a' AS CHAR(3)) AS VARCHAR) || ']', length(CAST(CAST('a' AS CHAR(3)) AS VARCHAR));
    SELECT '__char_varchar_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  '), (4, CAST('a' || chr(9) AS CHAR(3)), VARCHAR 'c')) AS t(id, c, v) WHERE v = c ORDER BY id;
    SELECT '__char_constant_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  '), (4, CAST('a' || chr(9) AS CHAR(3)), VARCHAR 'c')) AS t(id, c, v) WHERE c = CAST('a' AS CHAR(3)) ORDER BY id;
    SELECT '__varchar_char_constant_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  '), (4, CAST('a' || chr(9) AS CHAR(3)), VARCHAR 'c')) AS t(id, c, v) WHERE v = CAST('a' AS CHAR(3)) ORDER BY id;
    SELECT '__char_range_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  '), (4, CAST('a' || chr(9) AS CHAR(3)), VARCHAR 'c')) AS t(id, c, v) WHERE c < CAST('a' AS CHAR(3)) ORDER BY id;
    SELECT '__char_range_or_equal_predicate__';
    SELECT id FROM (VALUES (1, CAST('a' AS CHAR(3)), VARCHAR 'a'), (2, CAST('a' AS CHAR(3)), VARCHAR 'a  '), (3, CAST('b' AS CHAR(3)), VARCHAR 'b  '), (4, CAST('a' || chr(9) AS CHAR(3)), VARCHAR 'c')) AS t(id, c, v) WHERE c >= CAST('a' AS CHAR(3)) ORDER BY id;
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
    SELECT '__char_range_predicate__';
    SELECT id FROM remote_memory.default.remote_version_char_probe WHERE c < CAST('a' AS CHAR(3)) ORDER BY id;
    SELECT '__char_range_or_equal_predicate__';
    SELECT id FROM remote_memory.default.remote_version_char_probe WHERE c >= CAST('a' AS CHAR(3)) ORDER BY id;
  ")"
  # The range filters include a value with a character below a space. Servers from 482
  # compare a char column with a varchar value without padding, so the connector binds
  # char filter values as char.
  assert_output_equals "${char_expected}" "CHAR/VARCHAR local-versus-remote semantics" "${char_actual}"
  # Servers before 482 keep the trailing spaces of a char value in a cast to varchar, so
  # only a remote server of the local version receives casts
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
  if [[ "${same_version}" == "true" ]]; then
    assert_output_contains "remote_memory:Query[" "CHAR cast delegation to a remote of the local version" "${char_cast_plan}"
    assert_output_not_contains "ScanFilter" "CHAR/VARCHAR predicate delegation to a remote of the local version" "${char_predicate_plan}"
  else
    assert_output_not_contains "remote_memory:Query[" "CHAR cast stays local for another remote version" "${char_cast_plan}"
    assert_output_contains "ScanFilter" "CHAR/VARCHAR predicate stays local for another remote version" "${char_predicate_plan}"
  fi

  # The connector checks at an interval, 10 seconds by default, that the remote server is
  # still the server process whose version and settings it read. Another remote server
  # takes the host name here while the local server keeps running. The local server must
  # then send the full list only to a server of its version with default settings: a
  # Trino 483 that receives the json_parse filter returns no row, and a server with the
  # legacy settings returns other rows for the regular expression and the division.
  echo "Running remote server change assertions"
  local check_interval_seconds=10
  local change_table_sql="CREATE TABLE memory.default.remote_version_change_probe AS SELECT * FROM (VALUES (1, 'a' || chr(10), '{\"b\":1,\"a\":2}', DECIMAL '1.00', DECIMAL '3.00'), (2, 'ab', '{\"a\":2,\"b\":1}', DECIMAL '2.00', DECIMAL '3.00')) AS t(id, v, js, one, three)"
  local change_sql="
    SELECT '__change_regex__', id FROM remote_memory.default.remote_version_change_probe WHERE regexp_like(v, 'a\$') ORDER BY id;
    SELECT '__change_json__', id FROM remote_memory.default.remote_version_change_probe WHERE json_format(json_parse(js)) = '{\"b\":1,\"a\":2}' ORDER BY id;
    SELECT '__change_decimal__', id, CAST(one / three AS varchar) FROM remote_memory.default.remote_version_change_probe ORDER BY id;
  "

  # Tables of the memory catalog go with the server, so the new server gets the table
  # again. The wait for the interval makes the next query plan with a new check also when
  # the new server started faster than the interval.
  replace_remote_server() {
    local version="$1"
    local config="$2"

    echo "Replacing the remote server with Trino ${version}, ${config} settings"
    LOCAL_TRINO_VERSION="${local_version}" REMOTE_TRINO_VERSION="${version}" REMOTE_TRINO_CONFIG="${config}" \
      docker compose -p "${project_name}" -f "${compose_file}" up -d --force-recreate --no-deps trino-remote-version
    wait_for_trino trino-remote-version "remote Trino ${version}, ${config} settings"
    trino_exec trino-remote-version "${change_table_sql}" >/dev/null
    # The local server can keep the address of the server before it for 30 seconds
    for attempt in $(seq 1 30); do
      if trino_exec trino-local-version "SELECT count(*) FROM remote_memory.default.remote_version_change_probe" >/dev/null 2>&1; then
        break
      fi
      sleep 2
    done
    sleep "${check_interval_seconds}"
  }

  assert_remote_evaluation_of_functions() {
    local expected="$1"
    local context="$2"
    local plan

    assert_local_evaluation "${context}" "${change_sql}"
    plan="$(trino_exec trino-local-version "EXPLAIN SELECT id FROM remote_memory.default.remote_version_change_probe WHERE regexp_like(v, 'a\$')")"
    if [[ "${expected}" == "true" ]]; then
      assert_output_not_contains "filterPredicate" "${context}: the function filter runs on the remote server" "${plan}"
    else
      assert_output_contains "filterPredicate" "${context}: the function filter runs locally" "${plan}"
    fi
  }

  trino_exec trino-remote-version "${change_table_sql}" >/dev/null
  trino_exec trino-local-version "${change_table_sql}" >/dev/null
  if [[ "${same_version}" == "true" ]]; then
    assert_remote_evaluation_of_functions true "remote server of the local version"
    replace_remote_server "${remote_version}" legacy-settings
    assert_remote_evaluation_of_functions false "remote server restarted with other settings"
  elif [[ "${remote_config}" == "legacy-settings" ]]; then
    assert_remote_evaluation_of_functions false "remote server with other settings"
    replace_remote_server "${remote_version}" common
    assert_remote_evaluation_of_functions true "remote server restarted with default settings"
  else
    assert_remote_evaluation_of_functions false "remote server of another version"
    replace_remote_server "${local_version}" common
    assert_remote_evaluation_of_functions true "remote server replaced by a server of the local version"
    replace_remote_server "${remote_version}" common
    assert_remote_evaluation_of_functions false "remote server replaced by a server of another version"
  fi

  echo "Remote version smoke test passed for local Trino ${local_version} -> remote Trino ${remote_target}"
)

failures=0
for remote_target in "${remote_versions[@]}"; do
  set +e
  run_version "${remote_target}"
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
