#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

bool() {
  if "$@"; then
    printf 'true'
  else
    printf 'false'
  fi
}

extract_parent_version() {
  grep -A 3 '<parent>' pom.xml | grep '<version>' | head -n 1 | sed -E 's/.*<version>([^<]+)<.*/\1/'
}

extract_target_jdk() {
  sed -n 's:.*<project.build.targetJdk>\([^<]*\)</project.build.targetJdk>.*:\1:p' | head -n 1
}

workflow_jdk() {
  local file="$1"
  sed -n 's/^[[:space:]]*java-version:[[:space:]]*["'\'']\{0,1\}\([0-9][0-9]*\)["'\'']\{0,1\}[[:space:]]*$/\1/p' "$file" | head -n 1
}

local_java_major() {
  if ! command -v java >/dev/null 2>&1; then
    printf 'missing'
    return
  fi

  local raw version major
  raw="$(java -version 2>&1 | head -n 1)"
  version="$(printf '%s\n' "$raw" | sed -n 's/.*version "\([^"]*\)".*/\1/p')"
  if [[ -z "$version" ]]; then
    printf 'unknown'
    return
  fi

  if [[ "$version" == 1.* ]]; then
    major="$(printf '%s\n' "$version" | cut -d. -f2)"
  else
    major="$(printf '%s\n' "$version" | cut -d. -f1)"
  fi

  if [[ "$major" =~ ^[0-9]+$ ]]; then
    printf '%s' "$major"
  else
    printf 'unknown'
  fi
}

join_by_comma() {
  local IFS=,
  printf '%s' "$*"
}

requested_version="${1:-}"
[[ "$requested_version" =~ ^[0-9]+$ ]] || fail "requested version must be an integer"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || fail "not inside a git repository"
cd "$repo_root"

[[ -f pom.xml ]] || fail "pom.xml not found at repository root"

current_version="$(extract_parent_version)"
[[ "$current_version" =~ ^[0-9]+$ ]] || fail "could not parse current Trino version from pom.xml"

if [[ -n "$(git status --short)" ]]; then
  fail "working tree is dirty; commit, stash, or discard local changes before running trino-release"
fi

git fetch origin --prune >/dev/null 2>&1 || fail "git fetch origin failed"

current_branch="$(git rev-parse --abbrev-ref HEAD)"
main_sha="$(git rev-parse main 2>/dev/null)" || fail "local main branch not found"
origin_main_sha="$(git rev-parse origin/main 2>/dev/null)" || fail "origin/main not found"
[[ "$main_sha" == "$origin_main_sha" ]] || fail "local main is not synchronized with origin/main"

if (( 10#$requested_version > 10#$current_version )); then
  release_case="upgrade"
elif (( 10#$requested_version < 10#$current_version )); then
  release_case="backport"
else
  release_case="same"
fi

root_pom_url="https://repo.maven.apache.org/maven2/io/trino/trino-root/${requested_version}/trino-root-${requested_version}.pom"
raw_pom_url="https://raw.githubusercontent.com/trinodb/trino/${requested_version}/pom.xml"

if curl -fsI "$root_pom_url" >/dev/null 2>&1; then
  on_central="true"
else
  on_central="false"
fi

if git ls-remote --exit-code --tags https://github.com/trinodb/trino.git "refs/tags/${requested_version}" >/dev/null 2>&1; then
  upstream_tag="true"
else
  upstream_tag="false"
fi

if [[ "$on_central" != "true" && "$upstream_tag" != "true" ]]; then
  fail "Trino ${requested_version} was not found in Maven Central or upstream tags"
fi

if [[ "$on_central" == "true" ]]; then
  target_jdk="$(curl -fsSL "$root_pom_url" | extract_target_jdk)"
else
  target_jdk="$(curl -fsSL "$raw_pom_url" | extract_target_jdk)"
fi
[[ "$target_jdk" =~ ^[0-9]+$ ]] || fail "could not parse project.build.targetJdk for Trino ${requested_version}"

workflow_files=(
  ".github/workflows/build.yml"
  ".github/workflows/release.yml"
  ".github/workflows/delta-smoke.yml"
)

current_jdks=()
for file in "${workflow_files[@]}"; do
  [[ -f "$file" ]] || fail "workflow file not found: $file"
  value="$(workflow_jdk "$file")"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "could not parse java-version from $file"
  current_jdks+=("$value")
done

if [[ "${current_jdks[0]}" != "${current_jdks[1]}" || "${current_jdks[0]}" != "${current_jdks[2]}" ]]; then
  fail "workflow java-version drift detected: ${workflow_files[0]}=${current_jdks[0]}, ${workflow_files[1]}=${current_jdks[1]}, ${workflow_files[2]}=${current_jdks[2]}"
fi
current_jdk="${current_jdks[0]}"

requested_branch="release/trino-${requested_version}"
current_branch_name="release/trino-${current_version}"

if git show-ref --verify --quiet "refs/heads/${requested_branch}"; then
  release_branch_exists_local="true"
else
  release_branch_exists_local="false"
fi

if git show-ref --verify --quiet "refs/remotes/origin/${requested_branch}"; then
  release_branch_exists_origin="true"
else
  release_branch_exists_origin="false"
fi

release_branch_for_current_local_sha=""
if git show-ref --verify --quiet "refs/heads/${current_branch_name}"; then
  release_branch_for_current_exists_local="true"
  release_branch_for_current_local_sha="$(git rev-parse "refs/heads/${current_branch_name}")"
else
  release_branch_for_current_exists_local="false"
fi

release_branch_for_current_origin_sha=""
if git show-ref --verify --quiet "refs/remotes/origin/${current_branch_name}"; then
  release_branch_for_current_exists_origin="true"
  release_branch_for_current_origin_sha="$(git rev-parse "refs/remotes/origin/${current_branch_name}")"
else
  release_branch_for_current_exists_origin="false"
fi

if [[ "$release_case" == "upgrade" ]]; then
  if [[ "$release_branch_for_current_exists_local" == "true" && "$release_branch_for_current_local_sha" != "$main_sha" ]]; then
    fail "local ${current_branch_name} points to ${release_branch_for_current_local_sha}, expected main ${main_sha}; update or delete the stale preserve branch before upgrading"
  fi
  if [[ "$release_branch_for_current_exists_origin" == "true" && "$release_branch_for_current_origin_sha" != "$main_sha" ]]; then
    fail "origin/${current_branch_name} points to ${release_branch_for_current_origin_sha}, expected main ${main_sha}; reconcile the stale preserve branch before upgrading"
  fi
fi

if [[ "$release_case" == "upgrade" && "$release_branch_for_current_exists_origin" != "true" ]]; then
  preserve_branch_push_needed="true"
else
  preserve_branch_push_needed="false"
fi

remote_tags=()
while IFS= read -r tag; do
  remote_tags+=("$tag")
done < <(
  git ls-remote --tags --refs origin "trino-${requested_version}-r*" |
    awk '{print $2}' |
    sed 's#refs/tags/##' |
    sort -u
)

existing_tags=""
if (( ${#remote_tags[@]} > 0 )); then
  existing_tags="$(join_by_comma "${remote_tags[@]}")"
fi

max_release=0
for tag in ${remote_tags[@]+"${remote_tags[@]}"}; do
  suffix="${tag#trino-${requested_version}-r}"
  if [[ "$suffix" =~ ^[0-9]+$ ]] && (( suffix > max_release )); then
    max_release="$suffix"
  fi
done
next_tag="trino-${requested_version}-r$((max_release + 1))"

cat <<EOF
requested_version=${requested_version}
current_version=${current_version}
case=${release_case}
current_branch=${current_branch}
main_sha=${main_sha}
origin_main_sha=${origin_main_sha}
on_central=${on_central}
upstream_tag=${upstream_tag}
target_jdk=${target_jdk}
current_jdk=${current_jdk}
local_java_major=$(local_java_major)
release_branch_exists_local=${release_branch_exists_local}
release_branch_exists_origin=${release_branch_exists_origin}
release_branch_for_current_exists_local=${release_branch_for_current_exists_local}
release_branch_for_current_exists_origin=${release_branch_for_current_exists_origin}
release_branch_for_current_local_sha=${release_branch_for_current_local_sha}
release_branch_for_current_origin_sha=${release_branch_for_current_origin_sha}
preserve_branch_push_needed=${preserve_branch_push_needed}
existing_tags=${existing_tags}
next_tag=${next_tag}
EOF
