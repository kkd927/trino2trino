#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

target_version="${1:-}"
[[ "${target_version}" =~ ^[0-9]+$ ]] || fail "target version must be an integer"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || fail "not inside a git repository"
cd "${repo_root}"

list_supported_versions() {
  if [[ -f RELEASES.md ]]; then
    sed -n 's/^| \([0-9][0-9]*\) |.*/\1/p' RELEASES.md
    return
  fi

  git tag --list 'trino-*-r*' |
    sed -n 's/^trino-\([0-9][0-9]*\)-r[0-9][0-9]*$/\1/p'
}

versions=()
version_count=0
while IFS= read -r version; do
  versions[version_count]="${version}"
  version_count=$((version_count + 1))
done < <(
  list_supported_versions |
    sort -n -u
)

[[ "${version_count}" -gt 0 ]] || fail "no supported Trino versions found in RELEASES.md or local release tags"

selected=()
selected_count=0

add_version() {
  local version="$1"
  local index

  [[ -n "${version}" ]] || return 0
  [[ "${version}" != "${target_version}" ]] || return 0
  [[ "${selected_count}" -lt 2 ]] || return 0

  for ((index = 0; index < selected_count; index++)); do
    [[ "${selected[index]}" != "${version}" ]] || return 0
  done

  selected[selected_count]="${version}"
  selected_count=$((selected_count + 1))
}

highest_less_than() {
  local ceiling="$1"
  local index
  local version
  local match=""

  for ((index = 0; index < version_count; index++)); do
    version="${versions[index]}"
    if (( 10#${version} < ceiling )) && [[ "${version}" != "${target_version}" ]]; then
      match="${version}"
    fi
  done

  printf '%s' "${match}"
}

newest_not_target() {
  local index

  for ((index = version_count - 1; index >= 0; index--)); do
    if [[ "${versions[index]}" != "${target_version}" ]]; then
      printf '%s' "${versions[index]}"
      return 0
    fi
  done
}

oldest="${versions[0]}"
newest="${versions[$((version_count - 1))]}"
middle="${versions[$(((version_count - 1) / 2))]}"

if (( 10#${target_version} >= 10#${newest} )); then
  add_version "${oldest}"
  add_version "${middle}"
elif (( 10#${target_version} <= 10#${oldest} )); then
  add_version "${middle}"
  add_version "${newest}"
else
  add_version "$(highest_less_than "${target_version}")"
  add_version "${newest}"
fi

if [[ "${selected_count}" -lt 2 ]]; then
  add_version "${middle}"
fi

if [[ "${selected_count}" -lt 2 ]]; then
  add_version "${oldest}"
fi

if [[ "${selected_count}" -lt 2 ]]; then
  add_version "$(newest_not_target)"
fi

if [[ "${selected_count}" -eq 0 ]]; then
  printf '\n'
else
  output="${selected[0]}"
  for ((index = 1; index < selected_count; index++)); do
    output="${output} ${selected[index]}"
  done
  printf '%s\n' "${output}"
fi
