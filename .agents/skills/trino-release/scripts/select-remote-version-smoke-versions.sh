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

versions=()
version_count=0
while IFS= read -r version; do
  versions[version_count]="${version}"
  version_count=$((version_count + 1))
done < <(
  sed -n 's/^| \([0-9][0-9]*\) |.*/\1/p' RELEASES.md |
    sort -n -u
)

[[ "${version_count}" -gt 0 ]] || fail "no supported Trino versions found in RELEASES.md"

selected=()
selected_count=0

add_version() {
  local version="$1"
  local index

  [[ -n "${version}" ]] || return 0
  [[ "${version}" != "${target_version}" ]] || return 0
  [[ "${selected_count}" -lt 3 ]] || return 0

  for ((index = 0; index < selected_count; index++)); do
    [[ "${selected[index]}" != "${version}" ]] || return 0
  done

  selected[selected_count]="${version}"
  selected_count=$((selected_count + 1))
}

# Adds a supported release that no other release stands in for, also when the
# three samples are already selected.
add_required_version() {
  local required="$1"
  local index

  [[ "${required}" != "${target_version}" ]] || return 0
  for ((index = 0; index < selected_count; index++)); do
    [[ "${selected[index]}" != "${required}" ]] || return 0
  done
  for ((index = 0; index < version_count; index++)); do
    if [[ "${versions[index]}" == "${required}" ]]; then
      selected[selected_count]="${required}"
      selected_count=$((selected_count + 1))
      return 0
    fi
  done
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

# Preserve the broad version samples above, then include a different remote
# release with native EWKB geospatial support when one is available.
if (( 10#${target_version} >= 481 )); then
  geospatial_version="$(highest_less_than "${target_version}")"
  if [[ -n "${geospatial_version}" ]] && (( 10#${geospatial_version} >= 481 )); then
    add_version "${geospatial_version}"
  fi
fi

# Trino 482 alone drops the negation of a negated NULLIF and times out on a cast
# compared with date_trunc, so no other release shows that the connector keeps
# such expressions local.
add_required_version 482

if [[ "${selected_count}" -eq 0 ]]; then
  printf '\n'
else
  printf '%s\n' "${selected[@]}" | sort -n | tr '\n' ' ' | sed 's/ $//'
  printf '\n'
fi
