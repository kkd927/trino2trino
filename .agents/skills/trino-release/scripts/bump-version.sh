#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

replace_literal() {
  local file="$1"
  local from="$2"
  local to="$3"
  FROM="$from" TO="$to" perl -0pi -e 's/\Q$ENV{FROM}\E/$ENV{TO}/g' "$file"
}

replace_regex() {
  local file="$1"
  local pattern="$2"
  local replacement="$3"
  perl -0pi -e "s/${pattern}/${replacement}/g" "$file"
}

from_version="${1:-}"
to_version="${2:-}"
target_jdk="${3:-}"

[[ "$from_version" =~ ^[0-9]+$ ]] || fail "from version must be an integer"
[[ "$to_version" =~ ^[0-9]+$ ]] || fail "to version must be an integer"
[[ "$target_jdk" =~ ^[0-9]+$ ]] || fail "target JDK must be an integer major version"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || fail "not inside a git repository"
cd "$repo_root"

versioned_files=(
  "pom.xml"
  "README.md"
  "docker-compose.yml"
  "CONTRIBUTING.md"
  "docs/src/main/sphinx/connector/trino.md"
  "docs/delta-smoke.md"
  "testing/delta-smoke/docker-compose.yml"
  "testing/delta-smoke/run.sh"
)

workflow_files=(
  ".github/workflows/build.yml"
  ".github/workflows/release.yml"
)

for file in "${versioned_files[@]}" "${workflow_files[@]}"; do
  [[ -f "$file" ]] || fail "expected file not found: $file"
done

replace_literal "pom.xml" "<version>${from_version}</version>" "<version>${to_version}</version>"

for file in \
  "README.md" \
  "docker-compose.yml" \
  "CONTRIBUTING.md" \
  "docs/src/main/sphinx/connector/trino.md" \
  "docs/delta-smoke.md" \
  "testing/delta-smoke/docker-compose.yml" \
  "testing/delta-smoke/run.sh"
do
  replace_literal "$file" "trinodb/trino:${from_version}" "trinodb/trino:${to_version}"

  replace_literal "$file" "trino-trino-${from_version}" "trino-trino-${to_version}"

  replace_literal "$file" "Trino-${from_version}-blue" "Trino-${to_version}-blue"

  replace_literal "$file" "Trino ${from_version}" "Trino ${to_version}"
done

current_jdks=()
for file in "${workflow_files[@]}"; do
  value="$(sed -n 's/^[[:space:]]*java-version:[[:space:]]*["'\'']\{0,1\}\([0-9][0-9]*\)["'\'']\{0,1\}[[:space:]]*$/\1/p' "$file" | head -n 1)"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "could not parse java-version from $file"
  current_jdks+=("$value")
done

if [[ "${current_jdks[0]}" != "${current_jdks[1]}" ]]; then
  fail "workflow java-version drift detected before JDK update"
fi

current_jdk="${current_jdks[0]}"
if [[ "$current_jdk" != "$target_jdk" ]]; then
  for file in "${workflow_files[@]}"; do
    replace_regex "$file" 'Set up JDK [0-9]+' "Set up JDK ${target_jdk}"
    replace_regex "$file" 'java-version: "[0-9]+"' "java-version: \"${target_jdk}\""
  done
  replace_regex "CONTRIBUTING.md" '\*\*JDK [0-9]+\+\*\*' "**JDK ${target_jdk}+**"
fi

mkdir -p .mvn/modernizer
for file in violations.xml violations-production-code-only.xml; do
  curl -fsSL "https://raw.githubusercontent.com/trinodb/trino/${to_version}/.mvn/modernizer/${file}" \
    -o ".mvn/modernizer/${file}"
done

printf 'Changed files:\n'
git status --short -- "${versioned_files[@]}" "${workflow_files[@]}" .mvn/modernizer || true

printf '\nResidual references to %s outside ignored directories:\n' "$from_version"
if ! find . \
  \( -path './.git' -o -path './target' -o -path './.mvn' -o -path './.agents/skills/trino-release' \) -prune \
  -o -type f -print0 |
  xargs -0 grep -nI -E "\\b${from_version}\\b"
then
  printf 'none\n'
fi
