#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

extract_parent_version() {
  grep -A 3 '<parent>' pom.xml | grep '<version>' | head -n 1 | sed -E 's/.*<version>([^<]+)<.*/\1/'
}

html_to_text() {
  python3 -c '
import html
import re
import sys

data = sys.stdin.read()
data = re.sub(r"(?is)<(script|style).*?</\1>", "\n", data)
data = re.sub(r"(?i)<br\s*/?>", "\n", data)
data = re.sub(r"(?i)</?(li|p|h[1-6]|tr|div|section)\b[^>]*>", "\n", data)
data = re.sub(r"(?s)<[^>]+>", " ", data)
data = html.unescape(data)
for line in data.splitlines():
    line = re.sub(r"\s+", " ", line).strip()
    if line:
        print(line)
'
}

requested_version="${1:-}"
current_version="${2:-}"

[[ "$requested_version" =~ ^[0-9]+$ ]] || fail "requested version must be an integer"

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || fail "not inside a git repository"
cd "$repo_root"

if [[ -z "$current_version" ]]; then
  current_version="$(extract_parent_version)"
fi
[[ "$current_version" =~ ^[0-9]+$ ]] || fail "current version must be an integer"

if (( 10#$requested_version >= 10#$current_version )); then
  fail "backport impact review requires requested_version < current_version"
fi

start=$((10#$requested_version + 1))
end=$((10#$current_version))

cat <<EOF
backport_range=${start}..${end}
release_note_base=https://trino.io/docs/current/release/release-<version>.html

Review these matches as evidence only. Cross-check any matching feature,
type, API, property, or plan marker against current source/tests/docs before
editing.

EOF

risk_pattern='Add the .* type|Add .* type|Add support for|Breaking change|Remove|Defunct|SPI|JDBC driver|Base JDBC|connector API|configuration property|trino-base-jdbc|trino-client|trino-jdbc|trino-parser|trino-matching|trino-plugin-toolkit|Airlift'

for version in $(seq "$start" "$end"); do
  url="https://trino.io/docs/current/release/release-${version}.html"
  html="$(curl -fsSL "$url")" || fail "failed to fetch $url"
  matches="$(
    printf '%s\n' "$html" |
      html_to_text |
      grep -E -i "$risk_pattern" |
      sed 's/^[[:space:]]*//;s/[[:space:]]*$//' |
      awk '!seen[$0]++' ||
      true
  )"

  if [[ -n "$matches" ]]; then
    printf 'release-%s %s\n' "$version" "$url"
    printf '%s\n' "$matches" | sed 's/^/  - /'
    printf '\n'
  fi
done

cat <<'EOF'
Suggested follow-up searches:
  rg -n "<release-note symbol or property>" src testing docs README.md docker-compose.yml .github
  rg -n "RemoteTrinoQuery|EXPLAIN|projection|merge|number|NumberType|SqlNumber" src testing docs README.md

Classify likely issues as:
  must-remove, signature-risk, runtime-config-risk, test-expectation-risk, or needs-compile-confirmation.
EOF
