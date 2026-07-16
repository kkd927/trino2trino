#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_jvm_size="${TRINO_FAST_TEST_JVM_SIZE:-2g}"

cd "${repo_root}"
exec mvn -B \
  -Dnjord.enabled=false \
  -Dair.check.skip-all=true \
  -Dair.test.jvmsize="${test_jvm_size}" \
  test \
  "$@"
