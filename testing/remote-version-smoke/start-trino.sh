#!/usr/bin/env bash
set -euo pipefail

readonly source_plugin_dir=/usr/lib/trino/plugin
readonly smoke_plugin_dir=/tmp/trino-smoke-plugins

rm -rf "${smoke_plugin_dir}"
mkdir -p "${smoke_plugin_dir}"

IFS=',' read -ra plugins <<< "${SMOKE_PLUGINS:?set SMOKE_PLUGINS}"
for plugin in "${plugins[@]}"; do
  source_path="${source_plugin_dir}/${plugin}"
  if [[ ! -d "${source_path}" ]]; then
    printf 'Required smoke-test plugin directory does not exist: %s\n' "${source_path}" >&2
    exit 1
  fi
  ln -s "${source_path}" "${smoke_plugin_dir}/${plugin}"
done

exec /usr/lib/trino/bin/run-trino
