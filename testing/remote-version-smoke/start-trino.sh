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

# The image sizes the heap to 80% of the container memory. After many queries, the
# classes and compiled code outside the heap need more than the remaining 20%, and the
# container is killed. A fixed maximum heap leaves room for them, and the JVM lowers the
# initial heap to it. Pass a single -J option: the launchers of Trino 480 to 483 join
# repeated -J values into one JVM argument.
jvm_options=()
if [[ -n "${SMOKE_JVM_HEAP:-}" ]]; then
  jvm_options=(-J "-Xmx${SMOKE_JVM_HEAP}")
fi

if [[ -x /usr/lib/trino/bin/run-trino ]]; then
  exec /usr/lib/trino/bin/run-trino "${jvm_options[@]}"
fi

exec /usr/lib/trino/bin/launcher run --etc-dir /etc/trino "${jvm_options[@]}"
