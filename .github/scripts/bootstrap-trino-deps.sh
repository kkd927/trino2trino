#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

trino_version="$(grep -A 3 '<parent>' pom.xml | grep '<version>' | head -n 1 | sed -E 's/.*<version>([^<]+)<.*/\1/')"
local_artifact="${HOME}/.m2/repository/io/trino/trino-base-jdbc/${trino_version}/trino-base-jdbc-${trino_version}.pom"
remote_artifact_url="https://repo.maven.apache.org/maven2/io/trino/trino-base-jdbc/${trino_version}/trino-base-jdbc-${trino_version}.pom"

if [[ -f "${local_artifact}" ]]; then
  echo "Trino ${trino_version} JDBC artifacts already exist in the local Maven repository."
  exit 0
fi

if [[ "$(curl -s -o /dev/null -w '%{http_code}' "${remote_artifact_url}")" == "200" ]]; then
  echo "Trino ${trino_version} JDBC artifacts are available from Maven Central."
  exit 0
fi

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

echo "Bootstrapping Trino ${trino_version} JDBC artifacts from upstream sources."
git clone --depth 1 --branch "${trino_version}" https://github.com/trinodb/trino.git "${workdir}/trino"

pushd "${workdir}/trino" >/dev/null
mvn -pl core/trino-spi,client/trino-client,client/trino-jdbc,core/trino-parser,lib/trino-matching,lib/trino-plugin-toolkit,plugin/trino-base-jdbc -am install -DskipTests -Dair.check.skip-all=true
popd >/dev/null
