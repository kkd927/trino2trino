# Remote version smoke

This is a release-time diagnostic smoke test for Trino-to-Trino federation
against a remote Trino version that may differ from the local plugin build.
It is not a compatibility guarantee and is intentionally narrower than the
remote Delta smoke test.

The probe starts two Trino containers:

- local Trino at the version from `pom.xml`, with the locally built
  `target/trino-trino-<version>` plugin mounted
- remote Trino at the version passed to `run.sh`, exposing `tpch` and `memory`
  catalogs

Run after building the plugin:

```bash
mvn -B clean verify
testing/remote-version-smoke/run.sh <remote-version>
```

Multiple remote versions can be checked sequentially:

```bash
testing/remote-version-smoke/run.sh <remote-version> <another-remote-version>
```

Diagnostics are written under
`target/remote-version-smoke/<local-version>-to-<remote-version>/` on failure, or
on success when `REMOTE_VERSION_SMOKE_ALWAYS_LOGS=true` is set.
