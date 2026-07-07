# Basic remote-version smoke probe

This is a release-time diagnostic probe for basic Trino-to-Trino federation
against a remote Trino version that may differ from the local plugin build.
It is not a compatibility guarantee and is intentionally narrower than the
strict Delta smoke test.

The probe starts two Trino containers:

- local Trino at the version from `pom.xml`, with the locally built
  `target/trino-trino-<version>` plugin mounted
- remote Trino at the version passed to `run.sh`, exposing only the `tpch`
  catalog

Run after building the plugin:

```bash
mvn -B clean verify
testing/basic-remote-smoke/run.sh <remote-version>
```

Multiple remote versions can be checked sequentially:

```bash
testing/basic-remote-smoke/run.sh <remote-version> <another-remote-version>
```

Diagnostics are written under
`target/basic-remote-smoke/<local-version>-to-<remote-version>/` on failure, or
on success when `BASIC_REMOTE_SMOKE_ALWAYS_LOGS=true` is set.
