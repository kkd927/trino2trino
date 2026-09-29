# Remote version smoke

This is a release-time diagnostic smoke test for Trino-to-Trino federation
against a remote Trino version that may differ from the local plugin build.
It is not a compatibility guarantee and is intentionally narrower than the
remote Delta smoke test.

The probe starts two Trino containers:

- local Trino at the version from `pom.xml`, with the locally built
  `target/trino-trino-<version>` plugin mounted alongside geospatial
- remote Trino at the version passed to `run.sh`, exposing `tpch` and `memory`
  catalogs and loading those connector plugins alongside geospatial

Each container is limited to 1536 MiB of memory and two CPUs by default. The
limits can be adjusted with `REMOTE_VERSION_SMOKE_MEMORY_LIMIT` and
`REMOTE_VERSION_SMOKE_CPU_LIMIT`. Readiness polling uses the lightweight
`/v1/info` endpoint instead of starting a CLI query for every attempt, and
related assertions are batched to avoid repeatedly starting the Trino CLI JVM.

Every version pair checks user-written remote geospatial SQL returning text
through `system.query`. This does not test native geospatial column reads.
Native `Geometry` and `SphericalGeography` reads, including a 2D EWKB value
with SRID and a nested array with NULL, are checked only when both Trino
versions are at least 481. Remote versions before 481 do not provide
`ST_AsEWKB`, so native geospatial reads from them are outside the supported
scope. For the current Trino 482 plugin, native reads are checked against
remote 481; SRID preservation is covered by the 482-to-482 integration tests.
This does not validate the plugin release built for local Trino 481.
The release version selector includes a compatible older remote version when
one is available, in addition to its broad version samples.

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
