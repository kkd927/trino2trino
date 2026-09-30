# Contributing to trino2trino

## Prerequisites

- **JDK 25+**
- **Maven 3.9+**
- **Docker** and **Docker Compose** (for local testing)

## Build

```bash
# Build only (skip tests and enforcer checks)
mvn clean verify -DskipTests -Dair.check.skip-all=true

# Build with Trino enforcer checks
mvn clean verify -DskipTests

# Run all tests
mvn test -Dair.check.skip-all=true
```

For a lighter local edit/test loop, use the fast-test wrapper. It skips checks
that are still run by the full build and limits the test JVM heap to 2 GB:

```bash
testing/fast-test.sh -Dtest=TestTrinoConnectorTest
```

Additional Maven options are passed through. This wrapper is only for local
iteration; run `mvn -B clean verify` before submitting a change. Override the
heap when needed with `TRINO_FAST_TEST_JVM_SIZE`.

The geospatial integration tests for this Trino 481 source line load the matching
official geospatial plugin ZIP from the Trino GitHub release, caching it under
`target/`. For offline runs, set `TRINO_GEOSPATIAL_PLUGIN_ZIP` to a previously
downloaded `trino-geospatial-481.zip` file. When backporting to another local
Trino version, update the test plugin ZIP and version-specific spatial tests
before running the full suite.

## Test Suites

| Test class | Coverage |
|------------|----------|
| `TestTrinoTypeParser` | Type name parsing |
| `TestTrinoConnectorTest` | Base JDBC contract, integration, type mapping, and unsupported-type fallback |
| `TestGeospatialTransport` | EWKB transport classification, nested decoding, and malformed payload rejection |

## Local Docker Environment

A `docker-compose.yml` is included for local testing with two Trino instances:

```bash
# Build the plugin first
mvn clean verify -DskipTests -Dair.check.skip-all=true

# Start local (8080) and remote (9090) Trino instances
docker compose up -d

# Query remote Trino through the local instance
docker exec -it trino-local trino
```

```sql
-- Connected to trino-local (port 8080)
SELECT * FROM trino.tpch.tiny.nation LIMIT 5;
```

## Delta Lake Smoke Test

The default `Build and Test` CI workflow validates the packaged plugin against
a separate Trino 481 cluster backed by a Delta Lake catalog. It reuses the
`target/trino-trino-481` package produced by `mvn -B clean verify`.

To run the same smoke test locally:

```bash
mvn -B clean verify
testing/remote-delta-smoke/run.sh
```

This starts local and remote Trino containers, Adobe S3Mock, and Hive Metastore for
the remote Delta smoke test.
Failure diagnostics are written to `target/remote-delta-smoke/`. See
`docs/remote-delta-smoke.md` for details.

The separate [remote version smoke test](testing/remote-version-smoke/README.md)
checks selected different Trino versions, including native geospatial reads
when both versions support EWKB transport.

## Documentation

- `README.md` — user-facing overview and usage guide
- `docs/src/main/sphinx/connector/trino.md` — detailed connector reference (Sphinx format)
