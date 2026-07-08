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

## Test Suites

| Test class | Coverage |
|------------|----------|
| `TestTrinoTypeParser` | Type name parsing |
| `TestTrinoTypeMapping` | Type mapping and transport modes |
| `TestTrinoUnsupportedTypeHandling` | Unsupported type fallback |
| `TestTrinoConnectorTest` | Connector contract tests |
| `TestTrinoConnectorIntegration` | Integration tests |

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
a separate Trino 482 cluster backed by a Delta Lake catalog. It reuses the
`target/trino-trino-482` package produced by `mvn -B clean verify`.

To run the same smoke test locally:

```bash
mvn -B clean verify
testing/remote-delta-smoke/run.sh
```

This starts local and remote Trino containers, MinIO, and Hive Metastore for
the remote Delta smoke test.
Failure diagnostics are written to `target/remote-delta-smoke/`. See
`docs/remote-delta-smoke.md` for details.

## Documentation

- `README.md` — user-facing overview and usage guide
- `docs/src/main/sphinx/connector/trino.md` — detailed connector reference (Sphinx format)
