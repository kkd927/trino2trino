# Delta Lake remote-catalog smoke test

This repository's default test suite validates the generic Trino-to-Trino
contract with in-process Trino 480 query runners. It intentionally does not
start a Delta Lake stack for every pull request.

The optional Delta smoke test validates the common production shape where a
small federated Trino 480 cluster queries a separate Trino 480 cluster whose
remote catalog is backed by Delta Lake.

## Scope

The smoke test starts:

- `trino-local-delta`: Trino 480 with the `trino2trino` plugin mounted from
  `target/trino-trino-480`
- `trino-remote-delta`: Trino 480 with a native `delta_lake` catalog
- MinIO as S3-compatible object storage
- Apache Hive Metastore with an embedded Derby backend and Hadoop S3A support
  for validating Delta table locations stored in MinIO

The test creates tiny Delta tables through remote Trino, then queries them
through the local `remote_delta` catalog.

It verifies:

- metadata visibility through `SHOW`/`information_schema`
- basic Delta table reads
- decimal, date, array, map, row, and high-precision decimal values
- partition predicate filtering
- local-to-remote federation joins
- same-remote joins
- `TABLE(remote_delta.system.query(...))` passthrough
- `EXPLAIN` output showing remote Trino query delegation

This is a packaging and integration smoke test for `trino2trino`, not a full
Delta Lake connector certification suite.

## Run

Build the plugin first:

```bash
mvn -B -Dair.check.skip-all=true -DskipTests package
```

Run the smoke test:

```bash
testing/delta-smoke/run.sh
```

The script tears down the Docker Compose stack by default. Keep the stack
running for inspection with:

```bash
DELTA_SMOKE_KEEP_RUNNING=true testing/delta-smoke/run.sh
```

Useful endpoints while the stack is running:

- local federated Trino: `http://localhost:18080`
- remote Delta Trino: `http://localhost:19080`
- MinIO console: `http://localhost:19001`
- Hive Metastore thrift: `localhost:19083`

## Notes

- The Hive Metastore image extends `apache/hive:4.0.1` only by copying the
  bundled Hadoop S3A jars into Hive's runtime classpath. The smoke stack also
  mounts a small `core-site.xml` so the metastore can validate `s3://` Delta
  table locations against MinIO. It uses embedded Derby to keep the smoke test
  lightweight and self-contained. Production deployments should use a durable
  metastore backend.
- The Delta catalog uses MinIO credentials intended only for local testing.
- The default `mvn verify` path does not run this test.
