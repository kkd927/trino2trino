# trino2trino

[![Build and Test](https://github.com/kkd927/trino2trino/actions/workflows/build.yml/badge.svg)](https://github.com/kkd927/trino2trino/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Trino](https://img.shields.io/badge/Trino-481-blue)

A read-only [Trino](https://trino.io/) connector that queries a remote Trino cluster via JDBC.

## What is this?

trino2trino lets you expose a remote Trino cluster as a local read-only catalog. This enables cross-cluster joins and federated queries without ETL pipelines, data duplication, or schema synchronization.

```text
Trino A (local)
  |- local catalogs
  +- remote  ->  trino2trino  ->  Trino B
```

```sql
SELECT l.id, r.metric
FROM local_catalog.schema.table l
JOIN remote.schema.table r ON l.id = r.id;
```

## Quick Start

### 1. Install

Choose the release matching your local Trino version from the
[supported Trino releases](https://github.com/kkd927/trino2trino/blob/main/RELEASES.md), download its plugin ZIP, and extract it
into the Trino plugin directory:

```bash
# Example for Trino 481
unzip trino-trino-481.zip -d /usr/lib/trino/plugin/trino/
```

### 2. Configure

Create a catalog file such as `etc/catalog/remote.properties`:

```properties
connector.name=trino
connection-url=jdbc:trino://remote-trino-host:443/catalog_name?SSL=true
connection-user=myuser
connection-password=mypassword
```

Each connector instance maps exactly one remote catalog to one local catalog.

### 3. Query

```sql
SELECT * FROM remote.schema.table LIMIT 10;
```

## Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `connection-url` | JDBC URL to remote Trino (`jdbc:trino://host:port/catalog[/schema]`) | Required |
| `connection-user` | Username for remote Trino | OS user |
| `connection-password` | Password for remote Trino | - |
| `unsupported-type-handling` | Final fallback for truly unsupported types: `IGNORE` or `CONVERT_TO_VARCHAR` | `CONVERT_TO_VARCHAR` |
| `remote-delegation.enabled` | Enable Trino-native SQL rendering for remote fragments | `true` |

## Supported / Not Supported

| Feature | Supported | Notes |
|---------|-----------|-------|
| `SELECT` | Yes | |
| `JOIN` (cross-cluster) | Yes | |
| Predicate pushdown | Partial | Native columns and typed VARCHAR-transport temporal/interval columns only; JSON/VARBINARY transport columns are excluded |
| Projection pushdown | Yes | Trino-native expressions are delegated when the compatibility registry allows them |
| Aggregation pushdown | Partial | `count`, `count distinct`, `count_if`, `checksum`, `min/max`, `sum`, `avg` are pushed down for supported types; `stddev`, `variance`, `covariance`, `correlation`, `regression` are not |
| `LIMIT` / `ORDER BY ... LIMIT` | Yes | Remote TopN reduces transferred rows; local TopN verifies ordering because transport projection can wrap the remote query |
| Same-remote join pushdown | Partial | All comparison operators are supported, including `IS NOT DISTINCT FROM` and varchar inequalities; joins stay local when the cost-based strategy declines or a constant join condition is not an exact numeric or varchar |
| `TABLE(system.query(...))` passthrough | Yes | Top-level query statements only; remote access control is the security boundary |
| Table statistics (`SHOW STATS`) | Yes | Uses remote `SHOW STATS FOR <table>` |
| `INSERT` / `UPDATE` / `DELETE` / `MERGE` | No | Read-only connector |
| `CREATE` / `ALTER` / `DROP` | No | Read-only connector |
| User identity propagation | No | Uses configured `connection-user` |
| Remote session properties / roles | No | |

## Type Support

The connector uses five transport modes to maximize type coverage:

| Transport Mode | Strategy | Examples |
|---------------|----------|----------|
| **NATIVE** | JDBC reads the type exactly | `boolean`, `bigint`, `number`, `varchar`, `date`, `uuid`, `array(varchar)`, `map(varchar, bigint)`, `row(id uuid, data json)` |
| **VARCHAR transport** | Lossless string projection → decode back | `timestamp(p) with time zone`, `time with time zone`, intervals, high-precision `timestamp(p>9)` |
| **VARBINARY transport** | Project as `VARBINARY` → decode back | `HyperLogLog`, `P4HyperLogLog`, `qdigest(T)`, `setdigest`, `tdigest` |
| **JSON transport** | Recursive JSON rewrite → decode back | `array(timestamp(3))`, `array(timestamp(12))`, `array(time(3))`, `array(date)`, `map(varchar, interval day to second)`, structural columns whose non-native descendants can be represented safely through JSON transport |
| **UNSUPPORTED** | Fallback (`IGNORE` or `CONVERT_TO_VARCHAR`) | Opaque or connector-specific types without a safe transport rule |

See the [connector reference](docs/src/main/sphinx/connector/trino.md) for detailed type transport rules.

## Pushdown & Statistics

- Predicate pushdown for native columns and typed VARCHAR-transport temporal/interval columns
- Trino-native expression and projection delegation for compatible casts, arithmetic, comparisons, `LIKE`, `IN`, regexp, JSON, date/time, dereference, and subscript expressions
- `LIMIT` and partial `ORDER BY ... LIMIT` pushdown with local ordering verification
- Aggregation pushdown for `count`, `count distinct`, `count_if`, `checksum`, `min/max`, `sum`, `avg` on supported types
- Same-remote join pushdown for supported join shapes
- Table statistics via `SHOW STATS FOR <table>` on the remote side

`AUTOMATIC` join pushdown requires remote column statistics, including the
distinct-value count for join keys. If the remote catalog only reports a table
row count, the join safely remains local.

Remote delegation delegates compatible remote subtrees and safely falls back to
local evaluation for unsupported expressions. Disabling it
(`remote-delegation.enabled=false`) leaves only the baseline JDBC pushdown path
enabled. The equivalent catalog session property is `remote_delegation_enabled`.

See the [connector reference](docs/src/main/sphinx/connector/trino.md) for pushdown behavior on transport-backed columns.

## Passthrough

Use `system.query` for remote-native SQL that connector SPI pushdown does not express cleanly:

```sql
SELECT *
FROM TABLE(
    remote.system.query(
        query => 'SELECT * FROM information_schema.tables LIMIT 10'
    )
);
```

- The inner SQL is sent without expression rewriting; the connector can strip a
  trailing semicolon and wrap the query to assign stable output aliases
- Output columns still go through normal transport rules
- Only top-level query statements (`SELECT`, `WITH`, `VALUES`, `TABLE`) are
  accepted. This syntactic check does not prove that invoked functions or table
  functions are free of side effects; remote access control and read-only
  credentials are the execution boundary
- Unlike normal table access, `system.query` is explicit user SQL; remote
  query preparation and execution failures are returned directly and are not
  treated as fallback candidates.

## Scope Model

- Normal table access maps one local catalog to one configured remote catalog;
  this 1:1 mapping is the contract of the default path
- All schemas under that remote catalog are exposed through normal metadata and table access
- Multiple remote catalogs require multiple local catalog property files
- `system.query` is an explicit escape hatch from that mapping: passthrough SQL
  may reference whatever the remote credentials can access

## Limitations

- Read-only connector surface: no `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `ALTER`, `DROP`
- `system.query` accepts only top-level query statements; top-level DDL, DML,
  and CALL statements are rejected before remote execution, but functions and
  table functions remain governed by remote access control
- `CALL system.execute(...)` is inherited from the base JDBC framework but is
  denied by this connector's read-only access control
- All remote SQL executes as the configured `connection-user`; end-user identity is not propagated
- Remote session properties and roles are not propagated
- Session-sensitive functions and casts such as `current_timestamp`,
  `current_date`, current time zone functions, locale-sensitive date formatting,
  and casts that depend on the session start date are evaluated locally.
  Time-zone-dependent expressions are delegated only when local and remote
  time zones match.
- Remote delegation probes `CHAR` to `VARCHAR` cast semantics and delegates
  these casts only when the remote retains the padding expected by Trino 481.
- Cross-cluster joins can only be improved with pushdown and statistics; the connector cannot remove the structural cost of federating between clusters
- Tested against Trino 481 querying remote Trino 481; cross-version compatibility is not claimed yet

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build, test, and development instructions.

### Delta Lake smoke test

The default `Build and Test` CI workflow runs a Docker-based remote Delta smoke
test after `mvn -B clean verify`. It covers the common deployment pattern where
a smaller federated Trino cluster queries a separate Delta Lake-focused Trino
cluster.

To run the same smoke test locally:

```bash
mvn -B clean verify
testing/remote-delta-smoke/run.sh
```

Failure diagnostics are written to `target/remote-delta-smoke/`. See
[docs/remote-delta-smoke.md](docs/remote-delta-smoke.md) for the topology and assertions.

## License

[Apache License 2.0](LICENSE)

## Acknowledgments

- Related upstream discussion: [trinodb/trino#21791](https://github.com/trinodb/trino/issues/21791)
