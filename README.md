# trino2trino

[![Build and Test](https://github.com/kkd927/trino2trino/actions/workflows/build.yml/badge.svg)](https://github.com/kkd927/trino2trino/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Trino](https://img.shields.io/badge/Trino-479-blue)

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

Download the plugin ZIP from [GitHub Releases](https://github.com/kkd927/trino2trino/releases) and extract it into the Trino plugin directory:

```bash
unzip trino-trino-479.zip -d /usr/lib/trino/plugin/trino/
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
| `trino.remote-delegation.enabled` | Enable Trino-native SQL rendering for remote fragments | `true` |
| `trino.remote-delegation.mode` | Delegation policy: `AUTO`, `OFF`, or `STRICT` | `AUTO` |

## Supported / Not Supported

| Feature | Supported | Notes |
|---------|-----------|-------|
| `SELECT` | Yes | |
| `JOIN` (cross-cluster) | Yes | |
| Predicate pushdown | Partial | Native columns and typed VARCHAR-transport temporal/interval columns only; JSON/VARBINARY transport columns are excluded |
| Projection pushdown | Yes | Trino-native expressions are delegated when the compatibility registry allows them |
| Aggregation pushdown | Partial | `count`, `count distinct`, `min/max`, `sum`, `avg` are pushed down; `stddev`, `variance`, `covariance`, `correlation`, `regression` are not |
| `LIMIT` / `ORDER BY ... LIMIT` | Yes | |
| Same-remote join pushdown | Partial | Supported join shapes only; `IS NOT DISTINCT FROM`, some inequality joins, and some complex joins are not pushed down |
| `TABLE(system.query(...))` passthrough | Yes | Row-returning read queries only |
| Table statistics (`SHOW STATS`) | Yes | Uses remote `SHOW STATS FOR <table>` |
| `INSERT` / `UPDATE` / `DELETE` / `MERGE` | No | Read-only connector |
| `CREATE` / `ALTER` / `DROP` | No | Read-only connector |
| User identity propagation | No | Uses configured `connection-user` |
| Remote session properties / roles | No | |

## Type Support

The connector uses five transport modes to maximize type coverage:

| Transport Mode | Strategy | Examples |
|---------------|----------|----------|
| **NATIVE** | JDBC reads the type exactly | `boolean`, `bigint`, `varchar`, `date`, `uuid`, `array(varchar)`, `map(varchar, bigint)`, `row(id uuid, data json)` |
| **VARCHAR transport** | `CAST(... AS VARCHAR)` → decode back | `time with time zone`, `interval year to month`, high-precision `timestamp(p>9)` |
| **VARBINARY transport** | Project as `VARBINARY` → decode back | `HyperLogLog`, `P4HyperLogLog`, `qdigest(T)`, `setdigest`, `tdigest` |
| **JSON transport** | Recursive JSON rewrite → decode back | `array(timestamp(12))`, `map(varchar, interval day to second)`, structural columns whose non-native descendants can be represented safely through JSON transport |
| **UNSUPPORTED** | Fallback (`IGNORE` or `CONVERT_TO_VARCHAR`) | Opaque or connector-specific types without a safe transport rule |

See the [connector reference](docs/src/main/sphinx/connector/trino.md) for detailed type transport rules.

## Pushdown & Statistics

- Predicate pushdown for native columns and typed VARCHAR-transport temporal/interval columns
- Trino-native expression and projection delegation for compatible casts, arithmetic, comparisons, `LIKE`, `IN`, regexp, JSON, date/time, dereference, and subscript expressions
- `LIMIT` and `ORDER BY ... LIMIT`
- Aggregation pushdown for `count`, `count distinct`, `min/max`, `sum`, `avg`
- Same-remote join pushdown for supported join shapes
- Table statistics via `SHOW STATS FOR <table>` on the remote side

`AUTO` mode delegates compatible remote subtrees and safely falls back to local
evaluation for unsupported expressions. `STRICT` mode fails a remote subtree when
it cannot be rendered for remote Trino. `OFF` leaves only the baseline JDBC
pushdown path enabled. Catalog session properties are
`remote_delegation_enabled` and `remote_delegation_mode`.

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

- The inner SQL string is sent to remote Trino as written
- Output columns still go through normal transport rules
- Table references must stay within the configured remote catalog
- Unlike normal table access, `system.query` is explicit user SQL; remote
  failures are returned directly and are not treated as fallback candidates.

## Scope Model

- Normal table access maps one local catalog to one configured remote catalog
- All schemas under that remote catalog are exposed through normal metadata and table access
- Multiple remote catalogs require multiple local catalog property files
- `system.query` may reference only the configured remote catalog; explicit cross-catalog references are rejected

## Limitations

- Read-only connector surface: no `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `ALTER`, `DROP`
- `system.query` is documented only for row-returning read queries
- All remote SQL executes as the configured `connection-user`; end-user identity is not propagated
- Remote session properties and roles are not propagated
- Session-sensitive functions such as `current_timestamp`, `current_date`, and
  current time zone functions are not delegated unless their semantics are
  represented by explicit, compatible SQL expressions.
- Negative dates (before year 0001) are not preserved correctly through JDBC
- Cross-cluster joins can only be improved with pushdown and statistics; the connector cannot remove the structural cost of federating between clusters
- Tested against Trino 479 querying remote Trino 479; cross-version compatibility is not claimed yet

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build, test, and development instructions.

## License

[Apache License 2.0](LICENSE)

## Acknowledgments

- Related upstream discussion: [trinodb/trino#21791](https://github.com/trinodb/trino/issues/21791)
