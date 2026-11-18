# trino2trino

[![Build and Test](https://github.com/kkd927/trino2trino/actions/workflows/build.yml/badge.svg)](https://github.com/kkd927/trino2trino/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Trino](https://img.shields.io/badge/Trino-484-blue)

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
[supported Trino releases](RELEASES.md), download its plugin ZIP, and extract it
into the Trino plugin directory:

```bash
# Example for Trino 484
unzip trino-trino-484.zip -d /usr/lib/trino/plugin/
```

The archive contains a `trino-trino-484` directory, which becomes the plugin
directory. Trino loads only the JAR files directly inside a plugin directory, so
do not extract the archive into a subdirectory of the plugin directory.

When you upgrade the connector, first delete the plugin directory of the
previous release, such as `/usr/lib/trino/plugin/trino-trino-483`, or the
directory that you created for the connector under another name. Trino fails to
start with `Connector 'trino' is already registered` when two plugin directories
contain the connector.

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
| `remote-server.check-interval` | Time after which the connector checks, before it plans the next query, that the remote server did not restart or change. See [Changes of the remote server](#changes-of-the-remote-server) | `10s` |

## Supported / Not Supported

| Feature | Supported | Notes |
|---------|-----------|-------|
| `SELECT` | Yes | |
| `JOIN` (cross-cluster) | Yes | |
| Predicate pushdown | Partial | Native columns and typed VARCHAR-transport temporal/interval columns only; JSON/VARBINARY/geospatial transport columns are excluded |
| Projection pushdown | Partial | Trino-native expressions are delegated when the compatibility registry allows them, and functions, casts, and arithmetic only to a remote server of the local version; geospatial expressions remain local |
| Aggregation pushdown | Partial | `count`, `count distinct`, `count_if`, `checksum`, `min/max`, `sum`, `avg` are pushed down for supported types; `stddev`, `variance`, `covariance`, `correlation`, `regression` are not |
| `LIMIT` / `ORDER BY ... LIMIT` | Yes | Remote TopN reduces transferred rows; local TopN verifies ordering because transport projection can wrap the remote query |
| Same-remote join pushdown | Partial | Supported comparison operators include `IS NOT DISTINCT FROM` and varchar inequalities; geospatial keys are excluded, and joins stay local when the cost-based strategy declines or a constant join condition is not an exact numeric or varchar |
| `TABLE(system.query(...))` passthrough | Yes | Top-level query statements only; remote access control is the security boundary |
| Table statistics (`SHOW STATS`) | Yes | Uses remote `SHOW STATS FOR <table>` |
| `INSERT` / `UPDATE` / `DELETE` / `MERGE` | No | Read-only connector |
| `CREATE` / `ALTER` / `DROP` | No | Read-only connector |
| User identity propagation | No | Uses configured `connection-user` |
| Remote session properties / roles | No | |

## Type Support

The connector uses six transport modes to maximize type coverage:

| Transport Mode | Strategy | Examples |
|---------------|----------|----------|
| **NATIVE** | JDBC reads the type exactly | `boolean`, `bigint`, `number`, `varchar`, `date`, `uuid`, `array(varchar)`, `map(varchar, bigint)`, `row(id uuid, data json)` |
| **VARCHAR transport** | Lossless string projection → decode back | `timestamp(p) with time zone`, `time with time zone`, intervals, high-precision `timestamp(p>9)` |
| **VARBINARY transport** | Project as `VARBINARY` → decode back | `HyperLogLog`, `P4HyperLogLog`, `qdigest(T)`, `setdigest`, `tdigest` |
| **Geospatial EWKB transport** | Project EWKB bytes → restore native type | `Geometry`, `SphericalGeography` |
| **JSON transport** | Recursive JSON rewrite → decode back | `array(timestamp(3))`, `array(timestamp(12))`, `array(time(3))`, `array(date)`, `map(varchar, interval day to second)`, structural columns whose non-native descendants can be represented safely through JSON transport |
| **UNSUPPORTED** | Fallback (`IGNORE` or `CONVERT_TO_VARCHAR`) | Opaque or connector-specific types without a safe transport rule |

See the [connector reference](docs/src/main/sphinx/connector/trino.md) for detailed type transport rules.

Day-time interval columns support up to 6 fractional-second digits. Source
columns with 7 to 12 fractional-second digits use the configured unsupported-type
handling, including inside complex types. Expressions producing these intervals
(such as subtracting `timestamp(9)` values) remain local when their result cannot
be read back with the same type.

Interval filter pushdown follows the remote column type name. Trino 484 and
newer report interval precision, and filters on those columns are pushed down.
Earlier versions report bare `interval day to second` and `interval year to month`
names. Filters on those columns run locally, and the connector reads them as
`interval day(9) to second(3)` and `interval year(9) to month`, the types the
engine uses for stored pre-484 definitions. Day-time values must fit a signed
64-bit microsecond count; larger legacy values fail with `JDBC_ERROR`.

### Remote Trino versions

The connector supports a remote server of the local version. To a remote server
of another version it sends only the basic list below. The release tests run
the basic list against a few older releases, the oldest of which is Trino 448.
They are not a general cross-version compatibility guarantee.

A remote server can return different results than the local Trino version for
the same expression, because of behavior changes and fixes between the two
releases. The connector therefore compares the version that the remote server
reports with the version of the local server, and checks three settings of a
remote server of that version:

| Remote server | Evaluated by the remote server |
|---|---|
| Reports the version of the local server and uses none of the [settings below](#remote-server-settings) | The full list: everything in [Pushdown & Statistics](#pushdown--statistics) |
| Reports any other version, or uses one of those settings | The basic list |

The basic list holds the operations that the connector sends to every remote
server:

- column filters
- comparisons between columns (`=`, `<>`, `<`, `<=`, `>`, `>=`,
  `IS [NOT] DISTINCT FROM`), `AND`, `OR`, `NOT`, `IS NULL`, and `IN`
- joins on those comparisons
- `count`, `count_if`, `checksum`, `min`, `max`, `sum`, and `avg`, with
  `GROUP BY`
- `ORDER BY ... LIMIT` and `LIMIT`

These apply to `boolean`, `tinyint`, `smallint`, `integer`, `bigint`, `real`,
`double`, `decimal`, `varchar`, `char`, `varbinary`, `date`, `time`,
`time with time zone`, `timestamp`, `timestamp with time zone`, `uuid`, and
`ipaddress` values. `<>`, `IS NULL`, `GROUP BY`, `count`, and `checksum` also
apply to `array`, `map`, and `row` values of those types. The remote version smoke test
runs every operation on every type against each remote version it tests, and
requires the result of the local server.

For a remote server of another version, the connector evaluates everything else
locally, on the rows that pass the pushed down filters. The basic list leaves
out these operations:

| Not in the basic list | Reason |
|---|---|
| Functions, casts, arithmetic, `LIKE`, and `NULLIF`, including the casts that the engine adds, such as the cast to `bigint` in `sum` and `avg` of smaller integer types and the cast in a comparison between different types | Their results change from release to release. Casts from `varchar` to integer types keep surrounding spaces before Trino 458, decimal multiplication returns another scale from Trino 477 through 480, Trino 482 drops the negation of a negated `NULLIF`, and servers before 484 return `timestamp(p) with time zone` values with p > 3 beyond the range that Trino 484 accepts |
| `number` values | Servers before 482 compare them by their binary form |
| Ordering comparisons, `min`, `max`, and `ORDER BY ... LIMIT` on `array` and `row` values | Servers before 482 fail when a null element decides the order |
| `IN` on `array`, `map`, and `row` values | Trino 448 checks too few elements for null |
| `=` and `IS [NOT] DISTINCT FROM` between `array`, `map`, or `row` columns, and joins on them | Servers before 482 fail an equality join on arrays and rows with a null element, because they order the values for the dynamic filter of the join. The connector cannot tell the condition of a join from a filter |
| `=`, `IS [NOT] DISTINCT FROM`, and `IN` between `real`, `time with time zone`, or `timestamp with time zone` columns, and joins on them | Servers before 484 compare a list of 2 to 7 such values by their raw form, in which equal values can differ by the sign of zero or by the time zone. They do so for an `IN` list and for the dynamic filter of an equality join, so a join on a `real` column misses the rows whose zero has the other sign. `<>` and ordering comparisons stay in the basic list |
| `IN` on `uuid` and `ipaddress` values inside an expression | The connector cannot send constants of these types in an expression, so the smoke test has no list to compare |
| Interval and JSON values | Servers before 484 have other interval types and normalize JSON text |
| `avg` of decimals with precision above 18 | Servers before 484 return a wrong average when the sum overflows 128 bits |
| Grouped aggregations with `DISTINCT` aggregates over different arguments | Servers before 484 can drop the group of NULL keys |
| `FULL JOIN` | Servers before 484 drop the unmatched row of an input with exactly one row when the join condition refers only to the other input |

Two servers have the same version only when their version strings are equal. A
build of Trino 484 that reports `484-patch1` receives the basic list from a
local server that reports `484`. A remote server newer than the local server
also receives the basic list. The smoke test of a connector release cannot cover
a remote release that did not exist yet, so upgrade the local cluster, with the
connector release for its version, before the remote cluster.

#### Remote server settings

A remote server of the local version also receives only the basic list when it
uses one of these settings, because it then evaluates the full list differently
from a server with default settings:

| Configuration property of the remote server | What it changes |
|---|---|
| `deprecated.legacy-arithmetic-decimal-operators=true` | The precision and scale of decimal `+`, `-`, `*`, and `/` |
| `deprecated.regex-library=RE2J` | The results of regular expression functions |
| `deprecated.legacy-varchar-to-char-coercion=true`, or the session property `legacy_varchar_to_char_coercion` | Casts from `char` to `varchar`, which keep the padding |

The connector finds these settings with one query, together with the remote
version. It runs the query again with every
[check of the remote server](#changes-of-the-remote-server), in the remote
session of the query that it plans at that time, so a remote server must use the
same settings for every user of the catalog.

The connector cannot read the settings of the local server:

- With `deprecated.legacy-arithmetic-decimal-operators=true` on the local
  server, decimal `+`, `-`, `*`, and `/` run locally when the two sets of rules
  give them different result types.
- With `deprecated.regex-library=RE2J` on the local server and the default
  library on a remote server of the same version, regular expression functions
  would return the results of the remote library. Set
  `remote-delegation.enabled=false` in the catalog.
- When both servers use the same setting, results are correct, but the remote
  server receives only the basic list.

The connector also changes how it sends some queries, to every remote version:

- Filter values for `char` columns are sent as `CAST(? AS char(n))`. Servers
  from 482 compare a `char` column with a `varchar` value without padding, so a
  filter such as `c < 'ab'` would miss the value `ab` followed by a tab.
- Filters with single `number` values run locally for servers before 484, which
  report `number` columns with older JDBC metadata. Servers before 482 compare
  `number` values by their binary form, and a value rounded to the supported
  precision can keep trailing zeros. Range filters on `number` columns stay on
  the remote server.
- Filters on `real`, `double`, and `number` columns that select all values but
  a few listed ones, such as `x < 1 OR x > 1`, run locally. The SQL for such a
  filter is `NOT (x = ?)`, which also selects NaN, and which servers before 482
  evaluate by the binary form of a `number` value.

Bugs in how the remote server reads its own tables affect results with and
without pushdown.

The connector does not work around the bugs of a remote release. The remote
server evaluates what the connector pushes down, as it does for a query that a
user sends to it:

- Trino 473 returns duplicate groups for a `GROUP BY` or `DISTINCT` with more
  than about 31 million groups, which Trino 474 fixed. For a Trino 473 remote
  server, set `aggregation-pushdown.enabled=false` in the catalog.
- Servers before 477 can return incorrect results after they unspill a hash
  aggregation. For such a remote server with `spill-enabled=true`, set
  `aggregation-pushdown.enabled=false` in the catalog.
- Servers before 484 compare an `IN` list with 2 to 7 values by their raw
  internal values when every listed value fits in 32 bits. They then miss rows
  that are equal to a listed value but stored with another time zone, offset,
  or sign of zero. The connector sends such a list for a column filter with
  several values, and for the dynamic filter of a join that the local server
  evaluates. A join on a `real` column can then miss the rows whose zero has
  the other sign. This applies to `timestamp(p) with time zone` values with p
  of at most 3 within about 8 minutes 44 seconds of 1970-01-01 00:00:00 UTC,
  `time(p) with time zone` values with p of at most 9 less than about 1.05 ms
  after midnight, and `real` zero.

Native geospatial reads require `ST_AsEWKB` on the remote Trino instance
(available from Trino 481) and an EWKB-backed geospatial type on the local
instance. Release verification checks the Trino 484 plugin against remote Trino
483 for `Geometry` and `SphericalGeography` reads; this does not
mean that the existing 481 and 482 plugin releases contain this feature.
Remote Trino 480 and older are not supported for native geospatial reads.
There is no automatic VARCHAR fallback for these remote versions.

With Trino 484 on both sides, geospatial reads also work inside `ARRAY`,
`MAP`, and `ROW` values, including map keys. `Geometry` SRIDs and Z coordinates
are preserved in the 484-to-484 tests; the 484-to-483 smoke test covers
2D values with SRID. Spatial functions on connector columns execute locally; geospatial
predicate and function pushdown and writes are not supported. Explicit SQL
inside `system.query` still executes on the remote Trino instance. Some local
`Geometry` equality joins may fail with dynamic filtering on Trino 484; normal
reads, local spatial functions, and joins on ordinary keys do not require
disabling dynamic filtering.

#### Changes of the remote server

The connector reads the remote version, remote server settings, function list,
and session time zone when it plans the first query of a catalog.
Before it plans a later query, it checks the remote server again when the last
check is older than `remote-server.check-interval`, 10 seconds by default. A
check reads the remote version and runs the settings query. When another server
process answers, or the version or a setting differs, the connector reads
everything again. An upgrade, a downgrade, or a restart of the remote server
with other settings needs no restart of the local catalog.

- A check is one request for the version and one query of constants on the
  remote server. It runs about once per interval while the catalog plans
  queries. One check runs at a time, and the queries that are planned meanwhile
  wait for it.
- When the remote server takes longer than the interval to restart, every query
  that the connector plans after the restart uses what the restarted server
  returned. In a test, a Trino 484 server of the official container image
  without data took 20 seconds to restart.
- When a host name or load balancer moves to another server that is already
  running, the connector can plan queries with the version and settings of the
  server before it until the interval ends. A shorter interval shortens that
  time.
- A query that was planned before the change and reads from the remote server
  after it runs as planned.
- If a check fails, for example because the remote server does not answer or
  its queue is full, the connector sends only the basic list, and it checks
  again after 10 seconds.
- The connector reads the version and settings of the one server that answers
  a check, and it cannot tell which server runs a query. A connection URL that
  leads to servers with different versions or settings, such as a gateway in
  front of several clusters, is not supported.

## Pushdown & Statistics

- Predicate pushdown for native columns and typed VARCHAR-transport temporal/interval columns
- Trino-native expression and projection delegation for compatible casts, arithmetic, comparisons, `LIKE`, `IN`, regexp, JSON, date/time, dereference, and subscript expressions. A remote server of another version than the local server, or with other [settings](#remote-server-settings), receives only the [basic list](#remote-trino-versions)
- `LIMIT` and partial `ORDER BY ... LIMIT` pushdown with local ordering verification
- Aggregation pushdown for `count`, `count distinct`, `count_if`, `checksum`, `min/max`, `sum`, `avg` on supported types
- Same-remote join pushdown for supported join shapes
- Table statistics via `SHOW STATS FOR <table>` on the remote side

Operations that compare or hash JSON values run locally for every remote server:
`=`, `<>`, `IS [NOT] DISTINCT FROM`, `IN`, `NULLIF`, `contains`,
`array_position`, `array_distinct`, `checksum`, `GROUP BY`, and `DISTINCT`, on
`json` values and on `array`, `map`, and `row` values that contain them. A JSON
value of Trino 484 can hold a date or time, for example from
`json_scalar(DATE '2026-01-01')`. The remote server sends it as a JSON string,
because JSON text has no date type, so the connector returns a JSON string
where the remote server holds a date. The remote server tells the two apart and
the local server cannot. Functions that read a JSON value as text or by
position, such as `json_extract_scalar`, `json_format`, and casts, are pushed
down and return the same result on either server.

Join pushdown is disabled by default (`join-pushdown.enabled`). `AUTOMATIC`
join pushdown requires remote column statistics, including the
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
  time zones match. The remote session uses the time zone of the JVM of the
  local node that opens the connection, unless the connection URL sets
  `timezone` or the remote server sets `sql.forced-session-time-zone`. All nodes
  of the local cluster must use the same JVM time zone, or the connection URL
  must set `timezone`.
- A remote server that reports another version than the local server evaluates
  only the [basic list](#remote-trino-versions). Functions, casts, and
  arithmetic then run locally, and so do the joins and aggregations above them.
- Cross-cluster joins can only be improved with pushdown and statistics; the connector cannot remove the structural cost of federating between clusters
- Full integration coverage uses Trino 484 querying remote Trino 484; bounded
  cross-version smoke tests cover selected older remote versions, not a general
  cross-version compatibility guarantee

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
