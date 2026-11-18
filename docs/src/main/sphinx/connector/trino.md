# Trino connector

The Trino connector queries a remote Trino cluster through JDBC and exposes it
as a local read-only catalog.

## Requirements

- Remote Trino must be reachable through the Trino JDBC driver
- Full integration coverage uses Trino 484 querying remote Trino 484; bounded
  smoke tests also cover selected older remote versions

## Configuration

Create a catalog properties file such as `etc/catalog/remote.properties`:

```{code-block} properties
connector.name=trino
connection-url=jdbc:trino://remote-trino-host:443/catalog_name
connection-user=myuser
connection-password=mypassword
```

Each connector instance maps one remote catalog to one local catalog.

Append `?SSL=true` for TLS connections:

```{code-block} properties
connection-url=jdbc:trino://remote-host:443/catalog_name?SSL=true
```

### Configuration properties

```{eval-rst}
.. list-table::
   :widths: 35, 55, 10
   :header-rows: 1

   * - Property name
     - Description
     - Default
   * - ``connection-url``
     - JDBC URL to the remote Trino instance
     - (required)
   * - ``connection-user``
     - Username for authentication
     -
   * - ``connection-password``
     - Password for authentication
     -
   * - ``unsupported-type-handling``
     - Final fallback for truly unsupported types: ``IGNORE`` or ``CONVERT_TO_VARCHAR``
     - ``CONVERT_TO_VARCHAR``
   * - ``remote-delegation.enabled``
     - Enable Trino-native SQL rendering for compatible remote fragments
     - ``true``
   * - ``remote-server.check-interval``
     - Time after which the connector checks, before it plans the next query,
       that the remote server did not restart or change. See
       :ref:`trino-remote-server-changes`
     - ``10s``
```

## Querying

After configuration, the remote catalog is visible through the local catalog:

```sql
SHOW SCHEMAS FROM remote;
SHOW TABLES FROM remote.myschema;
SELECT * FROM remote.myschema.mytable LIMIT 10;
```

### Cross-catalog joins

```sql
SELECT l.id, r.metric
FROM local_catalog.schema.table l
JOIN remote.external_schema.external_table r
    ON l.id = r.id;
```

## Type mapping

The connector uses six transport modes:

- **Native**: JDBC preserves the type exactly
- **VARCHAR transport**: unsupported scalar temporal or interval values are projected as ``VARCHAR`` and decoded back to the original logical type
- **VARBINARY transport**: top-level sketch types are projected as ``VARBINARY`` and decoded back to the original logical type
- **Geospatial EWKB transport**: top-level ``Geometry`` and ``SphericalGeography`` values are projected as EWKB and restored as native types
- **JSON transport**: unsupported structural columns are recursively rewritten, projected as JSON text, and decoded back to the original logical type
- **Unsupported**: only types that still cannot be represented safely

### Native reads

Native scalar reads include:

- ``boolean``
- ``tinyint``, ``smallint``, ``integer``, ``bigint``
- ``real``, ``double``, ``decimal(p,s)``
- ``number``
- ``char``, ``varchar``, ``varbinary``
- ``date``
- exact ``time(p<=9)`` and ``timestamp(p<=9)``
- ``uuid``, ``json``, ``ipaddress``

Native complex reads are allowed only when every descendant leaf is natively
readable. Date leaves, fractional ``time(p>0)`` leaves, and timestamp leaves at
every precision use JSON transport because the JDBC complex-value
representation cannot preserve them exactly.

Examples:

- ``array(varchar)``
- ``map(varchar, bigint)``
- ``row(id uuid, payload json)``
- ``row(events array(map(varchar, varchar)))``

### VARCHAR transport

Top-level scalar fallback currently covers:

- ``time with time zone``
- ``timestamp(p) with time zone`` at every precision; the wire value carries
  the UTC instant and original zone ID separately
- year-month intervals, preserving their field range and leading precision
  when reported by remote Trino 484 or newer
- day-time intervals with fractional-second precision up to 6, preserving their
  field range and precision
- high-precision ``time(p)`` and ``timestamp(p)`` when JDBC is not exact

These columns still appear locally as their original logical Trino types.
Source interval columns with fractional-second precision from 7 to 12 use the
configured unsupported-type handling, including inside complex types. Expressions
whose results cannot be read back with the same logical type are evaluated
locally. For example, subtracting two ``timestamp(9)`` columns can return a native
high-precision interval computed locally under either unsupported-type policy.

Remote versions before 484 report bare ``interval day to second`` and
``interval year to month`` type names. These map locally to
``interval day(9) to second(3)`` and ``interval year(9) to month``, the types
the engine uses when it reads stored pre-484 definitions. Day-time
values must fit a signed 64-bit microsecond count, from
``-106751991 04:00:54.775808`` to ``106751991 04:00:54.775807``. Larger legacy
values fail with ``JDBC_ERROR``; increasing the leading precision does not extend
this storage range.

### JSON transport

If a structural column contains a descendant that cannot be read natively, the
connector rewrites the whole column through JSON transport and reconstructs the
original logical type locally.

Examples:

- ``array(timestamp(12))``
- ``array(time(3))``
- ``array(date)``
- ``map(varchar, interval day to second)``
- ``row(ts timestamp(12), attrs map(varchar, varchar))``

JSON transport contract:

- ``map(varchar, v)`` stays a JSON object
- ``map(non-varchar, v)`` is normalized to ``array(row(key, value))``
- row fields are encoded positionally and reconstructed by the declared row type
- nested unsupported scalar leaves are encoded through string surrogates
- nested ``Geometry`` and ``SphericalGeography`` leaves use hex-encoded EWKB,
  including values in arrays, maps (keys and values), and rows

### Geospatial EWKB transport

``Geometry`` uses ``ST_AsEWKB(column)``; ``SphericalGeography`` uses
``ST_AsEWKB(to_geometry(column))``. The connector restores the original native
type locally. This requires ``ST_AsEWKB`` on the remote instance (available
from Trino 481) and an EWKB-backed geospatial type on the local instance. The
release verification checks the Trino 484 plugin against remote
483 for native geospatial reads. Existing plugin releases for local Trino 481
and 482 do not yet include this feature; they require separate backports and
verification. Native geospatial reads from remote Trino 480 or older are not
supported; there is no automatic ``VARCHAR`` fallback for these versions.

With Trino 484 on both sides, reads preserve ``Geometry`` SRIDs, actual Z
coordinates, empty and multi geometries, and nulls, including values nested in
arrays, maps (keys and values), and rows. Z preservation has been verified in
484-to-484 tests; the 484-to-483 smoke test covers 2D values with SRID. Ordinary tables and
``system.query`` results use the same transport path. A local Trino
installation must have its geospatial plugin available for native type
resolution (standard Trino distributions include it). This connector does not
write geospatial values or push down geospatial-key joins, predicates, or
functions. Some local ``Geometry`` equality joins may fail with dynamic
filtering on Trino 484; normal reads, local spatial functions, and joins on
ordinary keys do not require disabling dynamic filtering. The exact failing
call path has not been established.

### VARBINARY transport

Top-level statistical sketch types are projected as ``VARBINARY`` and decoded
back to the original logical Trino type locally.

Current coverage:

- ``HyperLogLog``
- ``P4HyperLogLog``
- ``qdigest(T)``
- ``setdigest``
- ``tdigest``

### Unsupported

``unsupported-type-handling`` remains the final fallback for types that still
cannot be represented safely. This mainly applies to opaque or connector-specific
types without an explicit transport rule, and nested descendants that do not yet
have a safe recursive transport contract.

## Query passthrough

Execute a manual top-level query statement on the remote Trino:

```sql
SELECT *
FROM TABLE(
    remote.system.query(
        query => 'SELECT * FROM information_schema.tables LIMIT 10'
    )
)
```

``system.query`` is an explicit bypass for the normal connector planning path:

- the inner SQL is sent without expression rewriting; the connector can strip
  a trailing semicolon and wrap the query to assign stable output aliases
- the connector still infers output columns and applies the normal type
  mapping and transport rules to the result
- only top-level query statements (``SELECT``, ``WITH``, ``VALUES``, ``TABLE``)
  are accepted; this syntactic check does not prove that invoked functions or
  table functions are free of side effects, so remote access control and
  read-only credentials are the execution boundary
- while normal table access maps 1:1 to the configured remote catalog,
  passthrough SQL is an explicit escape hatch from that mapping
- remote query preparation and execution failures are returned directly;
  explicit passthrough SQL is not a fallback candidate

## SQL support

The connector provides read-only access.

```{eval-rst}
.. list-table::
   :widths: 50, 50
   :header-rows: 1

   * - Feature
     - Support
   * - :doc:`SELECT </sql/select>`
     - Yes
   * - :doc:`INSERT </sql/insert>`
     - No
   * - :doc:`DELETE </sql/delete>`
     - No
   * - :doc:`UPDATE </sql/update>`
     - No
   * - :doc:`MERGE </sql/merge>`
     - No
   * - :doc:`CREATE TABLE </sql/create-table>`
     - No
   * - :doc:`CREATE TABLE AS </sql/create-table-as>`
     - No
   * - :doc:`DROP TABLE </sql/drop-table>`
     - No
   * - :doc:`ALTER TABLE </sql/alter-table>`
     - No
   * - :doc:`CREATE SCHEMA </sql/create-schema>`
     - No
   * - :doc:`DROP SCHEMA </sql/drop-schema>`
     - No
   * - :doc:`CREATE VIEW </sql/create-view>`
     - No
   * - ``COMMENT ON``
     - No
```

## Performance

### Pushdown

The connector supports:

- predicate pushdown
- projection pushdown
- Trino-native remote SQL delegation for compatible casts, comparisons, boolean
  logic, arithmetic, ``LIKE``, ``IN``, regexp, JSON, date/time functions,
  dereference, subscript, and aggregation expressions
- ``LIMIT`` pushdown
- partial ``ORDER BY ... LIMIT`` pushdown with local ordering verification
- aggregation pushdown for ``count``, ``count distinct``, ``count_if``,
  ``checksum``, ``min/max``, ``sum``, and ``avg`` on supported types
- same-remote join pushdown for supported join shapes

A remote server of another version than the local server, or with other
evaluation settings, receives only the basic list of
[Remote version differences](#remote-version-differences).

Operations that compare or hash JSON values run locally for every remote
server: ``=``, ``<>``, ``IS [NOT] DISTINCT FROM``, ``IN``, ``NULLIF``,
``contains``, ``array_position``, ``array_distinct``, ``checksum``,
``GROUP BY``, and ``DISTINCT``, on ``json`` values and on ``array``, ``map``,
and ``row`` values that contain them. A JSON value of Trino 484 can hold a date
or time, for example from ``json_scalar(DATE '2026-01-01')``. The remote server
sends it as a JSON string, because JSON text has no date type, so the connector
returns a JSON string where the remote server holds a date. The remote server
tells the two apart and the local server cannot. Functions that read a JSON
value as text or by position, such as ``json_extract_scalar``,
``json_format``, and casts, are pushed down and return the same result on
either server.

Join pushdown is disabled by default (``join-pushdown.enabled``).

Remote delegation delegates compatible remote subtrees and leaves unsupported
expressions as local fallback when that preserves semantics. Disabling it
(``remote-delegation.enabled=false``) leaves only the baseline JDBC pushdown
path enabled. The equivalent catalog session property is
``remote_delegation_enabled``.

Pushdown behavior for transport-backed columns is split:

- scalar ``VARCHAR`` transport (``timestamp(p>9)``,
  ``timestamp(p) with time zone``, ``time with time zone``, interval types)
  keeps tuple-domain predicate pushdown enabled via typed bind expressions such
  as ``CAST(? AS timestamp(12))`` or
  ``CAST(INTERVAL '0.000001' SECOND * CAST(? AS BIGINT) AS interval day(2) to second(6))``
- interval filter pushdown follows the remote column type name: columns that
  remote versions before 484 report as bare ``interval day to second`` or
  ``interval year to month`` are filtered locally. The decision uses only the
  column type, so the coordinator and every worker reach the same result even
  when a node cannot reach the remote server. If an interval filter still
  reaches a pre-484 server, the explicit parametric type makes the query fail
  instead of comparing against a zero interval
- interval expressions are evaluated only by a remote server of the local
  version, as described below. This includes interval intermediates in boolean
  or string expressions and applies to both remote delegation and the baseline
  JDBC rewriter
- structural ``JSON`` transport remains ``DISABLE_PUSHDOWN``
- sketch ``VARBINARY`` transport remains ``DISABLE_PUSHDOWN``
- geospatial EWKB transport remains ``DISABLE_PUSHDOWN``; spatial functions and
  predicates involving geospatial values are evaluated locally

This keeps remote filtering available where the connector can still bind the
original logical type safely, while avoiding pushdown on carrier-only
transports whose semantics would otherwise drift from the original remote
column.

### Remote version differences

The connector supports a remote server of the local version. To a remote
server of another version it sends only the basic list below. The release tests
run the basic list against a few older releases, the oldest of which is Trino
448. They are not a general cross-version compatibility guarantee.

A remote server can return different results than the local Trino version for
the same expression, because of behavior changes and fixes between the two
releases. The connector therefore compares the version that the remote server
reports with the version of the local server, and checks three settings of a
remote server of that version.

A remote server that reports the version of the local server and uses none of
the [settings below](trino-remote-server-settings) evaluates the full list:
everything under [Pushdown](#pushdown).

A remote server that reports any other version, or that uses one of those
settings, evaluates only the basic list, the operations that the connector
sends to every remote server:

- column filters
- comparisons between columns (``=``, ``<>``, ``<``, ``<=``, ``>``, ``>=``,
  ``IS [NOT] DISTINCT FROM``), ``AND``, ``OR``, ``NOT``, ``IS NULL``, and ``IN``
- joins on those comparisons
- ``count``, ``count_if``, ``checksum``, ``min``, ``max``, ``sum``, and
  ``avg``, with ``GROUP BY``
- ``ORDER BY ... LIMIT`` and ``LIMIT``

These apply to ``boolean``, ``tinyint``, ``smallint``, ``integer``, ``bigint``,
``real``, ``double``, ``decimal``, ``varchar``, ``char``, ``varbinary``,
``date``, ``time``, ``time with time zone``, ``timestamp``,
``timestamp with time zone``, ``uuid``, and ``ipaddress`` values. ``<>``,
``IS NULL``, ``GROUP BY``, ``count``, and ``checksum`` also apply to ``array``,
``map``, and ``row`` values of those types. The remote version smoke test runs every
operation on every type against each remote version it tests, and requires the
result of the local server.

For a remote server of another version, the connector evaluates everything else
locally, on the rows that pass the pushed down filters. This applies to both
remote delegation and the baseline JDBC rewriter. The basic list leaves out
these operations:

- functions, casts, arithmetic, ``LIKE``, and ``NULLIF``, including the casts
  that the engine adds, such as the cast to ``bigint`` in ``sum`` and ``avg``
  of smaller integer types and the cast in a comparison between different
  types. Their results change from release to release. Casts from ``varchar`` to integer types keep
  surrounding spaces before Trino 458, decimal multiplication returns another
  scale from Trino 477 through 480, Trino 482 drops the negation of a negated
  ``NULLIF``, and servers before 484 return ``timestamp(p) with time zone``
  values with p > 3 beyond the range that Trino 484 accepts
- ``number`` values. Servers before 482 compare them by their binary form
- ordering comparisons, ``min``, ``max``, and ``ORDER BY ... LIMIT`` on
  ``array`` and ``row`` values. Servers before 482 fail when a null element
  decides the order
- ``IN`` on ``array``, ``map``, and ``row`` values. Trino 448 checks too few
  elements for null
- ``=`` and ``IS [NOT] DISTINCT FROM`` between ``array``, ``map``, or ``row``
  columns, and joins on them. Servers before 482 fail an equality join on
  arrays and rows with a null element, because they order the values for the
  dynamic filter of the join. The connector cannot tell the condition of a join
  from a filter
- ``=``, ``IS [NOT] DISTINCT FROM``, and ``IN`` between ``real``,
  ``time with time zone``, or ``timestamp with time zone`` columns, and joins
  on them. Servers before 484 compare a list of 2 to 7 such values by their raw
  form, in which equal values can differ by the sign of zero or by the time
  zone. They do so for an ``IN`` list and for the dynamic filter of an equality
  join, so a join on a ``real`` column misses the rows whose zero has the other
  sign. ``<>`` and ordering comparisons stay in the basic list
- ``IN`` on ``uuid`` and ``ipaddress`` values inside an expression. The
  connector cannot send constants of these types in an expression, so the
  smoke test has no list to compare
- interval and JSON values. Servers before 484 have other interval types and
  normalize JSON text
- ``avg`` of decimals with precision above 18. Servers before 484 return a
  wrong average when the sum overflows 128 bits
- grouped aggregations with ``DISTINCT`` aggregates over different arguments.
  Servers before 484 can drop the group of NULL keys
- ``FULL JOIN``. Servers before 484 drop the unmatched row of an input with
  exactly one row when the join condition refers only to the other input

Two servers have the same version only when their version strings are equal. A
build of Trino 484 that reports ``484-patch1`` receives the basic list from a
local server that reports ``484``. A remote server newer than the local server
also receives the basic list. The smoke test of a connector release cannot
cover a remote release that did not exist yet, so upgrade the local cluster,
with the connector release for its version, before the remote cluster.

(trino-remote-server-settings)=
#### Remote server settings

A remote server of the local version also receives only the basic list when it
uses one of these settings, because it then evaluates the full list differently
from a server with default settings:

- ``deprecated.legacy-arithmetic-decimal-operators=true`` changes the precision
  and scale of decimal ``+``, ``-``, ``*``, and ``/``
- ``deprecated.regex-library=RE2J`` changes the results of regular expression
  functions
- ``deprecated.legacy-varchar-to-char-coercion=true``, or the session property
  ``legacy_varchar_to_char_coercion``, makes casts from ``char`` to ``varchar``
  keep the padding

The connector finds these settings with one query, together with the remote
version. It runs the query again with every
[check of the remote server](trino-remote-server-changes), in the remote
session of the query that it plans at that time, so a remote server must use
the same settings for every user of the catalog.

The connector cannot read the settings of the local server:

- With ``deprecated.legacy-arithmetic-decimal-operators=true`` on the local
  server, decimal ``+``, ``-``, ``*``, and ``/`` run locally when the two sets
  of rules give them different result types.
- With ``deprecated.regex-library=RE2J`` on the local server and the default
  library on a remote server of the same version, regular expression functions
  would return the results of the remote library. Set
  ``remote-delegation.enabled=false`` in the catalog.
- When both servers use the same setting, results are correct, but the remote
  server receives only the basic list.

The connector also changes how it sends some queries, to every remote version:

- Filter values for ``char`` columns are sent as ``CAST(? AS char(n))``.
  Servers from 482 compare a ``char`` column with a ``varchar`` value without
  padding, so a filter such as ``c < 'ab'`` would miss the value ``ab``
  followed by a tab.
- Filters with single ``number`` values run locally for servers before 484,
  which report ``number`` columns with older JDBC metadata. Servers before 482
  compare ``number`` values by their binary form, and a value rounded to the
  supported precision can keep trailing zeros. Range filters on ``number``
  columns stay on the remote server.
- Filters on ``real``, ``double``, and ``number`` columns that select all
  values but a few listed ones, such as ``x < 1 OR x > 1``, run locally. The
  SQL for such a filter is ``NOT (x = ?)``, which also selects NaN, and which
  servers before 482 evaluate by the binary form of a ``number`` value.

Bugs in how the remote server reads its own tables affect results with and
without pushdown.

The connector does not work around the bugs of a remote release. The remote
server evaluates what the connector pushes down, as it does for a query that a
user sends to it:

- Trino 473 returns duplicate groups for a ``GROUP BY`` or ``DISTINCT`` with
  more than about 31 million groups, which Trino 474 fixed. For a Trino 473
  remote server, set ``aggregation-pushdown.enabled=false`` in the catalog.
- Servers before 477 can return incorrect results after they unspill a hash
  aggregation. For such a remote server with ``spill-enabled=true``, set
  ``aggregation-pushdown.enabled=false`` in the catalog.
- Servers before 484 compare an ``IN`` list with 2 to 7 values by their raw
  internal values when every listed value fits in 32 bits. They then miss rows
  that are equal to a listed value but stored with another time zone, offset,
  or sign of zero. The connector sends such a list for a column filter with
  several values, and for the dynamic filter of a join that the local server
  evaluates. A join on a ``real`` column can then miss the rows whose zero has
  the other sign. This applies to ``timestamp(p) with time zone`` values with p
  of at most 3 within about 8 minutes 44 seconds of 1970-01-01 00:00:00 UTC,
  ``time(p) with time zone`` values with p of at most 9 less than about 1.05 ms
  after midnight, and ``real`` zero.

(trino-remote-server-changes)=
#### Changes of the remote server

The connector reads the remote version, remote server settings, function list,
and session time zone when it plans the first query of a catalog. Before it
plans a later query, it checks the remote server again when
the last check is older than ``remote-server.check-interval``, 10 seconds by
default. A check reads the remote version and runs the settings query. When
another server process answers, or the version or a setting differs, the
connector reads everything again. An upgrade, a downgrade, or a restart of the
remote server with other settings needs no restart of the local catalog.

- A check is one request for the version and one query of constants on the
  remote server. It runs about once per interval while the catalog plans
  queries. One check runs at a time, and the queries that are planned meanwhile
  wait for it.
- When the remote server takes longer than the interval to restart, every
  query that the connector plans after the restart uses what the restarted
  server returned. In a test, a Trino 484 server of the official container
  image without data took 20 seconds to restart.
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

### Statistics

``getTableStatistics()`` queries remote Trino with ``SHOW STATS FOR <table>`` and
feeds the result into the local optimizer. If the remote side cannot provide
statistics, the connector falls back to unknown statistics.

``AUTOMATIC`` join pushdown requires remote column statistics, including the
distinct-value count for join keys. If the remote catalog only reports a table
row count, the join remains local.

## Security

All remote SQL executes as the configured catalog-level credentials
(``connection-user`` / ``connection-password``).

Not supported:

- end-user identity propagation
- remote session property forwarding
- role delegation
- extra credential passthrough

Session-sensitive functions and casts such as ``current_timestamp``,
``current_date``, ``current_time``, current time zone functions,
locale-sensitive date formatting, and casts that depend on the session start
date are evaluated locally. Time-zone-dependent expressions such as
``from_iso8601_timestamp`` are delegated only when local and remote time zones
match. Expressions with explicit time zone operands, such as ``AT TIME ZONE
'Asia/Seoul'``, can be delegated when the rendered SQL is otherwise compatible.

The remote session uses the time zone of the JVM of the local node that opens
the connection, unless the connection URL sets ``timezone`` or the remote
server sets ``sql.forced-session-time-zone``. All nodes of the local cluster
must use the same JVM time zone, or the connection URL must set ``timezone``.

## Limitations

- Standard table access and metadata operations are read-only
- ``system.query`` accepts only top-level query statements; top-level writes
  (DDL, DML, ``CALL``) are rejected before remote execution, while functions
  and table functions remain governed by remote access control
- ``CALL system.execute(...)`` is inherited from the base JDBC framework but
  is denied by this connector's read-only access control
- A remote server that reports another version than the local server
  evaluates only the basic list. Functions, casts, and arithmetic then run
  locally, and so do the joins and aggregations above them
- Cross-cluster joins can only be improved with pushdown and statistics; the
  connector cannot remove the structural cost of federating between clusters
- Selected cross-version pairs are smoke-tested, but general cross-version
  compatibility is not guaranteed

## Testing scope

The test suite is centered on the generic contract exposed by remote Trino:

- type parsing and transport fallback
- native and transported complex type reads
- passthrough
- pushdown
- statistics
- federation behavior

The default ``Build and Test`` CI workflow also runs a Docker-based remote Delta
smoke test for the production shape where a small federated Trino 484 cluster
queries a separate Trino 484 cluster with a Delta Lake catalog. It reuses the
``target/trino-trino-484.zip`` package produced by ``mvn -B clean verify``,
unpacking it before starting the containers. It is documented in
``docs/remote-delta-smoke.md``.

The separate ``testing/remote-version-smoke/run.sh`` probe covers selected
different remote Trino versions. It checks native geospatial reads only when
both Trino versions are at least 481; other cross-version checks do not imply
native geospatial compatibility.
