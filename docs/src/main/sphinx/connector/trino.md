# Trino connector

The Trino connector queries a remote Trino cluster through JDBC and exposes it
as a local read-only catalog.

## Requirements

- Remote Trino must be reachable through the Trino JDBC driver
- This connector is currently tested against Trino 477 querying remote Trino 477

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

The connector uses five transport modes:

- **Native**: JDBC preserves the type exactly
- **VARCHAR transport**: unsupported scalar temporal or interval values are projected as ``VARCHAR`` and decoded back to the original logical type
- **VARBINARY transport**: top-level sketch types are projected as ``VARBINARY`` and decoded back to the original logical type
- **JSON transport**: unsupported structural columns are recursively rewritten, projected as JSON text, and decoded back to the original logical type
- **Unsupported**: only types that still cannot be represented safely

### Native reads

Native scalar reads include:

- ``boolean``
- ``tinyint``, ``smallint``, ``integer``, ``bigint``
- ``real``, ``double``, ``decimal(p,s)``
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
- ``interval year to month``
- ``interval day to second``
- high-precision ``time(p)`` and ``timestamp(p)`` when JDBC is not exact

These columns still appear locally as their original logical Trino types.

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
- same-remote equality join pushdown for supported join shapes; ``IS NOT
  DISTINCT FROM`` and inequality joins stay local on Trino 477

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
  ``INTERVAL '0.001' SECOND * CAST(? AS BIGINT)``
- structural ``JSON`` transport remains ``DISABLE_PUSHDOWN``
- sketch ``VARBINARY`` transport remains ``DISABLE_PUSHDOWN``

This keeps remote filtering available where the connector can still bind the
original logical type safely, while avoiding pushdown on carrier-only
transports whose semantics would otherwise drift from the original remote
column.

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

## Limitations

- Standard table access and metadata operations are read-only
- ``system.query`` accepts only top-level query statements; top-level writes
  (DDL, DML, ``CALL``) are rejected before remote execution, while functions
  and table functions remain governed by remote access control
- ``CALL system.execute(...)`` is inherited from the base JDBC framework but
  is denied by this connector's read-only access control
- Remote delegation probes ``CHAR`` to ``VARCHAR`` cast semantics and delegates
  these casts only when the remote retains the padding expected by Trino 477
- Cross-cluster joins can only be improved with pushdown and statistics; the
  connector cannot remove the structural cost of federating between clusters
- Cross-version compatibility is not yet claimed

## Testing scope

The test suite is centered on the generic contract exposed by remote Trino:

- type parsing and transport fallback
- native and transported complex type reads
- passthrough
- pushdown
- statistics
- federation behavior

The default ``Build and Test`` CI workflow also runs a Docker-based remote Delta
smoke test for the production shape where a small federated Trino 477 cluster
queries a separate Trino 477 cluster with a Delta Lake catalog. It reuses the
``target/trino-trino-477`` package produced by ``mvn -B clean verify`` and is
documented in ``docs/remote-delta-smoke.md``.
