# Trino connector

The Trino connector queries a remote Trino cluster through JDBC and exposes it
as a local read-only catalog.

## Requirements

- Remote Trino must be reachable through the Trino JDBC driver
- This connector is currently tested against Trino 479 querying remote Trino 479

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
- exact ``time(p)``, ``timestamp(p)``, ``timestamp(p) with time zone``
- ``uuid``, ``json``, ``ipaddress``

Native complex reads are allowed only when every descendant leaf is natively
readable.

Examples:

- ``array(varchar)``
- ``map(varchar, bigint)``
- ``row(id uuid, payload json)``
- ``row(events array(map(varchar, varchar)))``

### VARCHAR transport

Top-level scalar fallback currently covers:

- ``time with time zone``
- ``interval year to month``
- ``interval day to second``
- high-precision ``time(p)``, ``timestamp(p)``, and ``timestamp(p) with time zone`` when JDBC is not exact

These columns still appear locally as their original logical Trino types.

### JSON transport

If a structural column contains a descendant that cannot be read natively, the
connector rewrites the whole column through JSON transport and reconstructs the
original logical type locally.

Examples:

- ``array(timestamp(12))``
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

Execute manual row-returning read SQL on the remote Trino:

```sql
SELECT *
FROM TABLE(
    remote.system.query(
        query => 'SELECT * FROM information_schema.tables LIMIT 10'
    )
)
```

``system.query`` is an explicit bypass for the normal connector planning path:

- the inner SQL string is sent to remote Trino as written
- the connector still infers output columns and applies the normal type
  mapping and transport rules to the result
- explicit table references must stay within the configured remote catalog

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
```

## Performance

### Pushdown

The connector supports:

- predicate pushdown
- projection pushdown
- ``LIMIT`` pushdown
- ``ORDER BY ... LIMIT`` pushdown
- aggregation pushdown
- same-remote join pushdown for supported join shapes

Pushdown behavior for transport-backed columns is split:

- scalar ``VARCHAR`` transport (``timestamp(p>9)``,
  ``timestamp with time zone(p>9)``, ``time with time zone``, interval types)
  keeps tuple-domain predicate pushdown enabled via typed bind expressions such
  as ``CAST(? AS timestamp(12))`` or ``parse_duration(?)``
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

## Security

All remote SQL executes as the configured catalog-level credentials
(``connection-user`` / ``connection-password``).

Not supported:

- end-user identity propagation
- remote session property forwarding
- role delegation
- extra credential passthrough

## Limitations

- Standard table access and metadata operations are read-only
- ``system.query`` is documented only for row-returning read queries
- Negative dates (before year 0001) are not preserved correctly through JDBC
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
