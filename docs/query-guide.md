# Query guide

How to write a query so that a remote server of another version evaluates its
filters, aggregations, and joins.

In short:

- Compare a column with a constant. Keep functions, casts, and arithmetic off
  the column.
- One filter that stays local keeps the aggregation, join, and `LIMIT` above it
  local.
- Aggregate and group by plain columns. Apply functions to the result of an
  aggregate.
- Check the query with `EXPLAIN`.
- Use `system.query` for what cannot be written that way.

## When this guide applies

A remote server of the local version with default settings receives the full
list, and a query needs no change. Every other remote server receives only the
[basic list](../README.md#remote-trino-versions):

- a server that reports another version string, including a patch build and a
  newer release
- a server that uses one of the
  [evaluation settings](../README.md#remote-server-settings)
- a server whose check failed

The first case also holds while one of two clusters is upgraded before the
other. The log of the local coordinator names the case, for example:

```text
Remote Trino version 483 is not the local version 484, so functions, casts, and arithmetic run locally
```

The forms in this guide use only the basic list, which the connector sends to
every remote server.

The examples were run with a local Trino 484 and a remote Trino 483, both with
default settings. They use the local catalog `remote` and a remote table
`logs`:

| Column | Type | Content |
|---|---|---|
| `log_date` | `varchar` | The day, as `yyyy-MM-dd` |
| `event_time` | `bigint` | Seconds since the epoch |
| `created_at` | `timestamp(3)` | |
| `status`, `clicks` | `integer` | |
| `user_id`, `bytes` | `bigint` | |
| `score` | `double` | |
| `name` | `varchar` | |

## Why the form of a filter matters

The connector reads the rows of each remote table scan through one JDBC
connection. Every row that the remote server does not filter out or aggregate
comes through that connection.

This query casts the column, and casts are not in the basic list:

```sql
SELECT count(*) FROM logs WHERE CAST(log_date AS date) >= DATE '2026-11-18'
```

```text
Aggregate[type = PARTIAL]
└─ ScanFilterProject[table = remote:default.logs memory.default.logs constraint on [log_date] columns=[log_date:varchar:varchar], filterPredicate = (date '2026-11-18' <= CAST(log_date AS date))]
```

The local server evaluates the filter and counts the rows. This query returns
the same count and compares the column with a constant:

```sql
SELECT count(*) FROM logs WHERE log_date >= '2026-11-18'
```

```text
TableScan[table = remote:Query[SELECT count(*) AS "_pfgnrtd_0" FROM "memory"."default"."logs" WHERE "log_date" >= ?] columns=[_pfgnrtd_0:bigint:bigint]]
```

The remote server evaluates the filter and the count, and returns one row.

## Check a query with EXPLAIN

The plans in this guide are excerpts of `EXPLAIN` output. Read the line of the
table scan and the lines above it:

| In the plan | Meaning |
|---|---|
| `remote:Query[SELECT ...]` | The remote server runs this SQL. A `WHERE`, `GROUP BY`, or `JOIN` inside it was sent |
| `remote:default.logs ... constraint on [status]` | The remote server returns rows of the table. It received filters on the listed columns |
| `constraints=[... "log_date" ... OR "name" ...]` after `constraint on [...]` | The remote server also received this filter expression. Its columns are not in the list of `constraint on` |
| `filterPredicate = ...` | The local server evaluates this filter |
| `Aggregate`, `InnerJoin`, `TopN`, or `Limit` above the scan | The local server runs this step |
| `sortOrder=[...] limit=5` | The remote server sorts and limits the rows |

To see the SQL that the remote server received, read `system.runtime.queries`
on the remote server. The statements appear as
`PREPARE statement1 FROM SELECT ...`.

## Filters

Compare a column with a constant.

| Stays local | Reaches the remote server |
|---|---|
| `CAST(log_date AS date) >= DATE '2026-11-18'` | `log_date >= '2026-11-18'` |
| `from_unixtime(event_time) >= TIMESTAMP '2026-11-18 00:00:00 UTC'` | `event_time >= to_unixtime(TIMESTAMP '2026-11-18 00:00:00 UTC')` |
| `substr(log_date, 1, 7) = '2026-11'` | `log_date >= '2026-11-01' AND log_date < '2026-12-01'` |
| `status + 1 = 2` | `status = 2 - 1` |
| `clicks = user_id`, an `integer` and a `bigint` column | No other form. The engine casts one of two columns of different types |

A comparison of strings returns the result of the comparison of dates only when
every value of the column has the same format.

`status = 2 - 1` selects the rows of `status + 1 = 2` when no `status + 1`
overflows. With a row that holds the largest `integer` value, `status + 1 = 2`
fails and `status = 2 - 1` does not.

The constant can be an expression. The engine evaluates it before it plans the
query, so these filters reach the remote server:

```sql
WHERE log_date >= CAST(current_date - INTERVAL '7' DAY AS varchar)
WHERE log_date >= date_format(current_timestamp - INTERVAL '7' DAY, '%Y-%m-%d')
```

The engine derives only a coarse filter from a cast of a `varchar` column to
`date`. For `CAST(log_date AS date) >= DATE '2026-11-23'`, the remote server
received `"log_date" < ? OR "log_date" > ?` with the values `'1'` and `'2025'`,
which every value of the year 2026 passes.

The engine rewrites some filters on a `timestamp` column to a range of the
column, and the range reaches the remote server:

```sql
WHERE CAST(created_at AS date) = DATE '2026-11-18'
WHERE date(created_at) >= DATE '2026-11-18'
WHERE date_trunc('day', created_at) = TIMESTAMP '2026-11-18 00:00:00'
WHERE year(created_at) = 2026
```

Conditions that are joined by `AND` are sent one by one. A function in one
condition of an `OR` keeps the whole `OR` local:

| Filter | The remote server receives |
|---|---|
| `log_date >= '2026-11-18' AND lower(name) = 'user1'` | `"log_date" >= ?` |
| `log_date >= '2026-11-18' OR lower(name) = 'user1'` | No filter |
| `name = 'User1' OR status = 2` | The whole filter |
| `log_date LIKE '2026-11-2%'` | `"log_date" >= ? AND "log_date" < ?`, the range of the prefix. The local server evaluates the `LIKE` |

`IN`, `BETWEEN`, `<>`, `IS NULL`, and `NOT` over these comparisons reach the
remote server. A filter on a column whose type is not in the
[basic list](../README.md#remote-trino-versions), such as `json`, stays local.

## Aggregations

The remote server runs an aggregation when every filter of the query reached
it, and the aggregates and the `GROUP BY` use plain columns:

```sql
SELECT log_date, count(*), sum(bytes), min(score), max(score), avg(score)
FROM logs
WHERE log_date >= '2026-11-18'
GROUP BY log_date
```

```text
TableScan[table = remote:Query[SELECT "log_date", count(*) AS "_pfgnrtd_0", sum("bytes") AS "_pfgnrtd_1", min("score") AS "_pfgnrtd_2", max("score") AS "_pfgnrtd_3", avg("score") AS "_pfgnrtd_4" FROM "memory"."default"."logs" WHERE "log_date" >= ? GROUP BY "log_date"] ...]
```

These aggregations run on the local server, which reads every row that passes
the filters:

| Runs locally | Reason | Write instead |
|---|---|---|
| `sum(bytes * 2)` | Arithmetic in the argument | `sum(bytes) * 2` |
| `count_if(status = 1)` | An expression in the argument. `count_if` of a `boolean` column is sent | `count(*)` with `WHERE status = 1`, in a query without `GROUP BY` and without another aggregate |
| `GROUP BY date_trunc('day', created_at)` | A function in the grouping key | `GROUP BY` a column that holds the value, such as `log_date` |
| `sum(clicks)` and `avg(clicks)` of a `tinyint`, `smallint`, or `integer` column | The engine casts the values to `bigint`. `min`, `max`, and `count` of such a column are sent | `system.query` |
| `count(DISTINCT user_id), count(DISTINCT name)` with `GROUP BY` | DISTINCT aggregates over different arguments in a grouped aggregation. One DISTINCT aggregate is sent | One query for each |

Two of these forms return the result of the original only under a condition:

- `sum(bytes) * 2` returns the value of `sum(bytes * 2)` when no `bytes * 2`
  overflows. When one overflows, `sum(bytes * 2)` fails and `sum(bytes) * 2`
  can return a value.
- With `GROUP BY`, `count_if(status = 1)` returns 0 for a group without such a
  row, and `WHERE status = 1` removes the group.
  `count(*) FILTER (WHERE status = 1)` keeps the group and runs locally.

An aggregate function that the connector does not push down for any remote
server, such as `approx_distinct`, also runs locally.

A function on the result of an aggregate does not change what the remote
server runs. The remote server runs the aggregation of these queries, and the
local server applies the functions to the rows of the result:

```sql
SELECT log_date, sum(bytes) / 1024, round(avg(score), 2)
FROM logs WHERE log_date >= '2026-11-18' GROUP BY log_date

SELECT CAST(log_date AS date), count(*)
FROM logs WHERE log_date >= '2026-11-18' GROUP BY log_date
```

The remote server also evaluates a `HAVING` on an aggregate. For
`SELECT log_date, count(*) FROM logs GROUP BY log_date HAVING count(*) > 3`, it
received:

```sql
SELECT "log_date", "_pfgnrtd_0"
FROM (SELECT "log_date", count(*) AS "_pfgnrtd_0" FROM "memory"."default"."logs" GROUP BY "log_date") o
WHERE "_pfgnrtd_0" > ?
```

A function in the select list of a query without aggregation does not keep a
filter local either. The remote server evaluates the filter of
`SELECT lower(name) FROM logs WHERE log_date >= '2026-11-18'`.

## Joins

Join pushdown is disabled by default, as the
[README](../README.md#pushdown--statistics) describes. The local server then
joins two remote tables. The filters of each table still reach the remote
server. The dynamic filter of the join can reach it too, which depends on the
type of the join and on the number of rows of the other table. In one run of an
inner join with two matching users, the remote server received
`SELECT "user_id" FROM "memory"."default"."logs" WHERE "user_id" IN (?,?)`. The
plan of a left join with `logs` on the left had no dynamic filter.

With `join-pushdown.enabled=true`, the remote server can run an inner, left, or
right join on columns of the same type, and the aggregation above it. The
default strategy, `AUTOMATIC`, also needs column statistics of the remote
tables. The examples used `join-pushdown.strategy=EAGER`. These joins stay
local for a remote server of another version:

- `FULL JOIN`
- a join on `real` columns, or on columns with a time zone
- a join on columns of different types, such as `integer` and `bigint`
- a join above a filter that stays local

## ORDER BY ... LIMIT and LIMIT

The remote server sorts and limits the rows when every filter reached it and
the sort key is a column of a scalar type of the basic list. An `ORDER BY` on
an `array` or `row` column stays local. The local server sorts the returned
rows again:

```sql
SELECT user_id, bytes FROM logs WHERE log_date >= '2026-11-18' ORDER BY bytes DESC LIMIT 5
```

```text
TopN[count = 5, orderBy = [bytes DESC NULLS LAST]]
└─ TableScan[table = remote:default.logs memory.default.logs constraint on [log_date] sortOrder=[bytes:bigint:bigint DESC NULLS LAST] limit=5 columns=[user_id:bigint:bigint, bytes:bigint:bigint]]
```

Above a filter that stays local, the local server applies the
`ORDER BY ... LIMIT` or the `LIMIT` itself. For an `ORDER BY ... LIMIT` it reads
every row that the remote server returns. For a `LIMIT` without `ORDER BY` it
stops when it has enough rows. With the filter `lower(orderstatus) = 'o'` on
the 1,500,000 rows of `tpch.sf1.orders`, a `LIMIT 1` read 69,906 rows, and an
`ORDER BY orderkey LIMIT 1` read all of them.

## system.query

`system.query` sends SQL to the remote server as it is written. Use it for a
part of a query that has no form in the basic list:

- a filter with a function on the column, such as `lower(name) = 'user1'` or
  `regexp_like`
- an aggregation with a function in an argument or in the grouping key, or a
  `sum` of an `integer` column

```sql
SELECT *
FROM TABLE(
    remote.system.query(
        query => 'SELECT date_trunc(''day'', created_at) AS day, count(*) AS cnt
                  FROM memory.default.logs
                  WHERE lower(name) = ''user1'' AND log_date >= ''2026-11-22''
                  GROUP BY 1'))
ORDER BY day
```

The result is a table. Join it with a local table as any other:

```sql
SELECT n.name, q.cnt
FROM TABLE(
    remote.system.query(
        query => 'SELECT status, count(*) AS cnt
                  FROM memory.default.logs
                  WHERE lower(name) = ''user1''
                  GROUP BY status')) q
JOIN tpch.tiny.nation n ON n.nationkey = q.status
```

Do not use `system.query` for an operation that the basic list leaves out
because a remote release returns a wrong result. `system.query` returns that
result too. The [README](../README.md#remote-trino-versions) lists these
operations with their releases. For a remote server before Trino 484 they
include a grouped aggregation with DISTINCT aggregates over different arguments
and a `FULL JOIN`. Leave them to the local server, which the connector does by
default.

What differs from a query on the table:

- The remote server evaluates the SQL by the rules of its version. The result
  is what a user gets who queries the remote server directly, and it can differ
  from the result of the local version for the operations that the
  [README](../README.md#remote-trino-versions) lists.
- Table names are the names on the remote server. A name without a catalog
  resolves in the catalog of the connection URL.
- An error of the remote server fails the query, for example
  `Function 'no_such_function' not registered`. The local server does not
  evaluate the SQL instead.
- An `ORDER BY` inside the SQL does not order the result. Order the rows in the
  outer query.
- Only query statements are accepted, and the access control of the remote
  server decides what the SQL can read. See
  [Passthrough](../README.md#passthrough).
