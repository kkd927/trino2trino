# Remote version smoke

This is a release-time diagnostic smoke test for Trino-to-Trino federation
against a remote Trino version that may differ from the local plugin build.
It is not a compatibility guarantee and is intentionally narrower than the
remote Delta smoke test.

The probe starts two Trino containers:

- local Trino at the version from `pom.xml`, with the locally built
  `target/trino-trino-<version>.zip` unpacked and mounted alongside geospatial
- remote Trino at the version passed to `run.sh`, exposing `tpch` and `memory`
  catalogs and loading those connector plugins alongside geospatial

Each container is limited to 2048 MiB of memory and two CPUs by default, and
runs Trino with a 1 GiB heap. The image would size the heap to 80% of the
container memory, and after the smoke's queries the memory outside the heap
grows to about 550 MiB, so the container would be killed. The limits can be
adjusted with `REMOTE_VERSION_SMOKE_MEMORY_LIMIT`, `REMOTE_VERSION_SMOKE_HEAP`,
and `REMOTE_VERSION_SMOKE_CPU_LIMIT`. Readiness polling uses the lightweight
`/v1/info` endpoint instead of starting a CLI query for every attempt, and
related assertions are batched to avoid repeatedly starting the Trino CLI JVM.

The connector sends functions, casts, and arithmetic only to a remote server
that reports the version of the local server and evaluates them with default
settings. Every other remote server receives the basic list: comparisons, `AND`,
`OR`, `NOT`, `IS NULL`, `IN`, joins, the basic aggregates, and
`ORDER BY ... LIMIT`.

`run.sh <local version>-legacy-settings`, such as `run.sh 484-legacy-settings`,
starts the remote server of the local version with
`conf/legacy-settings/config.properties`. With those settings it evaluates
decimal arithmetic, regular expressions, and casts from `char` to `varchar` as
an earlier release does, so the connector must send it only the basic list. The
smoke then expects what it expects for a remote server of another version. The
release workflow runs it.

`basic-pushdown-types.txt` lists the types of the basic list with sample values.
For each type, the smoke creates a table that holds every pair of the values and
NULL, on the remote server and in the memory catalog of the local server. It
runs filters between columns, filters that compare one column with constants,
aggregations with and without `GROUP BY` and `DISTINCT`, `ORDER BY ... LIMIT`,
and inner, left, and right joins through the connector, with delegation
enabled and disabled, and on the local copy. All must return the same rows, and
the plan of every statement must show that the remote server evaluates it.
`TestTrinoCompatibilityRegistry` requires the type list to match the basic list
of `TrinoCompatibilityRegistry`, so a type enters the basic list only together
with these statements. These assertions run for every remote version.

The types file marks the types that have no `=` in the basic list. An equality
join makes the remote server build a dynamic filter from the values. Servers
before 484 compare a list of 2 to 7 `real` values or values with a time zone by
their raw form, and servers before 482 fail to order arrays and rows with a
null element. The smoke runs an equality join on `real` values with zeros of
both signs, which a remote server before 484 answers with 245 rows instead of
343. The join must stay local for every remote version but the local one. The
local join sends its dynamic filter to the remote server as a list of values,
which a server before 484 compares in the same way. The connector does not work
around bugs of a remote release, so the smoke compares the rows of this join
only for a remote server of the local release.

For local Trino 484 and newer, the smoke also checks interval equality, ranges,
arithmetic, negative and NULL values, and leading precision beyond two digits.
It checks submillisecond timestamp arithmetic with delegation both enabled and
disabled, plus timestamp/time differences, ordering, and aggregation under both
unsupported-type policies. It also runs one query for each behavior that changed
in a release up to 484 (JSON, `date_diff`, `to_unixtime`, `from_unixtime`,
`round`, decimal casts and arithmetic, `char` trim, `IN`, and decimal `avg`),
with delegation both enabled and disabled, and requires the local 484 result in
both cases. It checks that JSON and decimal division are delegated only to a
remote server of the local version. These assertions also run for a
same-version remote:
``testing/remote-version-smoke/run.sh 484``. The build workflow runs the smoke
against the same release and the previous one.

It also runs queries that the basic list leaves out, with delegation both
enabled and disabled, and requires the local 484 result:

- casts to a shorter `char`, casts from `timestamp` to a lower precision, and
  casts to `varchar(n)` and `date_add` calls that must fail
- a grouped aggregation with two `DISTINCT` aggregates and a NULL group, which
  must run locally for every remote version but the local one
- a join on a `char` to `varchar` cast with EAGER join pushdown
- TopN, `max`, and an ordering comparison on arrays with null elements, compared
  with local evaluation
- `DISTINCT`, `GROUP BY`, equality, inequality, and a join without complex join
  pushdown on `number` values with NaN and rounded values, for remote versions
  from 480
- top-level and nested JSON values that remote versions before 484 accepted but
  the 484 parser rejects
- filters on `real`, `double`, and `number` columns that select all values but
  a few listed ones, on rows with NaN. The SQL for such a filter is
  `NOT (x = ?)`, which NaN passes, so it must stay local for every remote
  version
- a `FULL JOIN` of two passthrough queries with one row each, which a remote
  server before 484 answers with one row instead of two. It must stay local for
  every remote version but the local one
- for a remote server of the local version, `DISTINCT`, `GROUP BY`, `=`, and
  `checksum` on JSON values that hold a date. The remote server sends the date
  as a JSON string, so these must run locally, while `json_extract_scalar` must
  stay on the remote server

It checks that a filter with a function is delegated only to a remote server of
the local version, and that a comparison between columns is delegated to every
remote version.

Finally, it evaluates every expression in `delegation-expressions.txt` through
the connector, with delegation enabled and disabled, and on a copy of the table
in the memory catalog of the local server. All three must return the same rows.
An expression that fails locally must fail through the connector too. A remote
server of the local version evaluates these expressions, so the same-version run
compares its results with local evaluation. For other remote versions the run
checks that the local server evaluates them on unchanged values. Add an
expression there for each behavior change that a later release brings to
delegated functions, casts, or operators. The list includes the differences
found in releases before 483, such as casts from `varchar` with surrounding
spaces to integer types and decimal multiplication,
`timestamp(p) with time zone` values beyond the range that Trino 484 accepts,
and edge cases of the cast parsers that Trino 483 rewrote. A few filters run
the same way, because a filter reaches the remote optimizer: Trino 482 drops
the negation of a negated `NULLIF` and times out on a cast compared with
`date_trunc`, and Trino 482 and 483 fail on a cast from `varchar` to `char`
compared with a constant. A filter with a function and a join on the result of
a passthrough query run the same way too, and the function must stay local for
every remote version but the local one.

At the end, another remote server takes the host name while the local server
keeps running. The connector checks the remote server at an interval, 10
seconds by default, so the run waits for the interval after the new server
answers. A filter with a regular expression, a filter with `json_parse`, and a
decimal division must then return the rows of local evaluation, and the plan
must show that functions run on the remote server only when it has the local
version and default settings:

- A run against the local version restarts the remote server with the legacy
  settings.
- A run with `-legacy-settings` restarts the remote server with default
  settings.
- A run against another version replaces the remote server with a server of the
  local version, and then with a server of the other version again.

Every version pair checks user-written remote geospatial SQL returning text
through `system.query`. This does not test native geospatial column reads.
Native `Geometry` and `SphericalGeography` reads, including a 2D EWKB value
with SRID and a nested array with NULL, are checked only when both Trino
versions are at least 481. Remote versions before 481 do not provide
`ST_AsEWKB`, so native geospatial reads from them are outside the supported
scope. For the current Trino 484 plugin, release verification checks native reads against
remote 483; Z preservation is covered by the 484-to-484 integration
tests. This does not validate plugin releases built for local Trino 481 or 482.
The release version selector includes a compatible older remote version when
one is available, in addition to its broad version samples. It also includes
Trino 482, the only release with the `NULLIF` and `date_trunc` behavior above.

Run after building the plugin:

```bash
mvn -B clean verify
testing/remote-version-smoke/run.sh <remote-version>
```

Multiple remote versions can be checked sequentially:

```bash
testing/remote-version-smoke/run.sh <remote-version> <another-remote-version>
```

Diagnostics are written under
`target/remote-version-smoke/<local-version>-to-<remote-version>/` on failure, or
on success when `REMOTE_VERSION_SMOKE_ALWAYS_LOGS=true` is set.
