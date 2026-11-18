## Changes since trino-483-r2

### Pushdown now depends on the remote version

- A remote server receives functions, casts, arithmetic, `LIKE`, and `NULLIF`
  only when it reports the version of the local server and uses default
  settings. Every other remote server receives the basic list: column filters,
  comparisons, `AND`, `OR`, `NOT`, `IS NULL`, `IN`, the common aggregations,
  `GROUP BY`, `ORDER BY ... LIMIT`, and joins on those comparisons. The local
  server evaluates the rest. The previous release sent these expressions to
  every remote version, and their results differ between Trino releases.
- With a local Trino 484 and a remote Trino 483, a query such as
  `SELECT count(*) FROM t WHERE date(ts) = DATE '2026-01-01'` now reads the
  rows of `t` and filters and counts them on the local server. Run the same
  Trino version on both clusters to keep the full pushdown, and upgrade the
  local cluster first.
- A remote server of the local version also receives only the basic list when
  it sets `deprecated.legacy-arithmetic-decimal-operators`,
  `deprecated.regex-library=RE2J`, or
  `deprecated.legacy-varchar-to-char-coercion`.
- The connector checks the remote server again while it plans queries, at the
  new catalog property `remote-server.check-interval`, 10 seconds by default.
  An upgrade, a downgrade, or a restart of the remote server with other
  settings needs no restart of the local catalog.
- These operations run locally for every remote server: comparisons,
  `GROUP BY`, and `DISTINCT` on JSON values, and filters on `real`, `double`,
  and `number` columns that select all values but a few listed ones, such as
  `x < 1 OR x > 1`. A `FULL JOIN` is pushed down only to a remote server of the
  local version.

The README sections "Remote Trino versions", "Remote server settings", and
"Changes of the remote server" list the details.

### Fixes of incorrect results

- A filter on a `char` column, such as `c < 'ab'`, missed values with a
  character below a space, such as `ab` followed by a tab, on remote servers
  from Trino 482.
- A filter such as `x < 1 OR x > 1` on a `real` or `double` column returned the
  rows with NaN.

### Trino 484

- The connector reads the parametric interval types and the JSON type of
  Trino 484. JSON columns keep the text that the remote server sends, so values
  that failed to parse before, such as numbers beyond the `double` range, no
  longer fail the query.
