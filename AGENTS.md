# Agent instructions

This file provides guidance to coding agents, such as Claude Code and Codex, when working with code in this repository.

## Project

trino2trino is a read-only Trino connector plugin (`connector.name=trino`) that
exposes one catalog of a remote Trino cluster as a local catalog over JDBC. Each
source line builds against one Trino version, the `trino-root` parent version
in `pom.xml`. Older Trino versions live on `release/trino-N` branches.

Branch names are `feature/`, `fix/`, or `chore/` plus a kebab-case description,
never `codex/` (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## Development direction

trino2trino is an independent open-source project. It is also proposed to
trinodb/trino as a built-in connector in trinodb/trino#30290 (roadmap issue
trinodb/trino#21791). Take the direction of the Trino community as the default
for design decisions. The project values of Trino are at
https://trino.io/development/vision and its contribution process at
https://trino.io/development/process. Where this project needs something of
its own, such as its releases per Trino version or the Docker smoke tests, keep
it and say that it is specific to this repository. A requirement that only an
upstream review can settle waits for that review.

The supported configuration is a remote server of the local version with
default settings. A remote server of another version receives the basic list
as a best effort.

- **Open-source criteria.** The users and their data are unknown. Decide with a
  rule that gives the same answer for every user, not by how likely a case is
  in one deployment.
- **Correct results first.** In a supported configuration, a query must return
  what Trino returns without pushdown. When the connector cannot show that for
  an operation, the operation stays local.
- **Secure.** A change to credentials, connection URL parameters,
  `system.query`, or remote error messages changes what a local user can reach
  on the remote cluster. Treat it as a security decision: state its effect in
  the connector reference and ask the user before making it.
- **State what is supported, and write code for that.** Outside the supported
  range, a simple fallback to local evaluation is acceptable. Otherwise
  document the behavior instead of adding code. A narrower documented scope is
  better than more code.
- **Fix a problem where it belongs.** A problem of the engine or of
  `trino-base-jdbc` belongs upstream. Keep a workaround for it here only while
  a supported configuration returns wrong results without it, and say what it
  works around. `ComplementFilterPushdown` is one: the base JDBC query builder
  renders the filter that NaN passes. A bug of a remote Trino release is a bug
  of the data source, which returns the same result to a user who queries it
  directly. Add no workaround for it: fall back to local evaluation or document
  it, as for other behavior outside the supported range.
- **Do what `trino-base-jdbc` and the other JDBC connectors do.** Before adding
  a mechanism, a configuration property, or a test pattern, check whether
  `trino-base-jdbc` or another JDBC connector already has one, such as
  `complex-expression-pushdown.enabled`. When a new Trino version changes them,
  take the same direction. Differ only for a reason that is specific to a Trino
  data source, and state it.
- **Keep it small.** Maintainers review and maintain every line. Add no
  feature, configuration property, fallback, or policy without a present need.
  Removing code is a valid result of a review.
- **Follow the Trino conventions.** `.github/DEVELOPMENT.md` in trinodb/trino
  covers code style, configuration and session properties, and `pom.xml`.
  Tests build on the base JDBC test classes, such as `BaseJdbcConnectorTest`,
  and run in the Maven build. The Docker smoke scripts under `testing/` are
  this repository's release check and do not exist in trinodb/trino. When a
  behavior is covered only by them, say so.

The rules for remote-version behavior below apply this direction to remote
servers of another version.

## Commits and pull requests

From https://trino.io/development/process#commit:

- One logical change per pull request. Every commit builds and passes tests.
- Mechanical changes, such as refactoring, renames, and static imports, go in
  their own commits, separate from changes of behavior.
- Subject line in the imperative mood, capitalized, at most 50 characters,
  without a period. Body wrapped at 72 characters, explaining what and why.
- Address review comments with `fixup!` commits placed after the commit they
  change.
- Add no AI attribution, such as `Co-Authored-By:` or `Assisted-by:` trailers,
  to commit messages or pull request descriptions.

The message format, the rule against AI attribution, and the rule that every
commit builds and passes tests apply to every commit in this repository. The
rules on one logical change per pull request, separate mechanical commits, and
`fixup!` commits apply to the branch of the upstream pull request.

## Build and test

Requires JDK 25 and Maven 3.9+, plus Docker for the smoke tests. Run Maven from
the repository root: `TestTrinoCompatibilityRegistry` reads
`testing/remote-version-smoke/basic-pushdown-types.txt` by relative path.

```bash
# Package without tests or checks
mvn clean verify -DskipTests -Dair.check.skip-all=true

# Full build: tests plus airstyle, checkstyle, modernizer, and the other Trino checks.
# Run this before submitting a change.
mvn -B clean verify

# Fix formatting drift that airstyle:check reports
mvn airstyle:format

# Local edit/test loop: skips checks, 2 GB test heap (override with TRINO_FAST_TEST_JVM_SIZE)
testing/fast-test.sh -Dtest=TestTrinoConnectorTest
testing/fast-test.sh -Dtest='TestTrinoCompatibilityRegistry#methodName'
```

The geospatial tests download `trino-geospatial-<version>.zip` from the Trino
GitHub release into `target/`. For offline runs, set
`TRINO_GEOSPATIAL_PLUGIN_ZIP` to a local copy.

### Docker smoke tests

Both need `target/trino-trino-<version>.zip` from `mvn -B clean verify`. CI
runs both after the build.

```bash
testing/remote-delta-smoke/run.sh                                 # same-version remote backed by Delta Lake
testing/remote-version-smoke/run.sh <version> <previous-version>  # one run per remote version
testing/remote-version-smoke/run.sh <version>-legacy-settings     # local-version remote with evaluation-changing settings
```

Diagnostics go to `target/remote-delta-smoke/` and
`target/remote-version-smoke/<local>-to-<remote>/` on failure, or on every run
with `REMOTE_VERSION_SMOKE_ALWAYS_LOGS=true`.
`testing/remote-version-smoke/README.md` lists what the version smoke asserts.

## Architecture

All code is in `io.trino.plugin.trino`. `TrinoClient`, a `BaseJdbcClient`, is
where the connector departs from base JDBC behavior.

**Transport.** For column types that JDBC cannot read exactly,
`TrinoTypeClassifier.transportKind` picks `VARCHAR_CAST`, `VARBINARY_CAST`,
`GEOSPATIAL_EWKB`, or `JSON_CAST`. When any column is not `NATIVE`,
`TrinoClient.applyTransportProjection` wraps the remote query as
`SELECT <cast per column> FROM (<query>) "_trino_transport"`, and codecs decode
the values. Because of that wrap, `isTopNGuaranteed` returns false.

**Two expression layers.** `TrinoRemoteSqlRenderer`, called through
`TrinoDelegationAnalyzer` and off with `remote-delegation.enabled=false`,
renders Trino-native SQL. The base JDBC `ConnectorExpressionRewriter` and
`AggregateFunctionRewriter` define behavior when delegation is off, and catch
predicates and aggregations that the renderer does not render. Every pushdown
entry point of `TrinoClient` checks `TrinoCompatibilityRegistry` before either
layer runs.

**Remote version gating.** `TrinoRemoteCapabilitiesCache` reads the remote
version, runs `EVALUATION_PROBE`, and checks again after
`remote-server.check-interval`. `TrinoCompatibilityRegistry` allows the **full
list** (functions, casts, arithmetic, and the rest) only when the remote
version string equals the local one and the probe shows default settings.
Every other server, including one whose check failed, receives the **basic
list**: comparisons, `AND`/`OR`/`NOT`, `IS NULL`, `IN`, joins,
`count`/`count_if`/`checksum`/`min`/`max`/`sum`/`avg`, and TopN, on the types
in `basic-pushdown-types.txt`. The Javadoc of `TrinoCompatibilityRegistry`
lists what each list leaves out and why.

**Column-level limits** in `TrinoClient.toColumnMapping` may depend only on
the type handle and the domain values, because workers rebuild the mapping
without the coordinator's capabilities.

**Tests.** `TrinoQueryRunner` starts a remote Trino (tpch, memory, geospatial)
and a local Trino with this connector, both in process. `TestTrinoConnectorTest`
is the one concrete class of the chain `BaseJdbcConnectorTest` ←
`AbstractTestTrinoTypeMapping` ← `TestTrinoConnectorIntegration` ←
`TestTrinoConnectorTest`, so `-Dtest=TestTrinoConnectorTest` runs the JDBC
contract, type mapping, and integration tests together. Most other `Test*`
classes test one class.

## Rules for remote-version behavior

- A registry check must depend on the function, operator, or types of a call,
  never on the shape of its arguments. The engine pushes the arguments down on
  their own and then the rest of the expression, which refers to them as
  columns.
- When a remote release evaluates an entry of the basic list differently,
  remove the entry so that the local server evaluates it. Ask the user before
  adding any condition on a remote release, a probe, or a version range.
- To add an entry or type to the basic list, add it to
  `testing/remote-version-smoke/basic-pushdown-types.txt` or the statements in
  `testing/remote-version-smoke/run.sh` first, and to the registry only after
  the version smoke passes for the releases that
  `.agents/skills/trino-release/scripts/select-remote-version-smoke-versions.sh`
  selects. `TestTrinoCompatibilityRegistry` fails when the types file and the
  registry disagree.
- When an entry leaves the basic list, update the registry Javadoc,
  `README.md`, and `docs/src/main/sphinx/connector/trino.md` together, with
  the reason and the release that fixed it.
- Report cross-version smoke results as a bounded smoke test, not as a
  compatibility guarantee or "supported from N".

## Releases and docs

- `.agents/skills/trino-release/SKILL.md` is the procedure for upgrading to or
  backporting a Trino version. It lists the upstream commits to read, and its
  `bump-version.sh` owns the versioned files. It makes local commits only:
  never push, tag, or publish. Tags are `trino-N-rK`, and pushing one runs
  `.github/workflows/release.yml`.
- User-visible changes of a release go in `.github/release-notes/<tag>.md`.
- `RELEASES.md` is generated by `.github/scripts/update-supported-releases.sh`;
  do not edit it by hand.
- `README.md` is the user guide and `docs/src/main/sphinx/connector/trino.md`
  the detailed reference; behavior changes usually touch both. Both use fixed
  terms ("basic list", "full list", "remote server of the local version");
  reuse them.
