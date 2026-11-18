---
name: trino-release
description: Prepare a local trino2trino release for a requested Trino version. Use when asked to add support or create a release for Trino NNN. Runs preflight checks, updates version-specific files, refreshes Trino build metadata, builds/tests, and creates local commits only. Never push, create tags, or publish GitHub releases; print the exact release commands instead.
argument-hint: <version>
disable-model-invocation: true
---

# Trino Release

Invoke this repo skill as `/trino-release <version>` in Claude Code or
`$trino-release <version>` in Codex to prepare a local release candidate for
trino2trino against a specific Trino version. Keep the
public release action manual: do not push, create tags, or run GitHub release
commands.

## Guardrails

- Work only in this repository.
- Stop on dirty working trees, unsynchronized `main`, missing Trino versions, or
  missing/incompatible local JDKs.
- Inspect both local and remote `trino-N-rK` tags before reporting the next tag.
- Use `release/trino-N` branches and `trino-N-rK` tags in reports, but do not
  create or push tags.
- Start backports from `main`, not from an existing older release branch.
- Do not modify `main` while working on a backport branch.
- Treat `.github/scripts/bootstrap-trino-deps.sh` as the dependency bootstrap
  entry point.
- Use `.mvn/modernizer/violations.xml` and
  `.mvn/modernizer/violations-production-code-only.xml` from the upstream Trino
  tag matching the requested version.

## Preflight

1. Require `$version`. If it is absent, ask for the Trino version.
2. Resolve the skill directory from the repository root:

   ```bash
   repo_root="$(git rev-parse --show-toplevel)"
   skill_dir="${repo_root}/.agents/skills/trino-release"
   ```

3. Run:

   ```bash
   "${skill_dir}/scripts/preflight.sh" "$version"
   ```

4. If preflight fails, report stderr and stop.
5. Parse the successful stdout as `key=value` lines. Important keys:
   `requested_version`, `current_version`, `case`, `target_jdk`,
   `current_jdk`, `local_java_major`, `release_branch_exists_local`,
   `release_branch_exists_origin`, `release_branch_for_current_exists_local`,
   `release_branch_for_current_exists_origin`,
   `release_branch_for_current_local_sha`,
   `release_branch_for_current_origin_sha`, `preserve_branch_push_needed`,
   `local_existing_tags`, `remote_existing_tags`, `existing_tags`, and
   `next_tag`. `existing_tags` is the de-duplicated union of local and remote
   release tags.
6. Enforce the common JDK gate before editing:
   if `local_java_major != target_jdk`, tell the user to install/select the
   target major JDK, for example with `sdk install java <major>` or
   `sdk use java <installed-version>`, then stop.
7. Run `git switch main`. Preflight already verified a clean tree and
   synchronized `main`.

## Case A: Upgrade (`requested_version > current_version`)

1. Preserve the current release line:
   - If `release_branch_for_current_exists_origin=true`, do not create or
     report a preserve-branch push command.
   - If origin does not have `release/trino-M` and the local branch does not
     exist, run `git branch release/trino-M HEAD` while staying on `main`.
   - If origin does not have it but the local branch already exists, do not
     recreate it. Set final reporting to include its push command.
2. Read the official release notes for each version in `(M, N]` from
   `https://trino.io/docs/current/release/release-<version>.html`.
   Focus on General, Security, SPI, JDBC driver, breaking changes, and changes
   related to `trino-base-jdbc`, `trino-client`, `trino-jdbc`, `trino-parser`,
   `trino-matching`, `trino-plugin-toolkit`, and Airlift.
   - The connector sends functions, casts, and arithmetic only to a remote
     server that reports the version of the local server. A behavior change of
     those in `(M, N]` cannot change pushdown results, so do not add a check or
     a probe for it. Add its edge cases to
     `testing/remote-version-smoke/delegation-expressions.txt`, which the
     same-version smoke compares with local evaluation.
   - A setting can make a server of release N evaluate those as an earlier
     release does. Trino 477 changed decimal arithmetic and added
     `deprecated.legacy-arithmetic-decimal-operators` to restore the earlier
     rules, and Trino 482 did the same with
     `deprecated.legacy-varchar-to-char-coercion`. Search the release notes of
     `(M, N]` for `can be restored` and `deprecated.`, and list the
     `@Config("deprecated.` properties of `FeaturesConfig` in release N. For
     each setting that changes the result of something the connector sends:
     1. Add one value to `EVALUATION_PROBE` and the property to
        `EVALUATION_SETTINGS` in `TrinoRemoteCapabilities`. A remote server of
        the local version that returns another value receives the basic list.
     2. Add the property to
        `testing/remote-version-smoke/conf/legacy-settings/config.properties`,
        `README.md`, and `docs/src/main/sphinx/connector/trino.md`.
     3. The connector cannot read the settings of the local server. When a call
        shows the local setting, as the result type of decimal arithmetic does
        in `TrinoCompatibilityRegistry`, keep the call local. Otherwise
        document the limit, as for `deprecated.regex-library`.

     Remove the value when Trino removes the setting.
   - The local server holds some remote values in another form than the remote
     server. Trino 484 added dates and times inside JSON values, which a server
     sends as JSON strings. An operation whose result depends on the lost part,
     such as a comparison of JSON values, must not be sent to any remote
     server: add it to the operations that `TrinoCompatibilityRegistry` leaves
     out of the full list. Check the types that release N changes for such a
     difference between the value and its text in the client protocol.
   - A remote server of any other version receives the basic list of
     `TrinoCompatibilityRegistry`: comparisons, `AND`, `OR`, `NOT`, `IS NULL`,
     `IN`, joins, the basic aggregates, and `ORDER BY ... LIMIT`, on the types in
     `testing/remote-version-smoke/basic-pushdown-types.txt`. Column filters and
     the SQL that the connector generates reach every remote version too. Check
     every behavior change and incorrect-result fix in `(M, N]` that can change
     one of those: comparison, ordering, hashing, grouping, or DISTINCT of a
     listed type, `count`, `count_if`, `checksum`, `min`, `max`, `sum`, `avg`,
     `IN`, the join types, and the planning of filters, joins, aggregations,
     and TopN. A planner rule can change a result without any change to an
     operator: Trino 484 fixed a rule that dropped a row of a `FULL JOIN`.
   - The release notes do not list every behavior change. The Trino 484 notes
     fold the commit that rejects unrepresentable `timestamp with time zone`
     values into another item. Also list the commits in `(M, N]` in a clone of
     `trinodb/trino`:

     ```bash
     git log --format='%h %s' M..N -- \
       core/trino-main/src/main/java/io/trino/operator/aggregation \
       core/trino-main/src/main/java/io/trino/type \
       core/trino-main/src/main/java/io/trino/sql/gen \
       core/trino-main/src/main/java/io/trino/sql/planner \
       core/trino-spi/src/main/java/io/trino/spi/type
     ```

     Read every title, and the message and diff of each commit that can change
     the result of an entry of the basic list.
   - List the commits in `(M, N]` that adapted the built-in JDBC connectors,
     and take the same direction. Read the code, the tests, and the connector
     reference of those commits. Differ only for a reason that is specific to
     a Trino data source, and state it:

     ```bash
     git log --format='%h %s' M..N -- plugin/trino-base-jdbc plugin/trino-postgresql plugin/trino-mysql
     ```
   - A remote server of the local version must be fully supported. A remote
     server of another version is supported as far as a simple fallback to
     local evaluation allows.
   - When a release evaluates an entry of the basic list differently, remove
     the entry or the type from the basic list and from
     `basic-pushdown-types.txt`, so that the local server evaluates it. Add it,
     with the reason and the release that fixed it, to the list in the Javadoc
     of `TrinoCompatibilityRegistry`, `README.md`, and
     `docs/src/main/sphinx/connector/trino.md`.
   - Do not add code that works around a bug of a remote release, such as a
     probe for a fix or a rewrite of the SQL that the connector sends. The
     remote server returns the same result for a query that a user sends to it.
     List the bug under the bugs of remote releases in `README.md` and
     `docs/src/main/sphinx/connector/trino.md`.
   - The settings probe described above is a condition on the settings of a
     remote server of the local version, not on a release. Ask the user before
     adding a condition on a remote release, a probe, or a version range.
   - To add an entry or a type to the basic list, add it to
     `basic-pushdown-types.txt` or to the statements in
     `testing/remote-version-smoke/run.sh` first. Add it to
     `TrinoCompatibilityRegistry` only after the remote version smoke passes for
     the releases that `scripts/select-remote-version-smoke-versions.sh N`
     selects. An entry that the Javadoc lists as left out can return when the
     oldest remote release that the smoke test runs has the fix that the
     Javadoc names.
   - A check must depend on the function, the operator, or the types of a call,
     not on the shape of its arguments. The engine also pushes down the
     arguments of an expression on their own and then the rest of it, which
     then refers to the arguments as columns.
   - For each fix that can reach the remote server, reproduce the old result on
     the oldest remote version that the smoke test runs, through the connector,
     and add a query to `testing/remote-version-smoke`.
3. Before editing code, summarize the expected impact to the user, including the
   classification above.
4. Run:

   ```bash
   "${skill_dir}/scripts/bump-version.sh" "$current_version" "$requested_version" "$target_jdk"
   ```

5. Review residual references printed by the script. Manually fix any legitimate
   missed references; leave unrelated numbers untouched.
6. Run `.github/scripts/bootstrap-trino-deps.sh`.
7. Build in two stages:

   ```bash
   mvn clean verify -DskipTests -Dair.check.skip-all=true
   mvn clean verify
   ```

   Use release notes and compiler/test failures to make the smallest compatible
   code changes needed. If the full verify fails only because `airstyle:check`
   reports formatting drift, run `mvn airstyle:format`, review the resulting
   diff, then rerun `mvn clean verify`. If Checkstyle reports a mechanical
   source-modernization rule from the new Trino/Airlift parent, make the
   smallest syntax-only change and rerun the failing Maven stage.
8. Run the strict same-version remote Delta smoke release gate:

   ```bash
   docker manifest inspect "trinodb/trino:${requested_version}" >/dev/null
   ```

   Fail the release preparation with an exact reason if Docker is unavailable,
   `testing/remote-delta-smoke/run.sh` is missing, or
   `trinodb/trino:${requested_version}` has no Docker image manifest. Otherwise
   run:

   ```bash
   testing/remote-delta-smoke/run.sh
   ```

   If the smoke fails, stop the release preparation and record the failing
   command and `target/remote-delta-smoke/` log directory.
9. Run the remote version smoke release gate:
   - Select remote version smoke versions:

     ```bash
     read -r -a remote_version_smoke_versions < <("${skill_dir}/scripts/select-remote-version-smoke-versions.sh" "$requested_version")
     ```

   - Fail the release preparation with an exact reason if Docker is unavailable,
     `testing/remote-version-smoke/run.sh` is missing, or either
     `trinodb/trino:${requested_version}` or `trinodb/trino:${remote_version}`
     has no Docker image manifest.
   - Otherwise run:

     ```bash
     testing/remote-version-smoke/run.sh "$remote_version"
     ```

   - Also run the smoke against a remote server of the requested version with
     the settings that change evaluation. It must receive only the basic list:

     ```bash
     testing/remote-version-smoke/run.sh "${requested_version}-legacy-settings"
     ```

   - If the smoke fails, stop the release preparation. Record the remote
     version, the failing command, and the log directory
     `target/remote-version-smoke/${requested_version}-to-${remote_version}/`.
   - Report this only as a bounded remote version smoke gate, not as a cross-version
     compatibility guarantee.
10. Write the changes that a user of the previous connector release sees to
    `.github/release-notes/<next_tag>.md`. The release workflow puts the file
    into the body of the GitHub release, before the install steps. Cover:
    - results that change, including fixes of incorrect results;
    - operations that move between the remote and the local server, with the
      remote versions or settings that decide it;
    - configuration and session properties that were added, changed, or
      removed;
    - steps that the upgrade needs.

    State each change from the side of the user, with an example query when
    pushdown changes, and name the README section with the details. Do not
    create the file when nothing changes for users.
11. Commit on `main`:

   ```bash
   git commit -am "Upgrade connector to Trino N"
   ```

   Include new files with `git add` first, such as the release notes file and
   files that code adaptation created. Do not commit on `release/trino-M`.
12. Final report:
    - summarize changes, tests, and release-note items that affected code;
    - name the release notes file, or state that the release has none;
    - summarize remote Delta smoke and remote version smoke results, including
      selected remote versions and any failing command or log location;
    - print `git push origin release/trino-M` only when
      `preserve_branch_push_needed=true`;
    - print `git push origin main`;
    - print `git tag <next_tag> && git push origin <next_tag>`;
    - state that pushing the tag triggers GitHub Release publication.

## Case B: Backport (`requested_version < current_version`)

1. If either `release_branch_exists_local=true` or
   `release_branch_exists_origin=true`, stop. Report `local_existing_tags`,
   `remote_existing_tags`, and `existing_tags`, then print only the re-release
   command using `next_tag`, after switching/updating that existing release
   branch manually.
2. Before branching or editing, run a backport impact review from `main`:

   ```bash
   "${skill_dir}/scripts/backport-impact.sh" "$requested_version" "$current_version"
   ```

   Use the helper output as evidence, not as an automatic decision. Review
   official release notes in `(N, M]` for features added after N that current
   `main` may depend on. Prioritize `Add the ... type`, `Add support for`,
   `Breaking change`, `Remove`, `Defunct`, `SPI`, `JDBC driver`, Base JDBC,
   connector API, configuration property, and Airlift lines. Search current
   source, tests, docs, and smoke configs for matching Trino APIs, types,
   properties, and EXPLAIN markers. Classify each likely issue before editing:
   - `must-remove`: type/API/feature first introduced after N, so unavailable in N.
   - `signature-risk`: connector or SPI method signatures changed across the gap.
   - `runtime-config-risk`: Docker/catalog/session properties may not exist in N.
   - `test-expectation-risk`: plan text or pushdown markers differ by version.
   - `needs-compile-confirmation`: release notes are suggestive but not decisive.

   Summarize expected removals/adaptations to the user before the version bump.
   For example, if release notes say a type was added in a version greater than
   N and current code references that type, plan to remove or disable it on the
   backport branch, then confirm with the target Trino artifacts and compiler.
3. Create and switch to the backport branch from `main`:

   ```bash
   git switch -c "release/trino-N"
   ```

4. Run:

   ```bash
   "${skill_dir}/scripts/bump-version.sh" "$current_version" "$requested_version" "$target_jdk"
   ```

5. Run `.github/scripts/bootstrap-trino-deps.sh`.
6. Build in two stages:

   ```bash
   mvn clean verify -DskipTests -Dair.check.skip-all=true
   mvn clean verify
   ```

7. Use the impact review and the actual compiler/test errors to make the minimal
   compatibility changes required. If compilation fails because current `main`
   code uses APIs missing from Trino N, inspect only the additional release notes
   or upstream sources needed to explain the failing symbols. If the full verify
   fails only because `airstyle:check` reports formatting drift, run
   `mvn airstyle:format`, review the resulting diff, then rerun
   `mvn clean verify`. If Checkstyle reports a mechanical
   source-modernization rule from the selected Trino/Airlift parent, make the
   smallest syntax-only change and rerun the failing Maven stage.
8. Run the strict same-version remote Delta smoke release gate:

   ```bash
   docker manifest inspect "trinodb/trino:${requested_version}" >/dev/null
   ```

   Fail the release preparation with an exact reason if Docker is unavailable,
   `testing/remote-delta-smoke/run.sh` is missing, or
   `trinodb/trino:${requested_version}` has no Docker image manifest. Otherwise
   run:

   ```bash
   testing/remote-delta-smoke/run.sh
   ```

   If the smoke fails, stop the release preparation and record the failing
   command and `target/remote-delta-smoke/` log directory.
9. Run the remote version smoke release gate:
   - Select remote version smoke versions:

     ```bash
     read -r -a remote_version_smoke_versions < <("${skill_dir}/scripts/select-remote-version-smoke-versions.sh" "$requested_version")
     ```

   - Fail the release preparation with an exact reason if Docker is unavailable,
     `testing/remote-version-smoke/run.sh` is missing, or either
     `trinodb/trino:${requested_version}` or `trinodb/trino:${remote_version}`
     has no Docker image manifest.
   - Otherwise run:

     ```bash
     testing/remote-version-smoke/run.sh "$remote_version"
     ```

   - Also run the smoke against a remote server of the requested version with
     the settings that change evaluation. It must receive only the basic list:

     ```bash
     testing/remote-version-smoke/run.sh "${requested_version}-legacy-settings"
     ```

   - If the smoke fails, stop the release preparation. Record the remote
     version, the failing command, and the log directory
     `target/remote-version-smoke/${requested_version}-to-${remote_version}/`.
   - Report this only as a bounded remote version smoke gate, not as a cross-version
     compatibility guarantee.
10. Write the changes that a user of the previous connector release sees to
    `.github/release-notes/<next_tag>.md`. The release workflow puts the file
    into the body of the GitHub release, before the install steps. Cover:
    - results that change, including fixes of incorrect results;
    - operations that move between the remote and the local server, with the
      remote versions or settings that decide it;
    - configuration and session properties that were added, changed, or
      removed;
    - steps that the upgrade needs.

    State each change from the side of the user, with an example query when
    pushdown changes, and name the README section with the details. Do not
    create the file when nothing changes for users.
11. Commit on the backport branch:

   ```bash
   git commit -am "Build against Trino N"
   ```

   Include new files with `git add` first, such as the release notes file and
   files that code adaptation created.
12. Final report:
   - summarize changes, tests, and release-note evidence that affected code;
   - name the release notes file, or state that the release has none;
   - summarize remote Delta smoke and remote version smoke results, including
     selected remote versions and any failing command or log location;
   - print `git push origin release/trino-N`;
   - print `git tag <next_tag> && git push origin <next_tag>`;
   - state that `main` was not changed and pushing the tag triggers GitHub
     Release publication.

## Case C: Same Version (`requested_version == current_version`)

Do not edit files. Report that no version change is needed, list
`existing_tags`, and print the optional re-release command using `next_tag`.

## Versioned Files

The deterministic bump script owns these version references:

- `pom.xml`
- `README.md`
- `docker-compose.yml`
- `CONTRIBUTING.md`
- `docs/src/main/sphinx/connector/trino.md`
- `docs/remote-delta-smoke.md`
- `testing/remote-delta-smoke/docker-compose.yml`
- `testing/remote-delta-smoke/run.sh`

If new versioned files appear, the residual reference scan should expose them.
