---
name: trino-release
description: Prepare a local trino2trino release for a requested Trino version. Use when asked to add support or create a release for Trino NNN. Runs preflight checks, updates version-specific files, refreshes Trino build metadata, builds/tests, and creates local commits only. Never push, create tags, or publish GitHub releases; print the exact release commands instead.
---

# Trino Release

Use this Codex repo skill as `$trino-release <version>` to prepare a local
release candidate for trino2trino against a specific Trino version. Keep the
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
3. Before editing code, summarize the expected impact to the user.
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
8. If Docker is available, check whether the upstream image exists:

   ```bash
   docker manifest inspect "trinodb/trino:${requested_version}" >/dev/null
   ```

   When the image exists and `testing/delta-smoke/run.sh` is present, run:

   ```bash
   testing/delta-smoke/run.sh
   ```

   If Docker, the image, or the smoke script is unavailable, report that it was
   skipped with the exact reason.
9. Commit on `main`:

   ```bash
   git commit -am "Upgrade connector to Trino N"
   ```

   Include new files with `git add` first if code adaptation created them. Do
   not commit on `release/trino-M`.
10. Final report:
    - summarize changes, tests, and release-note items that affected code;
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
8. Commit on the backport branch:

   ```bash
   git commit -am "Build against Trino N"
   ```

   Include new files with `git add` first if code adaptation created them.
9. Final report:
   - summarize changes, tests, and release-note evidence that affected code;
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
- `docs/delta-smoke.md`
- `testing/delta-smoke/docker-compose.yml`
- `testing/delta-smoke/run.sh`

If new versioned files appear, the residual reference scan should expose them.
