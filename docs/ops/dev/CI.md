# CI architecture

This page explains how swath's GitHub Actions CI is shaped and why, for a maintainer or
contributor who needs to change it. It owns the reasoning. The exact jobs, commands, triggers and
runners are owned elsewhere and are only summarized here:

- [`docs/internals/testing.md`](../../internals/testing.md): every Go job, the race shards, change
  detection, the Go caches, cross-OS skips, nightly and stress dispatches.
- The workflows in [`.github/workflows/`](../../../.github/workflows/) and the scripts in
  [`scripts/ci/`](../../../scripts/ci/). Each workflow job carries a comment explaining its own
  constraints; read it before changing the job.

## The workflows

| Workflow | Trigger | Job |
| --- | --- | --- |
| [`ci.yml`](../../../.github/workflows/ci.yml) | every pull request push/label change, every push to `main`, manual dispatch from any branch | the per-commit gate: Go jobs, script self-tests, `ci-gate` |
| [`nightly.yml`](../../../.github/workflows/nightly.yml) | 07:00 UTC on `main`; dispatch with a `jobs` selector | what is too slow, too timing-sensitive or too time-dependent for every commit |
| [`release.yml`](../../../.github/workflows/release.yml) | a pushed `v*` tag | retest and build the Go binaries and images at the tag, then publish, sign and attest |
| [`site.yml`](../../../.github/workflows/site.yml) | every pull request and push; dispatch | site consistency checks; deploy to GitHub Pages from `main` |
| [`cache-maintenance.yml`](../../../.github/workflows/cache-maintenance.yml) | main CI or CodeQL completion, hourly, dispatch | bound Go and CodeQL cache generations |
| [`go-stress.yml`](../../../.github/workflows/go-stress.yml) | dispatch only | repeat one Go test or pattern N times on a chosen runner |

The split follows one question: what does a failure mean? A red `ci.yml` means this commit broke
something and must not merge. A red nightly means an invariant broke outside the per-commit gate,
or time passed (a new advisory in the OSV scan of the Go release images, built from the `Dockerfile`)
without any commit at all. A red release means the
tag must not be published. `site.yml` keeps its checks and its deploy in one file so the deploy's
gate is the file itself, not a branch rule someone has to remember. `go-stress.yml` is a tool, not a
gate: it exists so a suspected flake is measured by repetition instead of being retried until green.

`release.yml` ships the Go implementation. It does not trust the `ci.yml`
run of the tagged commit to have produced the bytes it ships, but it does rely on that commit having
passed full Go validation. It separately verifies that the tag is an ancestor of `main` and requires
`go-full-gate` from the exact commit's main push run. It then reruns `go vet`
and the race tests unsharded on the tag's SHA (not `ci.yml`'s whole matrix), builds every target's
binaries once into the release archives, builds both images from those archived binaries, pushes
them by digest, smokes the digests, and only then applies tags and signs.
[`RELEASING.md`](../../../RELEASING.md#what-gates-a-release) owns what gates a release and why.

[`dependabot.yml`](../../../.github/dependabot.yml) keeps the pinned actions (in the workflows and the composite actions under `.github/actions/`), Docker base images (`Dockerfile`) and the three Go modules current, with one weekly pull request per configured group. The Actions and Docker groups take every update, majors included; the Go module groups take only minor and patch updates, so a Go module's major update arrives in a pull request of its own. In the root module and `objfile` the Azure SDK modules (`github.com/Azure/azure-sdk-for-go/*`) form a group of their own, out of the catch-all one: their minor releases change the requests swath sends, so each is reviewed alone. Each provider client in the root module (S3, GCS and Azure) holds the requests it sends to a wire snapshot (`TestWireSnapshot`, [clients](../../internals/clients.md#wire-snapshots)), so an AWS SDK or Google API update in the catch-all group that changes a request fails that test and is reviewed against the snapshot as well. It does not raise the `toolchain` line of `go.mod`, so the golang image digest in the `Dockerfile` and that line move together by hand. Tools downloaded by version are hand-bumped as well, each with the release asset's SHA-256 where the download is direct: the DuckDB CLI in the `ci.yml` jobs that read Parquet back and in `release.yml` `publish`, OSV-Scanner in `scripts/ci/osv-scan.sh`, Syft as the `syft-version` input of `anchore/sbom-action` in `release.yml`, go-licenses in `notices.sh`, and Buildx and the BuildKit image (`version` and `driver-opts` of `docker/setup-buildx-action` in `release.yml`, the image by digest).

## Registry and artifacts

GHCR is written by `release.yml` only. Pull requests, `main` pushes and dispatches build and smoke
selected Go images in `go-build`, and throw them away.
Go images run on packaging/dependency/full PRs and on every main/dispatch run; static cross-builds and
the license gate remain on every selected main-module PR. Nothing
outside a release pushes a tag, so the registry holds release versions and nothing else. An
unreleased commit's image is built locally from the checkout (`docker build .`).

The artifact uploads of `ci.yml`, `nightly.yml`, `go-stress.yml` and `release.yml` set their own `retention-days`
instead of taking the repository default:

- 14 days for the per-commit `go test -json` streams in `ci.yml`;
- 30 days for the diagnostics and reports of `nightly.yml` and `go-stress.yml`, and the release's
  `go test -json` stream;
- 1 day for an artifact that only hands files from one job to another in the same run.

## One required check: `ci-gate`

The branch rule on `main` requires exactly one status check, `ci.yml` `ci-gate`. It is an
aggregate job that `needs` every gating job and fails unless each one passed.

It exists because GitHub requires checks by name, and a matrix job reports one name per leg
(`go-race (crash)`, `go-xos (windows-latest)`). A
rule that names `go-race` matches nothing; a rule that names the legs goes stale the moment a shard
or platform is added. `needs` collapses all of a matrix's legs into one result, so a new leg extends
the gate without touching the branch rule. A rule listing jobs also cannot notice a job whose scope
silently shrank; an aggregate that inspects every dependency can.

Two details carry the weight:

- `if: always()`. Without it a failed dependency skips `ci-gate`, a skipped required check never
  reports, and the pull request waits instead of going red.
- `skipped` is a failure on every event. An unaffected job runs with its work skipped, so its
  result is `success`. A skipped job means a dependency failed or a tier was removed from the gate.
  This also makes the gate usable as evidence for full main validation.

The job writes a table of every dependency and its result to the step summary; that table is the
first place to look when the gate is red.

Cache maintenance is a separate housekeeping workflow and is not gated. Neither `nightly.yml` nor
`site.yml` is a required check.

## Change detection

[`scripts/ci/go-ci-policy.py`](../../../scripts/ci/go-ci-policy.py) selects complete Go modules and
tiers. PR selection follows changed paths and local module dependencies: reader changes include main,
object-file changes stay independent unless a consumer is added, and main changes do not retest an
unchanged independent module. Testdata, embedded files, generated-help documents and external replay
fixtures are inputs too. The exact module/tier mapping and escalation rules live in
[Go testing](../../internals/testing.md#change-detection); regression cases exercise the actual selector,
workflow matrix shell, rename/deletion behavior and the gate.

Main and manual dispatch always select full Go validation. PRs can request it with the `ci:full` label;
shared build/CI/toolchain changes and unknown ownership also select it. Platform and dependency changes
request broader runtime checks for their affected modules. This is module-level selection, not an
attempt to infer which individual assertions a source edit affects.

The provider evidence the replay fixtures are written from selects main and the fixture tier. An absent/zero base or empty diff selects full validation; a failed diff is an error. Both
sides of renames are considered.

Unaffected jobs succeed with skipped steps. Matrices use one `off` leg on ubuntu-latest when no work
is selected, so no platform runner is queued. The nightly workflow is unfiltered and also backstops
input-selection mistakes.

### Full validation and releases

`go-full-gate` depends directly on every Go job on full-scope PRs and main/dispatch. It requires every job to pass, every
Go selection to be enabled, the full race-shard matrix and exactly the three full platform legs.
Regression tests require both aggregates to include every Go job. Full-scope PRs exercise the
qualification before merge, but only its main push result can qualify a release. Main runs retain independent concurrency groups, so a newer merge cannot
cancel the older commit's qualification. A red main run must be fixed or reverted; green PR checks do
not qualify a release.

[`verify-release-go-ci.py`](../../../scripts/ci/verify-release-go-ci.py) requires that job's success
from the **newest main push run and its current attempt**, in `ci.yml` in this repository, at the
**exact tagged SHA**. PR/dispatch runs, another SHA, a skipped/missing gate, and stale successes are
not substitutes. Only a commit that was the head of a main push can qualify; an intermediate
commit in a multi-commit push or a PR branch commit merely reachable through a merge has no such run.
Tag the validated pushed main head instead. A pending/missing main run is given up to ten minutes; then release fails with a
message explaining the eligibility rule and how to wait for pending CI. A completed failed gate fails immediately. The guard uses read-only
Actions access and does not dispatch builds or publish anything.

## Why this PR scope

The PR loop keeps the complete race suite for affected modules, distinct heap/conformance checks,
cheap cross-compilation, and architecture-sensitive tests. It avoids repeating heavy CLI/engine
packages without race instrumentation on the same arm64 runner. Other packages retain their arm64
run, derived from the race-shard table. Packaging can wait for main except on relevant changes;
its license check stays on PRs. Moving off-critical-path jobs saves runner capacity, not necessarily
elapsed time. [Measurements](../../internals/measurements.md#ci-go-caches-race-shards-and-cross-os-legs-2026-11-09)
record the observed costs; there is no fixed PR-duration guarantee.

This is a deliberate latency/coverage tradeoff. Comparable projects use different policies:
[restic](https://github.com/restic/restic/blob/c734245fd3ae9bcac8ab3aaafb4482d4b5d0ae92/.github/workflows/tests.yml)
keeps multiple OSes and cross-builds on PRs;
[JuiceFS](https://github.com/juicedata/juicefs/blob/46b443cdd0f16ad11d30c819eb5c4052d12257de/.github/workflows/unittests.yml)
shards its core unit suite on Linux and has
[targeted Windows testing](https://github.com/juicedata/juicefs/blob/46b443cdd0f16ad11d30c819eb5c4052d12257de/.github/workflows/wintest.yml).
Those examples motivate explicit tiers, not a claim that deferring platform tests is universal practice.

## Script self-tests

`scripts` runs the self-tests of the Python scripts that no Go job exercises: the OSV image
policy (`osv-image-policy.py --self-test`, which the nightly `osv-scan` relies on), the
provider-conformance tools under `scripts/provider-conformance/`, the replay benchmark
runners under `scripts/benchmarks/replay/`, and the run explainer (`tools/explainer/explainer.py
--self-test`, over a captured run trace). They are hermetic and take
seconds, so the job runs on every event without change detection.

## Go jobs

The Go port's jobs run side by side so that a vet error reports in about a minute and the race
tests' wall time is their slowest shard, not their sum (one serial job once took 17.5 minutes).
[`docs/internals/testing.md`](../../internals/testing.md) owns the commands.

| Job | Purpose |
| --- | --- |
| `go-lint` | the license headers (`scripts/ci/check-spdx.sh`) and the Markdown links (`go test ./docs`) on every event, `gofmt`, `go vet`, `go mod tidy -diff`, and the shard table's self-test and coverage check |
| `go-race (SHARD)` | `go test -race` on one shard of the main module |
| `go-nonrace` | tests that measure the process (heap gates, the shape suite) and SeekParquet conformance, which `-race` would distort |
| `go-build` | static cross-builds of both commands for five targets, the version stamp, the Go lister and replay images (`Dockerfile`) for two architectures and their smokes; the image build runs the Go license gate (`notices.sh`) |
| `go-objfile`, `go-reader` | the nested modules `objfile` and `reader`: vet, tidy, race tests, cross-builds |
| `go-xos (OS)` | arm64 complement on ordinary PRs; full selected-module platform suites on escalation, main and dispatch |
| `go-fixtures` | the replay fixture tier on the fixtures `bench/replay/fixtures` writes |

The nested modules have their own `go.mod`, so `./...` at the root never reaches them; each needs its
own job or it is untested.

### Race shards

The shards are one table in
[`scripts/ci/go-race-shards.sh`](../../../scripts/ci/go-race-shards.sh). The `changes` job expands
it into the `go-race` matrix (`--matrix`), each leg asks it for its `go test` arguments, and
`go-lint` runs `--check` against the real package and test lists. The workflow names no shard, so
a shard cannot be added in one place and forgotten in the other. A row either splits one package by
`-run` pattern (with one remainder row that `-skip`s the rest) or runs whole packages by path and
`./x/...` prefix; `rest` is every package no row names, so a new package lands somewhere without
an edit. `--check` proves every package is in exactly one shard and, for a split package, that every
test is selected by exactly one of its rows; whole-package rows are checked for packages, not tests,
so a row whose packages have no tests passes and runs nothing. `--self-test` checks the table logic
against synthetic package lists without Go.

The heavy shards (`internal/listing` and `cmd/swath`, split by their crash, durable and kill
matrices) race on `ubuntu-24.04-arm`. Its weaker memory model makes it the better race target, and
the same suite took about 30 % less time there (`internal/listing` 490 s against 700 s). The
`rest*` shards stay on amd64: that gain was measured on `internal/listing` only, and the Parquet
encoder path differs by architecture (AVX2 on amd64, generic elsewhere; `internal/parquetout`
`footeronly_test.go` pins the bytes of both, #308), so the `rest*` shards' amd64 run and the
`ubuntu-24.04-arm` `go-xos` leg each exercise one of them. The nightly unsharded
`go-race-nightly` run on amd64 covers the other side of each heavy shard.

In the measured history (2026-11-09, about 200 runs), a warm run took 7–8 minutes and its critical
path was the single `go-race (rest)` shard at 5.4–8.4 minutes. That shard is now three
(`rest-replay`, `rest-source`, `rest`); their observations are recorded in [measurements](../../internals/measurements.md).

Each leg runs with `-count=1`, because a restored build cache would otherwise replay earlier
results as `(cached)` instead of racing them, and with a per-package `-timeout` below the job's
limit so a hang ends in Go's goroutine dump rather than a killed runner. The `go test -json` stream
is kept as the `go-race-SHARD` artifact for flakes and rebalancing, and the step prints the exact
command to rerun the shard locally.

### Cross-OS

Ordinary PRs retain arm64 coverage for packages raced on amd64, using the complement of the shard
table. They do not repeat `internal/listing` and `cmd/swath` there. Full platform validation runs
on main and dispatch, and on PRs selected by platform/dependency inputs or `ci:full`. Cross-builds
remain on PRs. The selected-module and platform-skip details belong to
[Go testing](../../internals/testing.md#cross-os).

macOS and Windows runtime coverage is usually deferred because those jobs and their runner queues
previously dominated PR turnaround. It has caught real platform differences, so it is retained on
main/nightly and required before release. Path escalation is an early-warning aid, not proof that
unselected code cannot have platform-specific behavior.

## Caching

The Actions cache is limited to 10 GB per repository. In the measured history it held 16.3 GB,
so entries were evicted continuously; the Go caches were 9.15 GB of that, in 16 per-job entries,
each carrying its own copy of the same module cache. The current layout removes the duplication
and bounds the generations.

### Go caches

The [`setup-go`](../../../.github/actions/setup-go/action.yml) composite action, used by every Go
job, restores two caches. DEPS is one hash of the `go.mod` and `go.sum` of all three modules.

| Cache | Key | Shared by |
| --- | --- | --- |
| module (`GOMODCACHE`) | `go-mod-OS-ARCH-DEPS` | every job on that OS and architecture |
| build (`GOCACHE`) | `go-build-OS-ARCH-JOB-DEPS-SHA` | one job, or one race shard |

Only a push to `main` saves; pull requests, and the nightly and stress jobs that use the
composite, restore `main`'s newest without saving. Pull requests do not save because each would
add a generation per job that only that pull request can read, and fork pull requests cannot write
the cache anyway.

The module cache's contents depend only on the dependency set, so its key names no job and no
commit, and one copy serves every job. A saved key is never overwritten: when several `main` jobs
try to save the same new key, the first wins and the others log a warning. Because the jobs build
different module graphs (`reader` has no cloud SDK), a `main` job that misses the module key
downloads every module's whole dependency graph (`go mod download all`, which includes the test
imports of dependencies that `go mod tidy -diff` loads) before it may save, so the first job to
finish cannot freeze a partial cache.

The build cache differs per job (race or not, target OS and architecture, nested module), so it
stays per job. It is keyed per commit so that parallel jobs never overwrite one another and each
`main` commit saves a whole cache; restore falls back to the job's newest entry with the same
DEPS, then any.

Per-commit build keys would grow without bound, so `cache-maintenance.yml` runs
[`scripts/ci/cache-prune.sh`](../../../scripts/ci/cache-prune.sh) after main CI, on CodeQL
completion events, hourly, and on manual dispatch. The hourly pass backstops dynamic CodeQL
setups that do not emit completion events and catches late writes. Only this housekeeping job
grants `actions: write`. It checks out `main`, never the triggering PR, and consumes no artifacts
from it. Its concurrency group cannot cancel a job in the main CI workflow.

The policy keeps the newest Go build cache per job family, Go module cache per OS/architecture,
and CodeQL dependency and overlay cache per family on `main`. Cache versions are separate
families. CodeQL PR dependency copies are deleted; scans can restore main's shared copy or
fetch dependencies again. Scanning remains enabled. Unknown cache key formats and other refs
are untouched. The script can print a plan without writes using `--plan` with an API cache array
on stdin.

Newest means creation time, not this run's commit: overlapping main runs must not remove a newer
run's caches. A fixed-order, deduplicated snapshot limits deletions to observed IDs; new entries
are left for the next pass. This bounds generations, not bytes: concurrent writes can temporarily
exceed the quota, and the retained families still need to fit the repository budget.

### Other caches

- **Release:** `release.yml` restores no cache: it sets up Go with `actions/setup-go` directly, its
  cache off, and builds and tests the tag cold.
- **BuildKit:** `type=gha` caches in the scope `swath-go`, written from `main` only (`go-build` on
  a push). A pull request reads
  them and writes nothing: a cache written on a pull request is scoped to that pull request's
  ref, so it serves no other run while taking 0.5–1 GB of the quota per open pull request.
- **Code scanning:** GitHub's default CodeQL setup remains enabled; the retention policy above
  prevents its PR dependency caches from accumulating in the shared budget.

## Concurrency

`ci.yml` groups pull request runs by ref with `cancel-in-progress`, so a new push cancels the
superseded run. `main` and dispatch runs each get a unique group (`github.run_id`), so a merge is
never cancelled or queued behind another: every merge runs its full suite.

The cache maintenance workflow serializes cleanup in its own group, so two prunes never race
over the same cache generations.

The other workflows follow the same rule: `nightly.yml` coalesces per ref; `release.yml` runs one
release at a time and never cancels; `site.yml` cancels superseded pull request checks but never a
running deploy.

## Runners and cost

Everything runs on GitHub-hosted runners. Linux jobs use `ubuntu-latest` (amd64) or
`ubuntu-24.04-arm`; Windows and macOS appear only in `go-xos`, `nightly.yml` `go-xos-macos` / `go-xos-windows` and
`go-stress.yml`. The choices above are driven mostly by wall time and queueing rather than by
minutes:

- Splitting by job and by shard trades runner count for wall time. Each new shard is one more
  runner per Go pull request and one more build-cache family on `main`.
- Change detection makes an unaffected job cost a checkout and a few seconds.
- The macOS leg is the one runner whose queue time dominated the run, hence its schedule.
- The heavy race shards run on arm64 because they are both faster and a better race target there.

## When a run is slow or red

- **Red gate:** open `ci-gate`'s step summary; it lists every dependency and its result. A
  `skipped` result on a pull request means a job-level `if:` removed a job from the gate.
- **Red `go-race` leg:** the log shows each package's result live, then the output of the failed
  tests; the step prints the command to rerun the shard. The full stream is the `go-race-SHARD`
  artifact.
- **Suspected flake:** do not rerun until green. Dispatch `go-stress.yml` with the test, a count
  and the runner, and cite the run and its tally in the pull request (see
  [`docs/internals/testing.md`](../../internals/testing.md#stress-repeats)).
- **Slow run:** compare the legs' durations; a shard that grew is rebalanced in the shard table
  using its `-json` artifact. A Go job that took minutes longer than usual on `main` probably
  missed its build cache; the `cache-prune` step summary lists the kept caches.
- **Red nightly:** an invariant broke outside the per-commit gate, or the OSV scan found a
  new advisory. Treat it as a bug on `main`.

## Changing CI without breaking the gate

- **A new job:** add it to `ci-gate`'s `needs`. If it should run only for some changes, gate its
  steps on `needs.changes.outputs.*`, not the job; a job-level `if:` fails every pull request. If it
  is legitimately event-gated, it passes as `skipped` only off pull requests.
- **A new Go path the jobs depend on:** add it to the Go regex in `changes` (and to `go-fixtures`'
  own list if that tier reads it), and update the change-detection list in
  [`docs/internals/testing.md`](../../internals/testing.md#change-detection).
- **A new race shard:** add a row to the table in `go-race-shards.sh` and nothing else; run
  `--self-test` and `--check` locally, and update the shard table in `docs/internals/testing.md`.
- **A new platform:** add a leg to the `go-xos` matrix that `changes` emits, for the events where it
  should run.
- **A new Go job:** set up Go through `./.github/actions/setup-go` with a unique `key`; its build
  cache is pruned automatically after the CI workflow completes.
- **Any change:** a commit that touches Go code needs the Go jobs green, and the owning page
  ([`docs/internals/testing.md`](../../internals/testing.md)) must be
  current in the same change.
