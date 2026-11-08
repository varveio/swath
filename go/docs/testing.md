# Testing

This page maps every test method of the Go port to what it proves, where it lives, how to run it, and when
CI runs it. The invariants the tests hold are in [internals/contracts.md](internals/contracts.md). The Java
tiers are owned by [docs/ops/dev/TESTING.md](../../docs/ops/dev/TESTING.md) and appear here only in the
summary.

## Summary

**PR** is every push to a pull request (a newer push cancels the superseded run), **main** every merge to
`main`, **dispatch** a manual run of the workflow from any branch, **nightly** the 07:00 UTC schedule on `main`.
"Go change" and "Java change" are the `changes` job's outputs ([change detection](#change-detection)). All
workflows are in `.github/workflows/`. The one required check is `ci.yml` `ci-gate`.

| Method | Proves | Trigger | Runner | Workflow / job |
|---|---|---|---|---|
| Format, vet, shard check | `gofmt -l` empty, `go vet ./...`, every test in exactly one race shard | PR, main, dispatch; Go change | ubuntu-latest | `ci.yml` `go-lint` |
| Race tests: `internal/listing`, `cmd/swath` | unit and law tests, oracles, the crash, kill and durable-spill matrices, under `-race` | PR, main, dispatch; Go change | ubuntu-24.04-arm | `ci.yml` `go-race (crash)`, `(durable)`, `(listing-rest)`, `(cmd-kill)`, `(cmd)` |
| Race tests: every other package | the same for the rest of the module | PR, main, dispatch; Go change | ubuntu-latest | `ci.yml` `go-race (rest)` |
| Race tests, unsharded | `go test -race ./...` on amd64: the amd64 race coverage of `internal/listing` and `cmd/swath` | nightly; dispatch `jobs=all` or `go-race-nightly` | ubuntu-latest | `nightly.yml` `go-race-nightly` |
| Heap gates (`SWATH_HEAP_GATES=1`) | the writer admission covers the measured live heap | PR, main, dispatch; Go change | ubuntu-latest | `ci.yml` `go-nonrace` |
| Shape suite, per push (`TestShapes`) | 21 shapes at 200,000 keys: exact keys, byte-identical Parquet at 16 and 5 workers, tenfold bounds | PR, main, dispatch; Go change | ubuntu-latest | `ci.yml` `go-nonrace` |
| Shape suite, full (`bench/shapes`, 88 arms) | about 2M keys per shape behind the fake's service behaviors: output key for key; numbers against a baseline, annotated | nightly; dispatch `jobs=all` or `go-shapes` | ubuntu-latest | `nightly.yml` `go-shapes` |
| Cross-OS default tier | `go vet` and the whole suite without `-race`; [platform skips](#cross-os) | PR, main, dispatch; Go change | macos-latest (arm64), windows-latest (amd64) | `ci.yml` `go-xos (macos-latest)`, `go-xos (windows-latest)` |
| Builds and the Go replay image | static builds for 4 targets, the version stamp, the journal's Windows test compile, the image for 2 architectures and its two smokes | PR, main, dispatch; Go change | ubuntu-latest | `ci.yml` `go-build` |
| Replay fixture tier (`SWATH_REPLAY_FIXTURES`) | the Go replay store and evidence replays on fixtures the Java replay writes | PR, main, dispatch; its own, wider change detection | ubuntu-latest | `ci.yml` `go-fixtures` |
| Stress repeats | one test or pattern repeated N times on a chosen OS, race setting and `GOMAXPROCS` | dispatch only | input: ubuntu-latest, ubuntu-24.04-arm, macos-latest, windows-latest | `go-stress.yml` `stress` |
| Opt-in Go tiers | full-scale laws, fuzzing, replay load, the durable spill campaign, `verify.py`, the replay oracle against Java | manual only | a contributor's or a benchmark host | none ([opt-in tiers](#opt-in-tiers)) |
| Live buckets and benchmark sweeps | behavior and performance on real providers | manual only, never scheduled | cloud batch hosts | none ([live runs](#live-runs)) |
| Java fast tier | unit, property and concurrency tests, instrumentation drift, license gate | PR, main, dispatch; Java change. Release | ubuntu-latest | `ci.yml` `fast-tests`; `release.yml` `fast-tests` |
| Java integration tier | LocalStack ITs, DuckDB read-back | PR, main, dispatch; Java change. Release | ubuntu-latest | `ci.yml` `integration-tests`; `release.yml` `integration-tests` |
| Java deep tier and `kill -9` ITs | timing- and schedule-sensitive tests, a real `kill -9` then `--resume` | main, dispatch; Java change. Nightly. Release | ubuntu-latest | `ci.yml` `deep-tests`; `nightly.yml` `deep-and-perf`; `release.yml` `deep-tests` |
| Java perf tier | PERF-1, PERF-2, SORT-PERF | nightly; dispatch `jobs=all` or `deep-and-perf` | ubuntu-latest | `nightly.yml` `deep-and-perf` |
| Java image check | both Java images build for 2 architectures and start | PR; Java change | ubuntu-latest | `ci.yml` `docker-check` |
| Java image smoke and publish | the image writes, reads and resumes Parquet listed from a public bucket; the replay image serves it | main with a Java change, dispatch. Release, when the repository is public and `PUBLIC_RELEASE_ENABLED` is `true` | ubuntu-latest | `ci.yml` `docker-publish`; `release.yml` `publish` |
| OSV scan | Java jar closures and images against published advisories | nightly; dispatch `jobs=all` or `osv-scan` | ubuntu-latest | `nightly.yml` `osv-scan` |
| Release guards | tag SHA and ancestry (`source`), version and release notes (`fast-tests`), assets and signatures (`publish`, under the same condition as above) | tag `v*` | ubuntu-latest | `release.yml` `source`, `fast-tests`, `publish` |
| Site and documentation checks | links, media pins, `docs/style.md` terminology over `docs/**/*.md`, run facts, accessibility | PR, main, dispatch | ubuntu-latest | `site.yml` `site-checks` |
| CodeQL (external check) | static analysis; languages and schedule are repository settings | as the setting says (on 2026-11-08: PR, push to `main`, weekly) | GitHub-hosted | code-scanning default setup, not configured in this repository (`gh api repos/varveio/swath/code-scanning/default-setup`); not in `ci-gate` |
| CodeRabbit (external check) | review comments | pull requests, by the installed app | CodeRabbit | not configured in this repository (no `.coderabbit.yaml`); not in `ci-gate` |

## Running tests locally

```bash
cd go
go test -count=1 -run '^TestName$' ./internal/listing    # one test: the local loop
go test -short ./...                                      # skips the binary-building and slowest arms
```

The whole gate is `go vet ./...` and `go test -race ./...`, which CI runs sharded. Full suites, whole race
shards and repeat loops do not run on a development host: CI runs them per push and
nightly, and [`go-stress.yml`](#stress-repeats) repeats a test on a runner. Locally, run the test you are
changing, `-count=1`, and at most `-count=3` to check a fix. On a shared host, run anything heavier than one
test through one lock, so two runs never contend for the cores and skew each other's timing:

```bash
flock /tmp/swath-gates.lock env GOMEMLIMIT=2GiB go test -race -count=1 -p 1 -run '^TestCrashMatrix$' ./internal/listing
```

What a `go-race` leg runs, from `go/` (runner instructions: a whole shard belongs on a runner or `go-stress.yml`, not a development host):

```bash
mapfile -t args < <(../scripts/ci/go-race-shards.sh crash)
go test -race -count=1 -timeout 20m "${args[@]}"
```

`-short` skips the `cmd/swath` tests that build the binary (the SIGKILL, forced-exit, deadline and segment
kill tests among them), the per-push shape suite and `TestWriterAdmissionCoversHeap`, and runs fewer seeds
of the `keyrange` laws. CI never passes `-short`.

## Continuous integration

### Change detection

`ci.yml` `changes` diffs the pull request's base (or the push's previous commit) against `HEAD`:

- **Go change:** a path under `go/`, `.github/workflows/ci.yml`, `.github/actions/setup-go/`, `Dockerfile.replay-go` or its ignore file,
  `scripts/ci/smoke-replay-container.sh` or `scripts/ci/go-race-shards.sh`.
- **Java change:** any path outside `go/` and `Dockerfile.replay-go*`, so a documentation or script change
  runs the Java tiers.

A dispatch, or a diff with no base, sets both. Every job still runs and only skips its steps, so on a pull
request `ci-gate` sees each job as `success` and treats `skipped` as a failure; off pull requests `skipped`
passes (`docker-check` is PR-only). `ci-gate` needs `changes`, `fast-tests`,
`integration-tests`, `docker-check`, `go-lint`, `go-race`, `go-nonrace`, `go-build`, `go-xos` and
`go-fixtures`. `go-fixtures` keeps its own detection: its inputs add the Java replay modules
(`swath-replay`, `swath-core`, `swath-model`), the shared Gradle build files and wrapper, and the provider
evidence archive and expectations under `docs/` ([replay.md](replay.md#continuous-integration)).

### Go jobs per push

| Job | Runs | Runner |
|---|---|---|
| `go-lint` | `gofmt -l .` (must be empty), `go vet ./...`, `go mod tidy -diff` (`go.mod` and `go.sum` match the imports), `scripts/ci/go-race-shards.sh --check` | ubuntu-latest |
| `go-race (SHARD)` | `go test -race -count=1 -timeout 20m -json` on one shard, one leg per shard below; the JSON stream is the `go-race-SHARD` artifact. Without a Go change, a single `go-race (off)` leg whose steps skip | per shard |
| `go-nonrace` | the tests that measure the process and must not run under `-race`: `SWATH_HEAP_GATES=1 go test -count=1 -timeout 15m -run '^TestWriterAdmissionCoversHeap$' ./internal/listing` and `go test -count=1 -timeout 15m -run '^TestShapes$' ./internal/shapes` | ubuntu-latest |
| `go-build` | `CGO_ENABLED=0` builds (`./build.sh`) of `swath` and `swath-replay` for linux/amd64, linux/arm64, darwin/arm64 and windows/amd64, each linux/amd64 `--version` naming the commit; `GOOS=windows go test -c ./internal/journal`; the Go replay image and its smokes ([replay.md](replay.md#continuous-integration)) | ubuntu-latest |
| `go-xos (OS)` | `go vet ./...` and `go test -count=1 -timeout 25m ./...` without `-race` (slow on macOS; the Windows race detector needs cgo) | macos-latest (arm64), windows-latest (amd64) |
| `go-fixtures` | `./gradlew :swath-replay:installDist`, `bench/replay/make-fixtures.sh`, then the gated tests named in the step's `FIXTURE_TESTS` with `SWATH_REPLAY_FIXTURES` set; a named test that skips or does not run fails the job | ubuntu-latest |

Every job above sets up Go through [`.github/actions/setup-go`](../../.github/actions/setup-go/action.yml): the
version `go.mod` names, with the module and build caches restored from the job's newest cache, keyed per job
(and race shard), runner OS and architecture, `go.sum` and commit. Only a push to `main` saves, under its
commit's key, so parallel jobs never overwrite one another's caches with partial ones and a pull request
restores `main`'s newest without saving. The DuckDB downloads (`integration-tests`, the dispatch-only
container smoke) retry five times, and the SeekParquet conformance step's `pip install` ten.

No per-push job races `internal/listing` or `cmd/swath` on linux/amd64 (`go-nonrace` runs one `listing` test there, without `-race`): they are raced on arm64 per push and on
amd64 nightly, and run without `-race` on macOS and Windows.

### Race shards

The race shards are the table in [`scripts/ci/go-race-shards.sh`](../../scripts/ci/go-race-shards.sh); the
`changes` job expands it into the `go-race` matrix, so a shard is added there and nowhere else:

| shard | `go test -race` arguments | runner |
|---|---|---|
| `crash` | `-run '^TestCrashMatrix$' ./internal/listing` | ubuntu-24.04-arm |
| `durable` | `-run '^(TestDurable.*\|TestDiskFaultMatrix)$' ./internal/listing` | ubuntu-24.04-arm |
| `listing-rest` | `-skip` the two patterns above, `./internal/listing` | ubuntu-24.04-arm |
| `cmd-kill` | `-run '^(TestSIGKILL.*\|TestForcedExit.*)$' ./cmd/swath` | ubuntu-24.04-arm |
| `cmd` | `-skip` the pattern above, `./cmd/swath` | ubuntu-24.04-arm |
| `rest` | every package no row names (a new package lands here) | ubuntu-latest |

A package split across rows has a remainder row that `-skip`s the union of its other rows' `-run`
patterns, so the split is complete and disjoint by construction. `--check` (in `go-lint`) lists the
packages and tests, built with `-race` as the shards build them, and fails when a row names a package that
does not exist, a package is in no shard, a split package has no remainder row or two, a row selects no
test, or a test is selected twice. A new package needs no edit; a new `-run` row needs its package's
remainder to stay nonempty.

The heavy packages race on arm64 only: its weaker memory model makes it the better race target, and the same
`-race` suite took about 30 % less time there than on amd64 (`internal/listing` 490 s against 700 s). The
nightly `go-race-nightly` covers amd64.

### Cross-OS

`go-xos` runs the default tier on macOS and Windows. Where a platform cannot run a case the test skips with
the platform behavior as its reason, never silently:

- **Windows** refuses journaled output and `swath resume` (`listing.ErrUnenforced`, no directory sync; #295
  tracks a write-through mode), so tests of journaled runs, crash recovery, resume and the durable spill call
  `datasettest.RequireJournal` and skip: `TestCrashMatrix`, the `listing` `TestDurable*` matrices among them.
  The `order` durable-spill unit tests open the cache directly, with no journaled run in between, and run on
  Windows too. Their directory listings go through the `os.Root`, never `root.FS()`, whose slash-only paths
  reject `order.DurableDir` there (#301). A test that checks a directory is empty lists it through a helper that
  fails on a listing error (`readDir`, `durableEntries`), so a failed listing never passes as an empty one. `TestDurableLedgerFailureEvicts` alone skips on Windows, for its
  timing premise (#309), not for anything the platform does. The
  process-kill tests (`cmd/swath` `sigkill_test.go`, `forcedexit_test.go`, `segments_durable_test.go`,
  `durable_campaign_test.go`) are not built there (`//go:build !windows`). Replay fixtures that only need a
  dataset list it unjournaled. Windows also refuses to rename a directory holding an open file (a running
  output cannot be replaced, the sort-fixture replacement cases), resets a connection closed with unread
  input (the replay diff's residual cases), has no permission bit that hides a directory, refuses `<` and
  `>` in names, needs privileges for symbolic links, has no SIGTERM to send, and its monotonic clock ticks
  about every 0.5 ms (a local request can time at 0 ns, which the timing checks allow).
- **macOS and Windows** refuse names that are not UTF-8 (APFS, NTFS): the byte-for-byte name cases run on
  Linux only.

macOS runs the crash, durable and kill matrices without `-race`.

### Nightly

`nightly.yml` runs at 07:00 UTC on `main` with no path filter, so it also backstops the change detection.
A dispatch (`gh workflow run nightly.yml --ref BRANCH -f jobs=JOB`) takes `jobs`: `all` (the default),
`deep-and-perf`, `osv-scan`, `go-race-nightly` or `go-shapes`.

| Job | Runs | On dispatch |
|---|---|---|
| `go-race-nightly` | `go test -race -count=1 -timeout 40m -json ./...`; the stream is the `go-race-nightly` artifact | `all`, `go-race-nightly` |
| `go-shapes` | `go run ./bench/shapes -out shapes.json -scratch DIR -baseline bench/baselines/shapes.json`; the summary is the `go-shapes-summary` artifact ([shape suite](#shape-suite)) | `all`, `go-shapes` |
| `deep-and-perf`, `osv-scan` | the Java tiers and the supply-chain scan ([docs/ops/dev/TESTING.md](../../docs/ops/dev/TESTING.md#when-ci-runs-each-tier)) | `all`, or their own name |

There is no weekly Go job, no Go fuzzing, `SWATH_PERF` or `govulncheck` run, and no Go dependency scan in CI.

### Stress repeats

`go-stress.yml` repeats a test on a runner instead of a development host. The dispatch ref is the code under
test:

```bash
gh workflow run go-stress.yml --ref BRANCH -f package=./internal/listing -f run='^TestX$' \
  -f count=50 -f race=true -f gomaxprocs=1 -f os=ubuntu-24.04-arm
```

| Input | Default | Meaning |
|---|---|---|
| `package` | `./...` | package pattern, relative to `go/` |
| `run` | (required) | the `-run` pattern |
| `count` | `50` | `-count` |
| `race` | `true` | `-race`; ignored on Windows |
| `gomaxprocs` | empty | `GOMAXPROCS`, unset when empty |
| `cpu` | empty | `-cpu` list (each test runs once per value, e.g. `1,2,4`), not passed when empty |
| `shuffle` | `false` | `-shuffle=on`; the seed is in the output, so `-shuffle=SEED` replays an order |
| `os` | `ubuntu-latest` | `ubuntu-latest`, `ubuntu-24.04-arm`, `macos-latest`, `windows-latest` |

It runs `go test -run RUN -count N -timeout 60m -json` in a 75-minute job, writes `run RUN x N on OS: P passed,
F failed` and the failing test names to the step summary, and keeps the JSON stream as the
`go-stress-OS-ARCH` artifact. Stress takes a package pattern and a `-run` pattern, not a race shard: to repeat
a shard, pass its row's arguments from [`scripts/ci/go-race-shards.sh`](../../scripts/ci/go-race-shards.sh).
A long test at a high count can exceed the job's limit: split the count across several dispatches. To cite a stress run in a pull request, link each run and give its inputs and its tally,
before and after the change when it fixes a flake, as #300 does.

### Release

`release.yml` runs on a `v*` tag and is Java-only: `source` (release guard self-tests, tag SHA and `main`
ancestry), `fast-tests` (`./gradlew verifyReleaseVersion build -PnoIntegration ...` and the license gate),
`integration-tests`, `deep-tests`, then `publish` (images smoked by digest, assets signed and verified), which runs
only when the repository is public and the `PUBLIC_RELEASE_ENABLED` variable is `true`. It
builds and tests nothing under `go/`; the Go release pipeline is #284.

## Flakes

**Policy.** A per-push test asserts structure (keys, order, bytes, counts, the events a barrier waits for),
not time or heap. Where a timing or memory number is the point, the bound sits about ten times from the
measured value, with the measurement beside it in the test, so it fails on an order-of-magnitude regression
and never on runner variance; the shape suite's bounds follow this rule. A heap bound that `-race` distorts
runs without it, behind `SWATH_HEAP_GATES` in `go-nonrace`; lighter heap checks (`listing` `memory_test.go`,
the default arm of `TestWriterAdmissionCoversHeap`) run in every tier. A test that flakes is fixed in the test: a barrier or a
deterministic assertion replaces the sleep, the spin or the ratio (#276, #291, #296, #300, #306, #319), not a retry or a silently
widened bound. The test runs of `go-race`, `go-nonrace`, `go-xos`, `go-race-nightly` and `go-stress.yml` pass an
explicit `-timeout`, so a hang ends in a goroutine dump; `go-fixtures` and the shard check's `go test -list` do not.

**Triage a red leg.**

1. Open the red `go-race (SHARD)` (or `go-xos (OS)`) leg. Its test step prints the exact `go test` command,
   each package's own output, then the group "output of the failed tests".
2. Download the `go-race-SHARD` artifact for the full `-json` stream: per-test starts, times and passing
   subtests (`jq 'select(.Action == "fail")'`).
3. Rerun the one test locally (`-count=1`, [above](#running-tests-locally)); to measure how often it fails,
   dispatch `go-stress.yml` on the leg's OS and, for a one-core flake, `gomaxprocs=1`.
4. A failure the change did not cause gets an issue naming the run and the test, and a fix that follows the
   policy. A rerun of the failed job proves nothing about the flake.

## Crash, kill and durable-spill matrices

| Method | Where | What it does | When it runs |
|---|---|---|---|
| `TestCrashMatrix` | `internal/listing` | an injected crash at selected occurrences of each `faults.Points` point (S3: 1st, 4th, 12th; GCS: 1st, 3rd, 7th), then resume: byte-identical to an uninterrupted run, row-identical in the arms with time cuts on | `go-race (crash)`; `go-race-nightly`; `go-xos (macos-latest)`; skips on Windows |
| `TestDurable*`, `TestDiskFaultMatrix` | `internal/listing` | the durable spill's kill matrix (torn writes, lost unsynced state), repeated recovery, ledger damage, ENOSPC and EIO at each write and fsync | `go-race (durable)`; nightly; macOS; skip on Windows |
| `TestVersionsDurableSpill`, `TestDurableVersions`, `TestVersionsSegmentsReadDurableFiles` | `internal/listing` | the durable spill with `--all-versions`: kills at a seal, a ledger append, an unlink, a part claim, mid recovery; the segment tail reading durable files | `go-race (listing-rest)` and `(durable)`; nightly; macOS |
| `TestSIGKILL*` | `cmd/swath` | the real process SIGKILLs itself at 13 points, in the durable spill and in the segment tail, then `swath resume` | `go-race (cmd-kill)`; nightly; macOS; not built on Windows |
| `TestForcedExit*` | `cmd/swath` | a run that cannot stop gracefully is forced out after the grace with the right exit code, report and error record | `go-race (cmd-kill)`; nightly; macOS; `forcedexit_test.go` not built on Windows, `TestForcedExitCode` (`forensics_test.go`) skips there |
| `TestSegmentsReplayDurableFiles` | `cmd/swath` | a sorted run whose segments read durable spill files, killed and resumed | `go-race (cmd)`; nightly; macOS; not built on Windows |
| Request-log oracle | `internal/listing` `TestDurableSpillResume`, `TestVersionsDurableSpill`; `cmd/swath` campaign | no data request of a resume starts inside a live certificate's `[max(cs, K), ce)` | in-process: with the tests' shards; campaign: [manual](#durable-spill-campaign-opt-in) |
| Durable spill campaign | `cmd/swath` `TestDurableMinIO`, `TestDurableCorpus` | the kill schedule through the real binary on MinIO and production key lists | manual; skips without its endpoint |

Each test's arms are in the [invariant table](#invariant-tests) and the [§6 coverage table](#durable-spill-campaign-opt-in).

## Opt-in tiers

Each gate is an environment variable a test reads; without it the test runs a smaller arm or skips. CI sets
only the two named.

| Gate | Enables | Where | CI |
|---|---|---|---|
| `SWATH_HEAP_GATES=1` | the full arms of `TestWriterAdmissionCoversHeap` (four S3 writers and one, 24 pages each; four local writers at their declared widths); by default one S3 writer over six pages | `internal/listing` | `go-nonrace` |
| `SWATH_REPLAY_FIXTURES=ROOT` | the Java-written fixture tier and the evidence replays; `TestRecordings` when recordings exist | `internal/replay/store`, `evidence`, `azure` | `go-fixtures` (not `TestRecordings`) |
| `SWATH_PERF=1` | the full-scale arm of a law that holds at a small scale (`TestAdmissionFlat`: the million-key list) | `internal/source/keyrange` | never |
| `SWATH_REPLAY_LOAD=1` | the replay server's load tests (`TestLoad*`) | `internal/replay/server` | never |
| `SWATH_REPLAY_JAVA=PATH` | the differential tests against the Java replay server | `bench/replaydiff` | never |
| `SWATH_MINIO_ENDPOINT`, `SWATH_CORPUS_ENDPOINT` | the durable spill campaign | `cmd/swath` | never |
| `SWATH_GROWTH=1` | the 10,000-part arm of `TestPartCountGrowth` | `internal/listing` | never |
| `SWATH_CLIENT_COST=N` | the client-cost measurement `TestListClientCost` (Linux) | `internal/source/gcs` | never |
| `SWATH_VERIFY_DIR=DIR` | `TestVerifyThroughput` | `internal/listing` | never |
| `SWATH_VERIFIER_FIXTURES=DIR` | `TestVerifierFixtures`, run by `test_dataset_readers.py` | `internal/listing` | never |
| `UPDATE=1` | rewrites the goldens: `docs/cli.md`'s flag block, `docs/internals/counters.md`, the report schema | `cmd/swath`, `internal/source/s3`, `internal/listing` | never; the comparisons run in the default suite |
| `-fuzz`, `-bench` | the fuzz targets and benchmarks | [fuzzing](#fuzzing) | never; seed corpora and `testdata/fuzz` run as tests in the default suite |

The heap tier exists because the full arms allocate hundreds of MiB and measure the live heap, which `-race`
distorts and slows (over three of the listing package's six and a half minutes under `-race` at two
processors).

## Live runs

No workflow lists a Go run against a real bucket. Live listings, production-corpus sweeps and recordings are
launched by hand on cloud batch hosts from the maintainers' tooling, never on a schedule; results that are
kept go to [measurements.md](measurements.md) with the run, date and command. The replay server's live matrix
is recorded once and replayed offline ([replay server oracle](#replay-server-oracle)). The one automatic
real-bucket listing in CI is the Java image smoke, `scripts/ci/smoke-container.sh`, an anonymous listing of a
small public prefix in `ci.yml` `docker-publish` and `release.yml` `publish`.

## Invariant tests

| Invariant ([internals/contracts.md](internals/contracts.md)) | Test |
|---|---|
| hostile keyspaces | `source/keyrange` `TestHostileFixturesMatchSequential`: the cardinality bomb (6,000 keys after a 200-byte shared run, plus neighbors of the prefix) and the encoding minefield (percent-encoding traps, `+`, spaces, `?#&=`, control characters, NBSP, BOM, U+FFFD, non-BMP, combining marks, long runs of one byte or scalar) list row-identical to the sequential oracle, parallel and seeded, and their ranges tile the keyspace |
| metamorphic laws | `source/keyrange` `TestMetamorphicLaws`: one table over every fixture and hostile keyspace; the listing is invariant to the page size (pages capped at 3, 7, 13, random short pages), the range decomposition (seed on and off, the seed's pagination and descent at pages of 3, 1, 4, 8, 16, 64 workers, forced random splits, a seeded resume), the memory budget (20 rows to 1 M rows) |
| the seed | `source/keyrange` `TestSeedDescent`: the waves, requests, levels, folders per level and stop reason on the single-root date layout (5 waves, 16 requests, 4 levels to 336 days), a 2,300-folder top level (3 pages), 95 % of the keys under the last folder (one level; no tile over 3/(4·workers) of the keys, an empty open frontier), one folder per level 100 deep (the level budget), top-level objects (the round-trip budget) and the page cap; `TestSeedListsEveryKey`: every shape, fixture and hostile keyspace row-identical, and the seed's tiles contiguous with cuts strictly ascending, safe, real folders, inside the scope; `TestSeedResume`: a resumed scope tiled with every cut above K, row-identical under random splits, on the S3-shaped and the inclusive-start providers; `TestSeedResumeAfterCrash`: seeded runs killed at random splits and range ends (`faults.At`), resumed with the seed, row-identical to the keyspace; `TestSeedChargesEmptyPages` (empty continuing pages before the first folder are seed requests and round trips: 10 on the top level stop at 6 round trips, 40 folders behind one each stop at 64 requests; `seed.requests` equals the delimiter requests sent), `TestSeedFailurePublishesCounters` (a failing or canceled second wave still reports its requests, round trips, pages and `seed.stop.failed`) |
| dataset format contract | `listing` `TestDatasetContract`: multi-part S3 datasets (sequential, parallel, seeded) and a local one with a non-UTF-8 name, read from the manifest alone: parts are exactly `data/` in name order, bytes and MD5 match, footer rows and exact base64 bounds match, keys rise strictly within and across parts, footers carry exactly the four `swath.*` stamps and `sorting_columns` key ascending, and the three S3 runs hold the same keys |
| versioned listing, every path | `source/keyrange` `TestGroupsEveryRowOnceInOrder`: every version row once, in wire order within its key and byte order across keys, through both `order.Run` and `order.Count`; pages of 1, 2, 3, 7 and 50, full or random short pages with empty continuations, three random seeds, inclusive and exclusive starts, sequential, forced and demand splits, delimiter seed with structure probes, row-weighted warm starts (including a cut at the hot key), latency-ranked splits, resume after ordinary and hot keys, and a prefix scope; a slow-folder arm requires an actual slow-range split and group buffering |
| version groups and split ownership | `source/keyrange` `TestGroupsTake` (page-by-page hold-back transitions, hot keys and empty pages), `TestGroupsSplitVetoAndRelist` (an open group vetoes a split; discarded held rows are relisted exactly once), `TestGroupsEmittedRowsOwnTheirStorage` (emitted rows release the original page arena and excluded tail headers), `TestGroupsNeedDetach` (groups require a detach function), `TestGroupsMaximumIncludesMiddleGroup`, `TestGroupMaximumAllEmissionPaths` (maximum group rows include interior groups, sequential and seed EOF), `TestGroupOverfetchRows` (rows beyond the upper key bound are counted and trimmed) |
| version token cycles and raw progress | `source/keyrange` `TestGroupsTokenCycle` (periods 2, 3 and 100 fail in range and sequential chains), `TestGroupsRejectSuppressedTokenCycle` (cycles fail even below the resume bound), `TestGroupRawProgressBounded` (repeated suppressed rows with fresh tokens eventually stop advancing the frontier), `TestGroupRawSuppressedOrder` (descending suppressed keys still fail), `TestGroupsOutOfOrder` (descending keys or an equal key at a closed seam fail) |
| version part boundaries | `parquetout` `TestGroupedRowCutsWaitForTheGroup`, `TestGroupedIntervalWaitsForTheGroup` (row, size and interval cuts wait for the next key; no group spans parts), `TestDeferredRowCutCalibratesBySize`, `TestDeferredTimerCutPromotedBySize` (a deferred row or time cut reaching the size target calibrates as a size cut, with the cut and deferred-row counters), `TestGroupedPinnedAmbiguousTail` (a pinned claim reproduces byte for byte at a group boundary; a grown tail fails with `ErrAmbiguousTail` before publication), `TestGroupedWriterAcceptsRepeatsNotDescents`; `listing` `TestVersionsTailSeamsAtKeyChanges` (a drained tail's 5-row cuts inside hot groups move to the next key change), `TestVersionsSegmentsAtKeyChanges` (the same on the segment-parallel tail, engaged from memory: byte-identical to the one-encoder run), `TestVersionsSegmentsReadDurableFiles` (60,000 keys with hot groups, journaled, the frontier held until every other range is done and a certificate sealed: the tail engages two workers or more that read durable files, with boundaries inside their frames and deferred to key changes; byte-identical to a serial run from memory); `order` `TestSegmentsCutAtKeyChanges` (the planner moves boundaries to key changes; the parts the serial drain's), `TestSegmentsSeamInsideAGroup` (a seam inside a group is refused) |
| S3 version markers and parser captures | `source/s3` `TestVersionsRejectMissingVersionMarker` (a truncated data page cannot skip a key's remaining versions), `TestRejectUnchangedMarkersOnKeyAdvance` (an unchanged marker pair fails even if returned keys advance), `TestVersionsFakeClientEndToEnd` (both markers, a prefix-only continuation and all rows in wire order), `TestVersionsFakeClientRefusals` (invalid markers and the versions permission error); `source/s3/client` `TestParseVersionsCaptures` (public captures against `encoding/xml`: versions and delete markers interleaved, literal null IDs, equal-time ties, URL encoding and common prefixes; requests and owner substitutions in `testdata/versions/PROVENANCE`), `TestParseVersionsMissingVersionMarker`, `TestParseVersionsInvalidPage`, `TestEveryVersionsTruncationFails`, `TestListObjectVersionsRequest`, `TestListObjectVersionsFailures` |
| GCS generation order and successor start | `source/gcs` `TestGenerationsRejectDescendingPageSeam`, `TestGenerationsSeamsAndEmptyPages`, `TestFetchGenerationsWithinPageOrder` (generations rise strictly for one key within and across pages, including an empty continuation, sequential and parallel), `TestFetchGenerationsResumeStart` (resume uses the key's NUL successor; a 1,024-byte key uses an inclusive start and drops its generations); `source/gcs/client` `TestVersionsRequest`, `TestVersionsCaptureParses` (versions selector and captured generation metadata) |
| version resume, prior weights and reports | `listing` `TestVersionsResumeAfterCrashDuringHotGroup` (only complete groups durable; resume reproduces clean parts and reports retained versus new rows), `TestResumeRefusesVersionModeSwitch` (current/all-versions schema changes refused both ways), `TestDatasetPriorCoalescesFinalVersionGroup` (a final group crossing row groups contributes its full mass), `TestVersionReportIncludesAllS3Counters`, `TestVersionReportS3CountsAndFilters` (count and dataset modes, filters, zero-size versions and delete markers), `TestVersionReportGCS` (noncurrent generations), `TestVersionReportCurrentAbsent`, `TestVersionReportEmptyCountsPresent` (mode-specific fields absent for current objects, zero counts present for empty version runs); `textout` `TestVersionsTableIncludesBooleans`, `TestVersionsTableFixtures` (exact S3 and GCS version table rows) |
| independent dataset readers (manual tier) | `test_dataset_readers.py` `WholeRows.test_sequences_multiplicity_metadata_and_nulls`, `DuckDBComparison.test_actual_sql_on_both_schemas` (S3 versions and GCS generations: ordered whole-row sequences and multisets preserve duplicates, nulls and metadata), `VerifierSchema.test_complete_gcs_contract_in_every_part`, `test_verifier_rejects_missing_generation_column`, `test_time_deleted_requires_utc_microseconds`, `test_verifier_runs_from_any_working_directory`; `listing` `TestVerifierFixtures` generates the real Parquet fixtures on request; see `verify.py` below |
| no gap, no overlap; boundary to the left | `source/keyrange` T1 (random splits cover every key once, traced intervals tile the keyspace) |
| safe pivots | `source/keyrange` T2 (pivot properties over adversarial bounds) |
| guards | `source/keyrange` T3 (every clause) |
| paging, trimming, truncation errors | `source/keyrange` T4; `source/s3` `TestOutOfOrderListingFails` |
| the S3 adapter's requests | `source/s3` `TestRequests`: the scope prefix, a resume's start-after, continuations without it, 1,000-key pages, the seed's delimiter listing in its first wave (beside the first data page), a cut at a folder whose name holds `%` sent as start-after (`seed-percent`); the engine's split decisions are covered by its laws (T1, resume, sorted output), not pinned by a hash |
| split balance (synthetic) | `source/keyrange` T6 (`p/%07d`, 200k keys, 20 ms pages, 16 workers) |
| pivot policy (synthetic) | `source/keyrange` `TestPivotPolicyEngages`: date-partitioned, nbm-like, flat and hashed keys (about 500 pages of 100 keys, 1 ms, 64 workers) keep `split.max_depth` within 4·log2(pages)+8, with the pivot-source and engagement counters naming the paths; `TestPrefixedFrontierSplits` (a scope prefix: the frontier still splits); `TestReflectionCeiling`, `TestGallopStride`, `TestGenerationAndAnchor`, `TestSliverRefused` (refused, not unsplittable: it decides again after each page); `TestInvalidScopeCeiling` (scope `ÿ`, byte ceiling `c3 c0`: a one-key first page is refused, not made unsplittable, and the next page splits inside the scope), `TestInvalidAnchor`; `FuzzPropose` (any proposed pivot is strictly inside its bounds, valid, Safe, at most 1,024 bytes; seeded from the fixture and hostile keyspaces); `TestStructureEngages`: with structure probes on their evidence (the command's default), date and nbm-like keep `split.max_depth` within 3·log2(pages)+8 (about 19 and 13), pivoting on folder boundaries; flat keys send at most 8 probes and none on stall evidence, hashed keys none; a split never waits for a probe, so the timed runs require the structural path or, when every probe was late (`structure.late`), the anchored reflection, and an inline arm (`probeInline`: each probe lands before the next decision) requires the structural path; `TestStructureSuppressionInherited` (children inherit the count of probes that found no folder; asserted with the probes inline, since whether a probe lands before its range splits is a race the engine never waits on), `TestStructureLevels`, `TestStructureFlag`. The metamorphic laws gain `w64/structure-on-frontier` and `w8/structure-auto/forced-splits/page=7`, T1 and `TestResumeWithSplitKills` run a share of their seeds with structure on its evidence |
| split admission | `source/keyrange` `TestGaugeEstimates` (the gauge reads a run in its own alphabet, a pivot past the page's digits as their end, and +∞ for a bound in another folder); `TestAdmissionEngages` (each gate refuses on the synthetic shapes at 64 workers, the listing is the keyspace and the ranges tile it); `TestAdmissionRedecides` (a refusal declines one page's split, not the range's); `TestAdmissionFlat` (a flat list of 1,000 pages ends with fewer than 360 ranges, within the depth bound and about 64 wide: 100,000 keys in pages of 100 by default, the million-key list in pages of 1,000 with `SWATH_PERF=1`) |
| count-only and retained bounds | `order` `TestCountCountsEveryRow`, `TestCountGuardsEachRange` (a range's own keys repeating or descending, or at or below the resume key, fail the count; overlapping ranges are not detected, by design), `TestCountStops`, `TestCountStopsOnEmptyBatches` (after a stop an empty `Rows` batch returns the cause), `TestDistinctCountsOnlyHandedRows` (distinct keys count only rows handed to the sink: rows validated by `send` but batched when the provider fails are not counted; the rows reach `send` on a range the drainer expands directly, through a child when a worker claims the root, so neither schedule waits on a gauge); `listing` `TestCountOnlyHoldsNothing` (no file under `TMPDIR`, no spill, the order-stage peaks zero, the heap at the pages in flight) and `TestRetainedBoundsDoNotPinPages` (real XML through the production parser behind a held-up first range: the peak live heap stays within the budget and the pages in flight, and fails with the cursor left a view of its page); `source/keyrange` `TestDetach` |
| GCS connections | `gcs/client` `TestTransportPoolSpreadsConnections`: an HTTP/2 TLS server sees exactly `PoolSize(workers)` connections, with equal shares, at 1, 16, 64 and 128 workers |
| order and memory bound | `order` `TestOrderedOutput`, `TestSinkFailureStopsPromptly`, `TestCancellation`, `TestFailedRangeStopsTheStream` (a failed range stops the drainer before it is seen as done: rows of later ranges already listed never reach the sink past the gap its dropped children leave; every run holds the drainer until the workers have expanded both siblings and holds the context's stop callback until the run returns, barriers rather than sleeps, so it fails on every run without the latch), `TestInterruptedRunReportsStall` (a run canceled, or failed by its provider, after 200 ms with nothing handed on reports that stall as `FrontierStuck`), `TestFinalFlushReportsStall` (one row batched until the final flush, a sink blocked until the deadline: the wait is the stall), `TestSegmentsSizedToBudget` (the segment tail with 1,000-byte keys among 220-byte ones, spilled, into stalled encoders, at a budget that holds exactly two workers: two are planned, by `row_budget`, they wait fewer than 16 times and hold within the budget), `TestSegmentsStayWithinBudget` (the same with the plan's share understated: the safety gate alone keeps the encoders' queues within a 4 MiB budget, 19 MB before), `TestRunBytes`, `TestSegmentsReleaseSpanStorage` (a span of a spilled frame is copied: eight segments read 28-row spans of one spilled 1,000-row frame, one after another on the test's goroutine, each decoding its own copy, every batch kept: the live heap grows within 2 MiB, about 7 without the copy), `TestSegmentUnitsDroppedByLastReader` (an in-memory unit read by three segments keeps its rows and tree charge until the last has read it, then drops both, once; no span shares its row array), `TestSegReserveIgnoresTree` (beside a tree at the full budget, reservations that fit the segments' budget are granted at once: the tree is not the segments' to wait for), `TestSegmentsCompletedPartsReleaseStorage` (finished segments keep nothing of their rows: seam keys copied, units dropped by their last reader; about 1,200 parts, spilled and in memory, within two budgets when the last segment finishes); `parquetout` `TestSegmentPastLimitCancels` (a segment refused at the part ceiling stops waiting on cancellation and returns its encoder slot; it hung before), `TestRouterAdmitIsByteIdentical` (encoders admitted mid-run change no part's bytes, and the shared codec makes no more zstd states than encoders admitted, before and after `Admit`), `TestCodecLeasesFollowLimit` (leases held at once make states up to the limit, a lease past it waits, `setLimit` admits more; the overlap is forced, so it holds at GOMAXPROCS 1, and a lease that should wait is observed for 50 ms), `TestLowerEncoderMemIsByteIdentical`, `TestValueLimits` (a value over its column's `MaxBytes` is the typed `schema.ErrValueTooLong`); `listing` `TestWriterAdmissionCoversHeap` (with `SWATH_HEAP_GATES=1`: four writers, 24 pages each, at the S3 schema's longest keys, one alone, and four at the filesystem schema's declared widths, binary, 24 pages each, with six encoders before `Admit`, stay within their admission, `parquetout.EncoderBytes`; 326 MiB against 96 before the shared codec) |
| deterministic output | `parquetout` `TestRouterIsDeterministicAcrossEncoders`, `TestColumnParallelEncodingIsByteIdentical`; `TestFooterOnlyChange` pins page, index and footer digests on every platform, with a golden for each of parquet-go's two dictionary-index encoder paths (amd64 with AVX2; everything else) |
| publication order and gate | `dataset` `TestCheck`, `TestPublishFailureLeavesNoManifest`; `listing` cancellation and encoder-failure tests |
| crash and resume (I1, I5, I6) | `listing` `TestCrashMatrix`, for S3 and GCS (a fake JSON API, `gcs/gcstest`): an injected crash (no cleanup, as a kill) at every point in `faults.Points` (split, range done, part closed, record written, rename, every part published, manifest installed), at its 1st, 4th and 12th occurrence (GCS: 1st, 3rd, 7th; spill on and off for S3; a first occurrence never reached fails the test, except the points not on a provider's path, later ones skip), and the first part's journal append (`journal-append#2`: the calibrating part claimed, unrecorded), then resume (the durable spill's points, `faults.SpillPoints` and `faults.RecoveryPoints`, have their own matrices on a fixture that reaches them, failing if one is not reached: `TestDurableKillMatrix` with torn writes and lost unsynced state, `TestDurableRepeatedRecovery`, `TestSIGKILLDurableSpill`): the dataset is byte-identical to an uninterrupted run (row-identical in the arms with the part interval on, whose time cuts depend on timing). `cmd/swath` `TestSIGKILLAtEveryPoint`, for S3 and GCS (the real process SIGKILLs itself at 13 points, including between a rename and its directory sync, before the manifest and after it, and at `journal-append:2`; every first occurrence must be reached except `split` in the seeded arms, whose ranges never split (S3 runs seeded and unseeded, the unseeded arm crossing it); byte-identical with `--part-interval=0`, since a 30 s time cut under load moves part boundaries, and row-identical in a 1 ms interval arm), `TestSegmentsReplayDurableFiles` (a journaled sorted run whose segments read durable spill files, segment boundaries inside their frames; killed after a seal, after a ledger append, and in a resume's recovery replay, each resumed: rows reconcile, fetched − filtered + replayed = emitted, and the dataset is the clean run's byte for byte; a resume that reuses certificates engages segments only once the drainer has passed its last replay range; with the offset reader's durable path disabled the clean run fails), `TestSIGKILLInSegments` (the real process SIGKILLs itself inside the segment-parallel tail, at `faults.SegmentPoints`: the segments committed and none opened, the 1st and 2nd segment part published with later ones open, every row handed on with the last part held before the router's `Close`; the clean run must report `resources.drain.segments` engaged with two workers or more and every point must be reached, so a fixture that stops reaching the tail fails; GOMAXPROCS 4 in the child, `--encoders 4`, spill on, `--part-interval=0`; the resumed parts are byte-identical and the manifest identical but for its observation times; an arm with the rest in memory, three workers, and one spilled, whose segments read spill frames), `TestGCSOutageThenResume` (503 to every request from three quarters through a run: exit 75, resumable, then `swath resume` byte-identical), `TestSIGKILLResume`: a real process killed with SIGKILL after 4 parts, resumed with `swath resume` (a frozen flag on resume is a usage error, exit 2). `TestCalibratingClaimRecoversRepeatedly`: the calibrating part claimed and unrecorded, the resume killed again (before and at the reproduction's claim, after the next record, and inside recovery while syncing the suspect directory it was moved to), a second resume byte-identical; `parquetout` `TestPinnedCalibratingPartCalibrates`. `TestResumeChecksIdentity`: typed mismatch errors, overwrite needed for a fresh run. `TestGCSResumeRefusesAnotherRoute`: refusal of a GCS resume through another route or billed project. `TestCredentialRotationResumes`: a signed run resumes with rotated keys (credentials are not identity), rotated keys with another region are refused; `TestGCSCredentialRotationResumes` the same on GCS (bearer tokens from a rotated authorized-user credential resume; another billed project is refused). `cmd/swath` `TestGCSCappedRunIsIncomplete`: a GCS run capped by `--max-keys` commits with incomplete coverage, exits 1 (`incomplete`, not resumable), keeps no journal, and `swath resume` refuses it unchanged. `listing` `TestDiskFaultMatrix`: a part file, a journal record and the manifest each fail at a write (half its bytes, then `ENOSPC`) and at its fsync (`EIO`): the run fails without a manifest, published parts stay, and a resume is byte-identical. `TestWatchdogWriteInFlightAtRelease`: a watchdog write under way at the end of a run keeps the lock held and the run waiting; a later write is refused. `TestClaimedPartKeepsItsColumns`: a GCS run by a build without `time_created` stopped with part 1 claimed (at `journal-append`, and as a torn record) is resumed by a build with it: part 1 is reproduced byte for byte with the claimed part's columns, the later parts carry the column (`parquetout` `TestCutReproducesClaimedFooter`: the claimed part's columns and `created_by` are each needed) |
| compositions | `listing` `TestResumeChangesPriorWorkersEncodersAndSpillFormat`: a run seeded from a cut file, spilling delta-coded zstd frames with 8 workers and 2 encoders, killed (after its third record, or with the calibrating part claimed), resumed with the prior removed or stale, 3 workers, 1 encoder and plain uncompressed frames: byte-identical to the unseeded clean run. `TestPriorSlowSplitPercentThrottleCompose`: a cut-file prior over folders named with `%`, slow-split on, one slow folder and a 503 SlowDown every seventh request: every key once, in order, with tiles from the prior, `%` bounds sent and throttles met (the slow-range reason itself does not fire on that shape). `TestTextFormattersShareTheSpillingBudget`: four formatters on the 128 KiB budget the spilling order stage shares: the file equals the one-formatter run's; a write failure is the run's error; a cancellation with spill live is the cancellation; the destination untouched and nothing left beside it |
| resume after K (I4) | `source/keyrange` `TestResumeAfterKey`: random keyspaces, split coins and K; the resume lists exactly the keys after K and its ranges tile (K, ⊤]. `TestResumeWithSplitKills`: K inside a date folder, also a `%` and a control-character key (listed from K's safe prefix), killed at random split points (`faults` "split") and resumed after the last key that reached the sink until done: exactly the keys after K, once each |
| spill (step 3) | `order` `TestSpillKeepsWorkersBusy` (slow dense frontier, 56 fast siblings, 220-byte keys, 8 workers, 500-row budget; the frontier is held until every sibling has finished emitting: identical output, buffered rows within budget + one batch per worker + the frontier path, every sibling completes behind the held frontier (spill keeps them progressing; a liveness property, not a speed figure), no files left; plain and zstd), `TestSpillCleanedOnFailure`; `TestSpillDiskBudget` (a cap at a quarter of the uncapped peak holds with identical output; a reserve larger than the disk spills nothing); frontier and budget waits are nonzero without spill; frame formats: `order` `TestDeltaFramesRoundTrip` (every producer's random sorted rows through delta, delta+zstd, plain and plain+zstd frames: arbitrary byte, empty and mutually prefixing keys, null fields, uppercase, quoted, short and multipart ETags, times across zero and int64 overflow; the reader's buffers are overwritten after each frame, so a row still viewing them fails), `TestDeltaFrameMalformed` (prefix expansion past the declared storage, storage above any written, prefix past the previous key, suffix past the frame, trailing bytes, foreign payload coding or frame type, a zstd bomb: refused before allocating, decode-cap refusals counted), `s3` `TestDeltaCodecForms`, `listing` `TestSpillFormatsIdentical` (spill forced in every format: Parquet byte-identical to no spill), `BenchmarkSpillFormats` (bytes, CPU and allocations per row) |
| summary semantics | `listing` `TestSummaryDistinguishesOffFromZero`: a disabled policy's counters are absent and its feature reads false; an enabled one reports zeros |
| attempt latency percentiles | `source/objhttp` `TestLatencyBuckets` (the finite buckets tile 0 to 2^40−1 ns, each at most a quarter of its lower edge wide), `TestLatencyQuantiles`, `TestLatencySaturation` (2^40−1 ns reports the last finite edge and no overflow; 2^40 ns and a 20-minute attempt count as `inflight.latency_overflow`, and a percentile among them reads open, 9223372036854775807) |
| layering | `source` `TestImportBoundary`: the pipeline packages import nothing under `internal/source/` (transitively), `internal/source` imports no provider, `cmd/swath` imports providers only with `_` |
| journal | `journal` `TestJournalRoundTripAndTornTail`, `TestCheck` (identity table: pipeline fields, and source fields added, dropped, changed or respelled) |
| overwrite safety | `dataset` `TestPrepareNeverDeletesTheTree`, `TestReplaceRemovesOnlyOwnEntries` |
| command surface and report schema | `cmd/swath` `TestCLIDocFlags` (docs/cli.md's flag block is the generated `--help` of `swath list`, `swath count` and `swath diag list`), `TestHelpGroupsCoverFlags` (every flag in exactly one help group; the Diagnostics group is exactly the flags `swath list` refuses), `TestMovedFlagsNameDiag` (each flag moved to `swath diag` is refused on `swath list` and `swath count` with exit 2 naming `swath diag`, accepted by `swath diag list`), `TestCountRefusesOutputFlags`, `TestCount`, `TestOldNamesRejected` (the names of the builds of `main` before 0.4 and Java's, `--output`, `--force`, `--endpoint-url`, `--quiet`, `--spill-max`, `--count-only`, and `--concurrency`, are unknown flags, exit 2), `TestEveryHelpExitsZero`; `listing` `TestReportSchemaGolden` (`testdata/report-schema.golden`: every JSON path of the run report), `TestReportSchemaDoc` (docs/cli.md's "Run report" names every field), `TestHistoryBounded` |

## Oracles

- **Filesystem:** a sorted `filepath.WalkDir` (`TestListingEqualsSortedOracle`), exact sequence, both
  engines, every tier, with and without DIR rows, reorder budgets from 1 to unbounded, tricky names.
- **S3 parallel vs sequential:** the parallel core's dataset is byte-identical to the sequential
  continuation-token listing (`listing` `TestS3ParallelEqualsSequential`, on gofakes3).
- **S3 client vs the AWS SDK:** `internal/source/s3/sdkoracle` puts the SDK behind the provider's `API`.
  `TestClientPagesMatchSDK` sends the same requests through both and compares every page field;
  `TestRowsMatchSDK` compares rows, sequential and parallel; `listing` `TestDatasetsMatchSDK` requires
  byte-identical datasets (SDK sequential vs client sequential, parallel and seeded, and SDK parallel) on a
  bucket with spaces, `+`, `%`, XML metacharacters, non-ASCII and 900-byte keys. Credentials and signing
  are the SDK's own modules; `TestCredentialChain` checks the chain's use (environment, profile,
  anonymous, a missing profile); `TestCredentialsFailClosed` that a configured source that is slow, mounted
  late, refused or down never becomes anonymous.
- **Page parser vs encoding/xml:** `FuzzParseList` (arbitrary documents) and `FuzzParseFields` (valid
  documents from fuzzed values) require that whenever encoding/xml accepts a document, the scanner returns the
  same page. `TestEveryTruncationFails` holds that no proper prefix of a page parses, so a cut body is never
  taken for a shorter page; `TestParseFailuresAreRetried` that such a body is retried and counted. The
  scanner reads the whole body before parsing, so network chunk boundaries never fall inside a token.
- **GCS client vs Google's JSON API client:** `TestClientPagesMatchGoogleClient` sends the same requests
  through the hand-rolled `objects.list` call and `google.golang.org/api/storage/v1` (a test dependency;
  the binary does not link it) to a fake JSON server and compares every field the schema keeps (names,
  prefixes, tokens, size, generation, metageneration, etag, update time, creation time, storage class, MD5,
  CRC32C, content type) across prefixes, delimiters, both offsets and keys that need escaping or hold control characters. Our
  client runs twice: on the fake's endpoint, and addressed to Google's endpoint (redirected to the fake), where
  it sends the production `fields=` selector, which the fake applies while the Google client reads full
  responses; `TestRowsCarryEveryField` checks the adapter's row mapping. The fake's page tokens are opaque
  keyed hashes bound on the server to their chain's parameters; it can serve empty continuing pages, short
  nonempty pages and a lowered page cap (`TestTokenChainsAndEmptyPages`). `TestEndOffsetOmitted` forces
  bounded ranges ending at 1,024-byte names and requires the omitted `endOffset`, the counter and the trim to
  engage; `TestDelimiterBoundaries` lists a folder marker, its descendants and its byte-order neighbors at
  page size one under every delimiter-sending policy. Credentials are tested against a local token endpoint
  (`internal/source/gcs/client`, `creds_test.go`).
- **Java swath on replay** (the differential oracle): see "Replay oracle" below.

## Replay oracle

The Java tool and its replay server are the reference producer. The Go tool is held to them on recorded
listings: Java-produced recordings are the differential input, and the Go dataset must hold the same rows
(`bench/diffds.py`, all 13 format-1 columns, the format-2 fields projected with the constants of
[upgrade.md](upgrade.md)). That establishes logical value parity, not format compatibility: Java's reader
does not read format 2 (its sorted reader requires the format-1 sort stamps, which format 2 replaced), so a
Go dataset is accepted by the downstream reader and by an independent Parquet reader, DuckDB through
`verify.py` (below; [upgrade.md](upgrade.md#gates)). In a fresh container (JDK 25 is required; the system JDK
will not run the launchers):

```bash
# 1. JDK 25 (Temurin) into its own directory; check the SHA-256 against the Adoptium API.
mkdir -p ~/jdk25 && cd ~/jdk25
curl -LO https://github.com/adoptium/temurin25-binaries/releases/download/jdk-25.0.5%2B8/OpenJDK25U-jdk_x64_linux_hotspot_25.0.5_8.tar.gz
sha256sum OpenJDK25U-jdk_x64_linux_hotspot_25.0.5_8.tar.gz   # 68982529869257d0ee6b824ec5aa270ff8b118bb438670da0b97ebdab178d7fb
tar xzf OpenJDK25U-jdk_x64_linux_hotspot_25.0.5_8.tar.gz && export JAVA_HOME=~/jdk25/jdk-25.0.5+8 PATH=~/jdk25/jdk-25.0.5+8/bin:$PATH

# 2. Build the Java CLI and replay server from this repository (about 75 s with a warm cache).
cd "$(git rev-parse --show-toplevel)"
./gradlew --max-workers=2 -Porg.gradle.java.installations.paths=$JAVA_HOME \
  -Porg.gradle.java.installations.auto-download=false :swath-replay:installDist :swath-cli:installDist
export PATH=$PWD/swath-replay/build/install/swath-replay/bin:$PWD/swath-cli/build/install/swath/bin:$PATH

# 3. Record a bucket with Java (sorted Parquet; anonymous for public buckets).
swath list s3://noaa-normals-pds --region us-east-1 --no-sign-request --concurrency 64 \
  --format parquet --sort -o /data/rec/noaa-normals-pds --report /data/rec/noaa-normals-pds/_swath_summary.json

# 4. Serve the recording (path-style S3, no auth, plain HTTP), optionally with recorded latency.
swath-replay serve --fixture /data/rec/noaa-normals-pds/data --bucket noaa-normals-pds --host 127.0.0.1 \
  --port 19090 --serving-mode sorted --metrics-port 19192 --inject-latency worker_page=95ms &
until curl -sf http://127.0.0.1:19192/healthz >/dev/null; do sleep 1; done

# 5. List it with the Go tool and compare.
cd go && go build -o /tmp/swath ./cmd/swath
/tmp/swath list s3://noaa-normals-pds -endpoint http://127.0.0.1:19090 -path-style -anonymous -o /tmp/go-ds
python3 bench/diffds.py /data/rec/noaa-normals-pds /tmp/go-ds       # identical: true
python3 verify.py /tmp/go-ds                                         # the SeekParquet dataset, read by DuckDB
```

- `bench/diffds.py A B` compares two datasets with DuckDB. It checks:
  - the row sets on the 13 canonical columns, in both directions;
  - that both datasets are strictly ascending by key bytes;
  - the dataset's minimum and maximum key.

  It also prints part counts and sizes side by side. Replay's S3 route renders `LastModified` in
  milliseconds; datasets listed through replay therefore agree at that precision, and `--ts-ms` compares a
  replayed listing with a direct one.
- **Java's reader on Go datasets:** not a gate since format 2. Replay's sorted serving mode validates the
  format-1 sort stamps and refuses a format-2 dataset; the results of that check in
  [measurements.md](measurements.md#output-identity-with-the-java-tool) predate format 2 and are historical.
  Format-2 acceptance is the downstream reader's and `verify.py`'s.
- **Synthetic fixtures** are generated with DuckDB and sorted with `swath-replay sort-fixture --capture DIR
  --output DIR`. A result from such a fixture is synthetic and names its generator. The 1M-key fixture of
  [measurements.md](measurements.md#output-identity-with-the-java-tool) is generated by `bench/gen_fixture_1m.py`.
- **Replay store fixtures:** the replay store's unit tests build every fixture in the test. Its Java-written
  evidence tier (`go/internal/replay/store` `TestJavaWrittenFixtures`, `TestSortsTheAzureEvidenceCorpus`,
  `TestStartupCost`, and `TestRecordings` when `recordings/BUCKET/dataset/data` exist) runs with
  `SWATH_REPLAY_FIXTURES=ROOT` and skips without it. `bench/replay/make-fixtures.sh ROOT/fixtures` writes
  that tree from this checkout with the Java replay distribution on `PATH` and `JAVA_HOME` set: DuckDB
  captures (synthetic 10k and 1M rows, the GCS and Azure evidence corpora), sorted by the Java
  `sort-fixture`. The script fixes every file's modification time, so the fixture identity the test
  compares with the Java server's is reproducible. CI runs this tier, except `TestRecordings`, in the
  `go-fixtures` job ([replay.md](replay.md#continuous-integration)).
  The same variable runs the GCS evidence replay (`go/internal/replay/evidence`, see
  [replay.md](replay.md#the-gcs-evidence-replay)); it needs only the committed archive and uses
  `ROOT/fixtures/evidence-gcs/sorted` when present. The Azure evidence replay (`go/internal/replay/azure`
  `TestAzureEvidenceReplay`) runs on the same variable and sorts `ROOT/fixtures/evidence-azure/capture`
  itself; CI's `go-fixtures` job runs both evidence replays.
- **Policy toggles:** Java's policy layer can be switched off per mechanism with the hidden
  `--engine-toggle NAME=off`. The names are `owner_split`, `density_ewma`, `radix_bands`,
  `structure_probes`, `far_ahead`, `alphabet_pivots`, `reflect`, `confetti_feedback`, `reflect_lift`,
  `fanout_tiling`, `mass_aware_seed` and `rate_anchored_sensing`, plus `readahead=on` and
  `tail_floor=MODE`. The seed is set with `--tune seed.mode=none|shallow`.
- **Run summaries:** `bench/summaries.py PATH...` tabulates Java's `_swath_summary.json` and the Go tool's `run.json` into the same columns (the Java-to-Go
  name mapping lives there).
  Each run is one TSV row, keyed by bucket, version and configuration, with every engagement counter as a
  column.
- **Policy arms on a recording:** `bench/policy_replay.sh ARMS BUCKET LATENCY_MS PORT REPS OUT` serves one
  recording with a fixed page latency and runs the arms listed in `ARMS` (Java flags, or a Go binary and its
  `swath diag list` flags) interleaved, `REPS` times; `bench/policy_table.py OUT` prints wall, requests,
  attempts (the replay server's HTTP count), splits, empty children, overfetched keys, probes and mean active
  ranges per arm.

- **Replay load tests:** `SWATH_REPLAY_LOAD=1` runs the replay server's load tests
  (`go/internal/replay/server` `TestLoad*`: the request ceiling under the fault proxy, the reader-slot bound under
  256 walks, no overrun at 64 walks), a few seconds each; they serve `ROOT/fixtures/synthetic-1m/sorted-rolled`
  when `SWATH_REPLAY_FIXTURES=ROOT` is set, else a generated fixture. `bench/replayload` is the walker the
  [replay measurements](replay.md#measurements) use.

## Replay server oracle

The Go `swath-replay` ([replay.md](replay.md)) is held to the Java one by `bench/replaydiff`: both servers on one
fixture, the same scripted matrix, token walks and seeded random requests to each, and every answer's status,
compared headers and body bytes required equal ([internals/replay.md](internals/replay.md#the-differential-driver);
[internals/replay-java.md](internals/replay-java.md) lists the documented divergences and residuals):

```bash
go build -o /tmp/replaydiff ./bench/replaydiff && ./build.sh -o swath-replay ./cmd/swath-replay
/tmp/replaydiff -java "$(command -v swath-replay)" -go ./swath-replay -fixture /data/sorted -bucket bkt \
  -report /tmp/diff.json
```

Against the providers themselves the check is a recording: the live matrix (provider cases, walks, altered and
invented tokens, the scripted matrix) is sent once through `bench/faultproxy -record` with `replaydiff
-live-record`, and `replaydiff -capture` replays the recording offline as often as needed, judging each answer by
the live comparison and its documented differences ([replay.md](replay.md#the-recorded-live-matrix)). A live run
is only how a recording is made. Recordings of real buckets stay local and are never committed; the tests CI
runs record fake providers in-process (`bench/replaydiff` `TestLiveMatrixRecordedReplaysOffline`,
`TestPinnedDifferencesAreDocumentedInBoth`; `bench/faultproxy` `TestRecordAnnotations`), and the provider
evidence they replay is the owned-bucket archive committed under `docs/`.

## The fake bucket

`bench/s3fake` serves a gofakes3 bucket loaded from a key list:

```bash
go run ./bench/s3fake -keys keys.tsv -bucket bench -addr 127.0.0.1:9000 &   # TSV: key, mtime (µs)
./swath list s3://bench -endpoint http://127.0.0.1:9000 -path-style -o /tmp/ds
```

Fake-bucket numbers are synthetic:
they measure client CPU and coordination, not S3 latency.

`bench/scalefake` (package `internal/source/scalefake`) serves a lazy generated keyspace of up to billions of
keys, or a sorted key file (`-key-file`, one key per line), as S3 ListObjectsV2 and GCS objects.list, with
seek and delimiter fidelity. Every listing request waits `-latency` unless a service behavior is asked for;
each is a flag of the command and a field of `scalefake.Options` for in-process use:

| flag | `Options` | behavior |
|---|---|---|
| `-latency D` | `Latency` | every listing answer waits D |
| `-jitter D` | `Jitter` | plus a uniform draw from [0, D), the same for the same request number and seed |
| `-slow p=D,...` | `Slow` | the longest prefix matching the key a page starts at sets its latency |
| `-first-latency D` | `FirstLatency` | the first listing request waits D instead |
| `-token-latency D`, `-start-latency D` | `TokenLatency`, `StartLatency` | extra wait for requests with, or without, a continuation token |
| `-page-cap N` | `PageCap` | at most N items per page, whatever the request asks |
| `-rate N` | `Rate` | a token bucket of N requests per second (burst N/10): a request past it is throttled at once |
| `-fail503 F` | `Fail503` | a fraction F of listing requests answered 503 (S3 `SlowDown`) after the delay |

```bash
go run ./bench/scalefake -key-file keys.txt -addr 127.0.0.1:19500 -latency 50ms -fail503 0.01 -slow d/000=500ms &
./swath list s3://bkt --endpoint http://127.0.0.1:19500 --path-style --anonymous --region us-east-1 --count-only
curl -s 127.0.0.1:19500/__scalefake/stats   # requests, pages, keys, throttled, in flight
```

**Test fixtures.** Tests build their buckets from two shared packages: `internal/source/s3/s3test` (a
gofakes3 bucket `bkt` whose bodies are the keys: `Backend` populates, `Handler` serves, `Delayed` adds
latency, `Serve` owns the server's lifecycle, `NoCredentials` hides the host's AWS credentials) and
`internal/source/gcs/gcstest` (`Strict`, the objects.list fake with opaque page tokens bound to their query,
empty and short pages and a fields selector, which the GCS provider's protocol tests run against; `Bucket`
and `Handler` serve it for run-level tests). A test that injects faults wraps the handler; protocol oracles
and malformed-response handlers stay in their tests. `keyrangetest` (engine laws), `scalefake` (lazy large
keyspaces, key lists and service behaviors), `shapes` (bucket shapes, below) and `faulttest` (transport disturbance) keep their own purposes.

**Which fake for which job.**

| Fake | Serves | Used by | CI |
|---|---|---|---|
| gofakes3 (`s3test`, `bench/s3fake`) | a real S3 implementation: small buckets with object bodies | protocol, oracle and run-level tests; the `go-build` image smoke | the default suite: `go-race`, `go-xos`, `go-race-nightly` |
| `gcstest` | the GCS JSON API with opaque page tokens bound to their query | GCS protocol and run-level tests, the GCS arms of `TestCrashMatrix` | the default suite |
| `scalefake` | lazy keyspaces and key files up to billions of keys, with service behaviors | the shape suite, `listing` memory and spill-format tests, measurements | the default suite, `go-nonrace`; full scale in `go-shapes` |
| `keyfake` | a sorted production key list as ListObjectsV2 at a fixed page latency | measurements and the durable spill campaign; private, not in this repository | never |

## Fault injection

`internal/source/objhttp/faulttest` is a fault-injecting HTTP proxy for tests and benchmarks (never linked
into the binary). In front of swath-replay or a fake bucket it answers a share of the requests in a time
window, or a range of request numbers (what a test that must see its faults on a loaded host uses), with the throttle answer (503 `SlowDown` for S3, 429 `rateLimitExceeded` for GCS), delays them, or
resets their connection; a rate cap throttles requests beyond N per trailing second. Request n of a run gets
the same faults for the same seed and plan. It records a timeline (100 ms buckets: arrivals, faults, most
requests in flight). A request counts as in flight from its arrival until the proxy releases its answer, so a
client that sends its next request on the previous answer is never seen with two; answers are forwarded
whole, not streamed, and `Plan.MaxBody` bounds what is buffered: a longer upstream body is answered `502` with
`X-Faulttest-Body-Limit` (`TestMaxBodyBoundsTheBuffer`). `bench/faultproxy` serves it:

```bash
go run ./bench/faultproxy -target http://127.0.0.1:19090 -listen 127.0.0.1:19091 \
  -window throttle:10s-25s:0.5 -window latency:0s-:0.2:300ms -timeline storm.tsv &
./swath list s3://BUCKET -endpoint http://127.0.0.1:19091 -path-style -anonymous -o /tmp/ds
kill %1   # writes storm.tsv and prints the totals
```

## verify.py

```bash
cd go
python3 -m pip install duckdb
python3 -m unittest -v test_dataset_readers
python3 verify.py OUT [REFERENCE]
```

`verify.py` and its reader tests are a manual tier; CI and the normal Go test suite do not run the Python
tests. The full reader suite needs DuckDB and Go on `PATH` (it invokes `listing` `TestVerifierFixtures`
to generate real Parquet datasets). Without DuckDB, only the schema-only tests run and the SQL and
verifier tests skip. The tests stay in `go/test_dataset_readers.py` beside `verify.py` and the shared
`dataset_schema.py`, since they cover both the verifier and `bench/diffds.py`.

`python3 /any/path/verify.py OUT [REFERENCE]` works from any working directory; its imports resolve beside
the script. It checks a SeekParquet dataset independently of the Go code: schema dispatch, the manifest's
SeekParquet members only, its key declaration (`unique` false exactly for the versions schemas), the swath
extension's fields and coverage, no `_SUCCESS` or `symlink.txt`, `data/` holds exactly the manifest's parts,
row counts, exact manifest bounds (`min_key_base64`/`max_key_base64`, decoded), each part's MD5 against
`extensions.swath.part_md5`, and the five `seekparquet.*` footer stamps and no other `seekparquet.*` or `swath.*`
entry.
Current-object keys rise strictly; version keys may repeat within a part but parts remain strictly
key-disjoint. S3 versions retain nullable delete-marker fields; GCS schemas are checked column by column
in every part, with generations rising within each key and `time_deleted` in UTC microseconds. An optional
`REFERENCE` compares ordered whole-row sequences and multisets through `bench/diffds.py`, using
`dataset_schema.py` for the shared schema contract.

## SeekParquet conformance

The dataset is a [SeekParquet](https://github.com/varveio/seekparquet) dataset, and the replay server reads it
through the seekparquet Go module, so the format's own conformance suite is a gate. CI's `go-nonrace` job runs
`scripts/ci/seekparquet-conformance.sh` (from `go/`; under a minute, most of it installing the suite's Python
dependencies):

1. It checks out the seekparquet repository at the tag of the module version `go.mod` pins (`go/v0.1.0`) and
   runs the suite's harness, `conformance/harness/run.py`, against that module's Go adapter
   (`internal/cmd/conformance`): the `reader` target must qualify, every core and generated case passing.
2. `listing` `TestSeekParquetFixtures` (skipped unless `SWATH_SEEKPARQUET_FIXTURES` names a directory) writes
   three datasets with this build's writer and publication: S3 current objects over four parts whose keys roll
   up at several depths; S3 versions, keys repeated in groups up to 3,000 rows across pages and row groups
   (`unique` false); filesystem keys over 1,024 bytes, half led by 1,100 `0xFF` bytes, a third of them symlinks whose link
targets are as long.
3. `scripts/ci/seekparquet_conformance.py` has the suite's reference adapter (a full pyarrow scan that checks
   every rule it can, page indexes and page bounds included) validate each dataset at the `full` and `digest`
   levels (`digest` checks nothing: swath writes no `sha256`), then requires the Go adapter to answer whole and
   paginated ranges, lookups, prefix reads and `/`-delimited listings over keys taken from the dataset exactly
   as the reference adapter does.

Run it locally the same way: `cd go && ../scripts/ci/seekparquet-conformance.sh /tmp/spc` (Python 3 with
`venv`, network access for the checkout and the pinned wheels). Results are recorded in
[measurements.md](measurements.md#seekparquet-conformance).

## Fuzzing

```bash
go test -run XXX -fuzz FuzzParseDirents ./internal/source/localfs         # getdents record parser
go test -run XXX -fuzz FuzzParseList -fuzztime 2m ./internal/source/s3/client
go test -run XXX -fuzz FuzzParseFields -fuzztime 1m ./internal/source/s3/client
go test -run XXX -fuzz FuzzPropose -fuzztime 1m ./internal/source/keyrange   # pivot policy
```

Failing inputs land in `testdata/fuzz/` and run as regular tests from then on. CI never fuzzes: the default suite (`go-race`,
`go-xos`, `go-race-nightly`) runs each target's seeds and `testdata/fuzz/` inputs as ordinary tests. A fuzzing session is a manual tier.

### Replay server targets

Each target seeds from the goldens and fixtures of its package, runs its seeds in a plain `go test`, and
states a property that holds for every input. `-fuzz` takes one target per run (a regular expression that
matches one name, from inside `go/`):

```bash
nice go test ./internal/replay/s3 -run XXX -fuzz '^FuzzToken$' -fuzztime 2m
```

| Target | Property |
|---|---|
| `s3` `FuzzToken` | `DecodeToken` never panics and answers a non-token with the one 400 `InvalidArgument`; an accepted string is the only spelling of its boundary; a boundary round-trips; a token with a character replaced, a decoded byte changed or padding added is refused (CRC-32 catches every error burst of 32 bits or fewer); one with one or four characters dropped or one appended is refused or is the only spelling of another boundary (the seed `four-dropped-is-a-token` is such a pair) |
| `s3` `FuzzParseRequest` | `ParseRequest` never panics; every refusal is a 400 `InvalidArgument`; an accepted request has a page of 0 to 1,000, UTF-8 bounds with no NUL where one is refused, and a token that decodes |
| `s3` `FuzzListingText` | any key, prefix and delimiter render to a listing `encoding/xml` parses; with `encoding-type=url` each echoed value decodes back to its bytes by S3's table (`+` a space); without it each reads back as the key's text with malformed UTF-8 replaced, wherever that text is representable in XML 1.0 (no CR, which the parser folds, and no control character, which S3 writes as a character reference) |
| `s3` `FuzzErrorText` | an error document carrying any value parses and reads back as the value, under the same representability |
| `s3` `FuzzRowFastPathMatchesUnits` | `TestRowFastPathMatchesUnits` for any key and text values, chunk size and cap: the same bytes, refusal, chunks and percent-encoding counts |
| `output` `FuzzEscaped`, `FuzzPercentEncoded` | `XMLEntity`'s escaping and the percent encoder write the same bytes at every chunk edge as the byte-by-byte definition; the percent encoding decodes back to the input |
| `gcs` `FuzzToken`, `FuzzTokenScope` | `decodeToken` never panics and refuses with `errToken`; an accepted token re-encodes to one that decodes alike; a round trip keeps the progress byte; another binding, or any altered character of the binding, is refused; a token is accepted exactly under the scope and fixture it was issued for |
| `gcs` `FuzzParse` | never panics; every refusal is a `*Failure`; an accepted request has a page of 1 to 1,000 and UTF-8 key parameters of at most 1,024 bytes |
| `azure` `FuzzMarker`, `FuzzMarkerScope` | the same properties for `az1.` markers, with the scope's fields (prefix, delimiter, `startFrom`, fixture identity, service version) each binding |
| `azure` `FuzzParse` | never panics; every refusal is an `*Error` with a status and code; accepted values are within the bounds |
| `azure` `FuzzCompareIsUTF16Order` | `Compare` is the comparison of UTF-16 code units for any two valid UTF-8 names, and a total order on any bytes |
| `azure` `FuzzListingOrder` | a walk over any valid names (flat or delimited, any prefix, `startFrom` and page size, native or row rollup) equals the brute-force UTF-16 listing |
| `azure` `FuzzListingXML` | a listing of any name and echoed values parses with `encoding/xml` and reads back as the input, a name sent `Encoded="true"` after percent-decoding; a name the route refuses is a `FixtureIncompatible` error |
## Durable spill campaign (opt-in)

The durable spill's crash tests above run on a 30,000-key fake bucket. The campaign repeats their schedule on
a real S3-compatible backend and on production key lists, through the real binary (`cmd/swath`
`durable_campaign_test.go`). For each fixture a reference run is listed first, and then a cold run with a
slow frontier: a page below the key at 10 % of the list waits 200 ms more, so the ranges ahead of it spill
and seal certificates. Each arm then kills a run with `SWATH_TEST_KILL_AT` at one of the following,
or with SIGKILL once a given share of the keys has been served:

- a durable file's capture frames, its seal, the ledger append, the ledger sync, and the first unlink;
- after the second part record;
- mid recovery replay (killed at the third ledger sync, then the resume killed at its second replayed
  frame);
- unsynced state lost, either unrecorded durable files cut short after a kill at a seal, or the ledger's
  last append torn after a kill at its sync;
- 40 % and 70 % of the keys served;
- 50 % of the keys served, then resumed with `--seed-from` the reference dataset (the `seed-from` arm). The
  prior must engage, and it must leave the reuse and request checks holding as without it.

The arm then resumes. Each schedule runs with `--part-interval=0` and again with a 1 s interval. A kill counts
only as the fault that was asked for: a hook kill must leave its marker on stderr, and a key-count kill must
be the harness's. A run that the 20-minute watchdog ends fails (`TestCampaignRunnerFaults`). Every arm asserts
the following:

- The dataset is byte-identical to the reference (row-identical, every column, with the interval on).
- Rows reconcile: retained plus listed rows equal the dataset, and the objects emitted, less the filtered
  rows, plus the replayed rows, equal the rows listed.
- No certificate is rejected.
- No data request of any resume starts in `[max(cs, K), ce)` of a certificate that is live in the ledger's
  durable prefix when that resume starts. Every resume is checked: the killed first resume of the mid-replay
  arm, and each try and the final resume of the suspect-composition arm. A resume that made requests must
  have logged some.
- The reuse arms are the unlink, mid-replay, 40 %, 70 % and seed-from kills, all of which fall after seals. Each needs
  certificates above K, and the resume must accept and replay some. The kills before the first seal are not
  asked to reuse.

One more arm, `suspect-composition`, runs at a 3 s interval. This is §6's "time cuts on" row: the composition is
forced, and the reproduction is checked byte for byte.

1. A run with serialized seals is killed at its sixth ledger sync, so five certificates are durable. Its parts
   are 64 MiB, so the interval closes them.
2. Copies of that output are resumed, without the slow frontier, and killed at the n-th journal append,
   after the part's claim and before its record. At 1 s, or behind the frontier, MinIO's time-closed parts
   held one certificate or one gap, never both. This repeats until the claimed part holds rows from two
   certificates' intervals and rows listed live between them.
3. The next resume must take those rows from replay. It rejects no certificate, accepts every certificate live
   above K (the part's among them), and replays at least the part's certificate rows. With reuse off, the arm
   fails.
4. That resume must also reproduce the claimed file at its ordinal byte for byte. This is the suspect-part
   invariant, claimed-file reproduction. Identity with the cold run's parts is the interval-0 arms' assertion,
   whose parts are cut alike.

The reference must list exactly the key file the fixture serves (`SWATH_CAMPAIGN_KEYS`, required), which is an
oracle independent of swath. Both tiers skip without their endpoint, so CI never runs them. The retained
results ([measurements.md](measurements.md#durable-spill)) reproduce with:

```bash
# MinIO in Docker, 250,000 keys (up, the test, then down, also on failure)
head -n 250000 noaa-nbm-grib2-pds.keys > nbm250k.keys
RESULTS=durable-minio.tsv bench/minio.sh campaign nbm250k.keys

# three production lists, each list's first 2,000,000 keys behind the private key-list fake (keyfake) at 5 ms
KEYFAKE=/path/to/keyfake MAX_KEYS=2000000 SLICE_DIR=/dev/shm RESULTS=durable-corpus.tsv \
  bench/durable_corpus.sh real-changesets.keys noaa-nbm-grib2-pds.keys argoverse-6M.keys
```

**What covers the contract's test matrix.** The durable spill contract's §6 rows, and the tier that runs each
one. "Campaign" is this tier; the in-process tests run on the 30,000-key fake and in `order` unit harnesses.

| §6 row | Covered by |
|---|---|
| kill matrix (§3.4-5, §3.8, after a part record, mid recovery replay) | campaign (all four fixtures); `listing` `TestDurableKillMatrix` (1st and 4th occurrences); `cmd/swath` `TestSIGKILLDurableSpill` (its recovery arms start from an unpublished prefix, K at ⊥, so they do not exercise recovery with K advanced; the campaign's recovery arms do) |
| ledger damage (flipped byte, foreign header) | `order` `TestDurableLedgerPrefix` |
| Sol 1 (reordered persistence, crash again in recovery) | `order` `TestDurableLedgerPrefix` (reordered); `listing` `TestDurableRepeatedRecovery` (its damaged snapshot is first resumed uninterrupted on a copy, the dataset the uninterrupted one, and must replay two captured frames or more; a snapshot that replays fewer is crashed again, at most six times) |
| Sol 2 (shutdown order) | `listing` `TestDurableShutdownOrder` |
| Sol 3 (ledger failure, eviction, credit) | `order` `TestDurableLedgerFailureEvicts`; `listing` `TestDurableLedgerTornWrite` |
| replaced output | `order` `TestDurableReplacedOutput`, `TestDurableReplacedMidCycle` |
| credit path | `order` `TestDurableCreditWithoutOrderLock` |
| new ancestors lost | `listing` `TestDurableNewAncestorsLost` |
| lock order | `order` `ledgerOrder` hook, in `TestDurableCertifiesExactRows`, `TestDurableCreditWithoutOrderLock`, `TestDurableLedgerFailureEvicts` |
| seal errors | `order` `TestDurableSealErrorStopsRun` |
| slots full | `order` `TestDurableSlotsFull` |
| history race | `order` `TestDurableHistoryRace` |
| retire defer | `order` `TestDurableRetireDefer` |
| repeated recovery | `listing` `TestDurableRepeatedRecovery` (each recovery step, on its own copy); not at every cleanup step twice |
| history across attempts | `order` `TestDurableHistoryAcrossAttempts`, `TestDurableRecoveryHistory`, `TestDurableReducedCap` |
| reuse (kill at 40 %, skewed fixture) | campaign: kills at 40 % and 70 %, slow frontier, request log; `keyrange` `TestReuseTilesAroundSpans` |
| pre-capture | `order` `TestDurableZeroRowCertificate`, `TestDurableReplayAboveK` |
| abandon | `order` `TestDurableAbandonThenSpill` |
| fence vs chunks | `order` `TestDurableCertifiesExactRows` (pages of 1, 999, 1,025, 3,000) |
| filtered | `order` `TestDurableZeroRowCertificate`; a long wholly filtered range: not covered |
| unsafe K, then crash again | `keyrange` `TestFencesHoldTheContract` (unsafe resume keys, no crash); crash again after an unsafe K: not covered |
| corruption | `order` `TestDurableCorruption`, `TestDurableFrameClaims` |
| foreign | `order` `TestDurableCorruption` (another run's file renamed into a recorded ID) |
| compression | `listing` `TestDurableCompressionFlip` |
| versions | `listing` `TestDurableVersions` |
| repeated keys (`--all-versions`) | `listing` `TestVersionsDurableSpill` (hot keys, a page boundary inside a group; killed at a seal, a ledger append, an unlink, a part claim with K inside a live certificate (required: a kill that leaves K outside every certificate, 5 of 160 under `-race`, is repeated on a fresh output up to six times, then fails), and mid recovery replay; byte-identical, no fresh request or continuation inside a live certificate; the frontier held by a gate the run's own events release, not by time); `source/keyrange` `TestGroupsFencesAtCompleteKeys` (fences only at complete keys) |
| exclusion | `listing` `TestDurableOverwrite`; a second process: the output lock's tests, not durable-specific |
| changed source | `listing` `TestDurableChangedSource` |
| a prior (`--seed-from`) on a resume with certificates (not a §6 row; the contract's priors rule) | `keyrange` `TestPriorTilesOnlyGaps`; campaign `seed-from` |
| time cuts on (suspect part over two replay files and a gap) | campaign `suspect-composition` (3 s interval) |
| reduced cap, ENOSPC | `order` `TestDurableReducedCap`; `listing` `TestDurableSealENOSPC`; ENOSPC on a captured frame: not covered |
| eviction | `order` `TestDurableEviction`, `TestDurableLedgerFailureEvicts` |

On the campaign's fixtures, unsynced loss is emulated in two ways only: unrecorded files are cut short after a
kill at a seal, and the ledger's last append is torn after a kill at its sync.

`SWATH_CAMPAIGN_RESULTS` (which both wrappers set) collects one line per arm. The retained results are
in [measurements.md](measurements.md#durable-spill).

## Shape suite

`internal/shapes` generates 21 bucket shapes, each hard for the range engine in one way: the reference (200
equal folders), a long shared prefix with a counter, an unpadded counter, flat hex, hashes under 16 folders, a
Zipf folder distribution with 90 % in one folder, a folder per key, multi-byte names, 1,024-byte keys,
siblings that sort around `/`, a 50-level chain, a date layout whose newest day holds 99 %, a sparse top over
one dense folder, folder-marker objects, trailing-byte variants, `%` in every key, three tiny buckets (100,
1,000 and 10,000 keys), a top level of 200,000 folders, and a density cliff. `Shape.Keys(n)` is deterministic
(the same keys on every machine for the same shape and scale), sorted and unique; nothing is read from disk.
`shapes.Run` lists a shape through `scalefake` in process, with the command's engine defaults and any of its
flags, and returns the report's numbers: wall over ideal (`(pages/workers + 7 + seed round trips) x RTT`),
requests and HTTP attempts per 1,000 keys, `split.max_depth`, the mean of active ranges, splits, empty
children, throttles, and for a dataset arm whether its keys are the shape's.

**Default tier.** `TestShapes` lists every shape at about 200,000 keys behind a 5 ms fake to Parquet at 16
and at 5 workers: both datasets are exactly the shape's keys and their parts are byte-identical. The 16-worker
run is held to bounds an order of magnitude from what it measures, so a tenfold regression fails and no
change short of that does (the measured values are beside each bound in the test): at most 10 requests per
1,000 keys (measured 1.1 to 2.0), `split.max_depth` at most 3·(6·log2(pages)+16) (at most 34 against 55 to
61), and a mean of at least a tenth of the workers active (8.8 to 15 of 16; shapes of 100 pages or more). The
tiny shapes are held to 50 requests per page plus 50 instead. It takes about 15 s on the ubuntu runner (40 s
on a busy 4-core box); under `-race` it skips (ten times slower, and the code it drives is race-tested by its
own packages), so CI runs it in the `go-nonrace` job; under the race shards the package's other tests run in
`rest`:

```bash
go test -count=1 -run 'TestShapes$' -v ./internal/shapes
```

**Nightly tier.** `bench/shapes` runs the arm table in `bench/shapes/arms.go` at full scale (about 2M keys
per shape, 88 arms): every shape at 50 ms and 64 workers; a latency ladder (10 and 200 ms); 256 workers;
pages capped at 100; at 10, 50 and 200 ms on the reference shape: jitter, 1 % and 5 % throttling (with and
without the limiter), a 300 request/s cap, a slow prefix over half the keys and over one folder, slow
continuations, slow seeks and a slow first page; `--slow-split` arms; and Parquet arms. Every arm checks its
output key for key against the shape, in order: a Parquet arm reads its dataset back, every other arm writes
its rows as an ordered TSV stream compared in memory as it arrives (`shapes.Run`), so an overlap between
ranges cannot hide behind an equal gap. It writes one JSON summary (the workflow's `go-shapes-summary`
artifact, `schema_version` 1) and compares it with `bench/baselines/shapes.json`:

```bash
go run ./bench/shapes -out shapes.json -baseline bench/baselines/shapes.json   # the nightly
go run ./bench/shapes -list                                                    # the arms
go run ./bench/shapes -run 'throttle|pq\.' -scale 200000 -out /tmp/s.json      # a subset, smaller
```

Only a wrong listing (rows that are not the shape's, an arm that fails or runs past `-arm-timeout`) fails
the job. A number (wall over ideal, requests or attempts per 1,000 keys, depth, mean active) that moved by
more than 20 % against the baseline, and by more than a small absolute floor, is annotated on the run and
tabled in its summary; so is a baseline that is missing, unreadable, of another schema version or scale, or
a summary that could not be written. None of these fails the job. Once the baselines have held steady for
about two weeks of nightlies, the comparison switches to failing the job.

**Baseline policy.** The baseline is the summary of a nightly run on GitHub's ubuntu runner (walls on another
machine are not comparable): download the `go-shapes-summary` artifact and commit it as
`bench/baselines/shapes.json`. A pull request that is meant to move a number updates the baseline in the same
pull request, from a run of its branch (`gh workflow run nightly.yml --ref BRANCH -f jobs=go-shapes`), and says
which numbers moved and why. A change that moves a number unintentionally is a regression to fix, not a
baseline to update.

## Measurements

The measurements, their configurations and the commands that reproduce them are in
[measurements.md](measurements.md); scripts are in `../bench/`.
