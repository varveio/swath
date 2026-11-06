# Operating swath (Go)

For the person who runs `swath` in production: how to install it, size and run a listing, watch it, stop and
resume it, and read what it produced. Flags and exit codes in full: [cli.md](cli.md). Why it behaves as it
does: [internals/durability.md](internals/durability.md), [internals/ordering.md](internals/ordering.md),
[internals/providers.md](internals/providers.md).

swath lists metadata only. It sends S3 `ListObjectsV2` or GCS `objects.list` requests and never reads, writes or
deletes objects.

## Install and verify

One static binary, pure Go (`CGO_ENABLED=0`), no runtime to install. Build it with Go 1.26 (the `go` directive
in `go.mod`):

```bash
cd go
./build.sh -o swath                                   # static, stripped, the revision stamped
GOOS=linux  GOARCH=arm64 ./build.sh -o swath-linux-arm64
GOOS=darwin GOARCH=arm64 ./build.sh -o swath-darwin-arm64
./swath --version        # swath-go VERSION (REVISION)
```

`build.sh` runs `CGO_ENABLED=0 go build -trimpath -ldflags '-s -w -X …listing.revision=REV'` with REV the
checkout's HEAD (`-dirty` with local changes in `go/`) or `$SWATH_REVISION` (CI sets its commit). A plain
`go build ./cmd/swath` works too but takes the revision from the Go toolchain's VCS stamp, which a build in a
git worktree may lack (`--version` then shows none).

The module cross-compiles for linux/amd64, linux/arm64, darwin/arm64 and windows/amd64. Only Linux has been
run. A stripped linux/amd64 binary is about 18 MB ([measurements](measurements.md#client-cpu-per-object-and-binary-size): 17.7 MB
with GCS).

`--version` prints the tool name, the module version (`devel` for a source build) and the VCS revision when the
build recorded one (`-dirty` if the tree was modified). Every report carries the same in `build`. Keep the
revision with any run you may need to investigate.

## Credentials

Secrets are never taken from flags. What fails closed: a credential source that is configured but does not
load stops the run with cause `auth` (exit 1) before any listing request. swath never falls back to
unsigned requests in that case.

**S3.** The AWS SDK's default chain: environment keys, the shared `config` and `credentials` files
(`--profile`, `AWS_PROFILE`), IAM Identity Center (SSO), web identity, `credential_process`, AssumeRole
profiles, container credentials (ECS, EKS Pod Identity), EC2 instance roles (IMDSv2).

- Requests go unsigned only when no source is configured anywhere and the lookup (bounded at 3 s) finds
  nothing, or with `--anonymous`, which ignores every credential.
- A configured source is waited for up to 30 s. A source that fails for good is `auth`. One that fails
  transiently (an STS 5xx or throttle) is kept and retried with each request.
- Refused before any request as configured but broken: an access key without its secret or the reverse, a
  session token without both keys, `AWS_ROLE_ARN` without `AWS_WEB_IDENTITY_TOKEN_FILE`, a profile that does
  not exist.
- Region: `--region`, else `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region`, else `us-east-1`. A
  wrong region is an error that names the bucket's region. Redirects are not followed. STS calls use the
  environment's or profile's region, not `--region`.
- Requester-pays buckets: `--request-payer`. Endpoint variants: `--fips`, `--dualstack`; S3-compatible stores:
  `--endpoint URL` (or `--endpoint-url`), addressed path-style unless `--path-style=false`.
- Bearer tokens instead of SigV4 (an S3-compatible endpoint that takes OAuth tokens, such as GCS's XML API):
  `--bearer-token-command 'gcloud auth print-access-token'`, refreshed every 45 m
  (`--bearer-token-refresh-interval`). A failing command fails the run with cause `auth`. The command is never
  recorded, so give it again to `swath resume DIR` ([cli.md](cli.md), "Bearer tokens").
- Not supported: access points and Multi-Region Access Points, Outposts, S3 directory buckets (a name ending
  in `--x-s3` is refused, exit 2).

**GCS.** `--gcs-credentials FILE` (a service-account or authorized-user JSON), else Application Default
Credentials (`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud file, the metadata server on Google Cloud).
Requests go unauthenticated only when none exists, or with `--gcs-anonymous`. A
`GOOGLE_APPLICATION_CREDENTIALS` that names a missing file is refused (`auth`). The token scope is
`devstorage.read_only`. Requester-pays buckets: `--gcs-user-project PROJECT` bills that project; it is part of
the resume identity.

The report's `source.stats` records `anonymous`, `credentials` (where they came from) and, for S3, `region`.

## Sizing a run

| Resource | Flag | Default | What to know |
|---|---|---|---|
| Listing concurrency | `--workers` | 64, whatever the CPU count | listing waits on the service, not the CPU; it is also the request limiter's ceiling |
| Row budget | `--row-budget` | 128 MiB | bounds the bytes of rows and scheduler metadata in flight (reorder storage and encoder queues). Not a cap on resident memory |
| Encoders | `--encoders` | GOMAXPROCS | a ceiling: each is admitted against 24 MiB of a writer allowance equal to `--row-budget` and the descriptor limit; the default 128 MiB admits 5. A warning names a shortfall |
| Spill disk | `--spill-max` | 0: half the free space on the output filesystem at start, after the reserve | rows listed ahead of a slow frontier go to `DIR/.swath/spill/`; past the cap, or at the reserve, listing waits instead of failing. The reserve is derived: one part per encoder at full size, at least 1 GiB (report: `resources.spill.reserve_bytes`; `swath diag list --spill-reserve` overrides it) |
| Part size | `--part-size` | 64 MiB | target Parquet bytes per part |

**Disk for the worst case** is the dataset itself plus the spill cap plus the reserve, all on the output's
filesystem (spill lives under the output directory). Estimate the dataset from a representative prefix: the
report's `output.bytes` over `output.rows_committed`. If the dataset outgrows the disk the run stops with exit
74 and resumes once there is space; spill alone never fails a run.

**Memory.** Outside `--row-budget` are the encoders' writers (24 MiB each), response bodies being parsed (up to
16 MiB each, one per worker in the worst case), compression scratch and the garbage collector's headroom. Read the actual figure from the report's
`resources.peak_rss_bytes` on a representative run, and size the host from that, not from `--row-budget`.

## Running

```bash
swath list s3://bucket/prefix -o /data/inv-2026-11-06 \
  --report /var/run/swath/attempt-1.json --max-duration 6h
```

- **Output directory.** `-o DIR` must be empty or absent. A non-empty directory is refused (exit 2) unless
  `--overwrite`, which replaces only a previous dataset of this tool. The output may not be the listed root or
  inside it.
- **The outcome of record** is the exit code plus the report written to `--report FILE` when the attempt ends,
  on failure too. Give each attempt a fresh path outside the output directory (or remove the old file first).
  The report's `outcome` holds `class`, `exit_code`, `resumable`, `cause`, `complete`, `stop_reason` and, when
  the outcome could not be written to `DIR/error.json`, `unrecorded`; `attempt.id` identifies the attempt.
  `--report-format json` also prints the report on stdout; the default prints one line.
- **`--max-duration`** (default 0, no limit) stops the run with exit 124; an object-store run is resumable.
  It is the only stop for a service that keeps answering without rows. Give every unattended attempt one.
- **Process deadline.** A stop is graceful; if the run has not stopped 15 s later the process writes its
  outcome and exits, at most 20 s after the stop. A supervisor's own kill deadline belongs above
  `--max-duration` plus 20 s. An attempt killed that way may have no report: treat it as unavailable, never as
  the outcome in an older `error.json`.
- **`--part-interval`** (default 30 s; 0: size only) closes an open part once it is that old, so the durable
  recovery point (what a resume keeps) is never much older than that on a slow frontier. With 0, part
  boundaries do not depend on timing.
- **`--stall-timeout`** (default 10 min; 0: never) stops a run with no progress and no usable answer from the
  service for that long (exit 75). A warning is printed at half.

## Watching a run

**On a terminal.** swath draws one progress line on stderr, redrawn every second: phase, objects (with rows a
resume recovered), live and average keys/s, parts, requests in flight, elapsed time, API calls and the cost
estimate. Off a terminal it is off unless asked: `--progress` appends the same record as a plain line every 30 s
(`--progress-interval` sets it, at least 1 s); a log never receives control sequences. `-q` turns the
automatic progress and the warnings off, `--no-progress` turns progress off everywhere. At the end, runs over
1.5 s, runs that wrote parts and runs that did not complete print Java's statistics block on stderr (objects,
time, keys/s, API calls per 1,000 objects, mean and peak in flight, throttles and retries when any, the
estimate, files, bytes and peak RSS); `--stats` forces it (also for a supervisor's log), `--no-stats` drops it.
An `INCOMPLETE (class)` first line names a stop, with the `swath resume DIR` command when it is resumable.
The one-line summary stays on stdout. Colour: `--color auto|always|never`, auto honouring `NO_COLOR`.

`DIR/.swath/progress.json` is rewritten every 2 s (atomic rename) while the run holds the output.

Its fields are listed once, under "Progress file" in [cli.md](cli.md).

`progress.json` also carries the live request account: `requests` (attempts sent), `retries`, `inflight` (now)
and `limit` (the adaptive limiter's current limit), and `cost_usd` on AWS S3 or GCS. The totals are in the
report, under
`source.stats.counters.values`: `attempts`, `requests`, `retries`, `throttles`, `timeouts`, `inflight.max_active`,
`inflight.busy_ns` (divided by the listing time, `timings.listing_ms`, it is the mean in flight), `limiter.decreases`,
`limiter.increases`, `limiter.max_depth`, `limiter.waits`, and `source.stats.mean_active` (mean active ranges). The
report's `resources` carries `budget_wait_ns`, `frontier_wait_ns`, `peak_nodes`, `peak_markers`,
`part_time_cuts` and `spill.peak_bytes`, `spill.peak_readers`, `spill.max_bytes`, `spill.reserve_bytes`.

| Signal | Healthy | Act when |
|---|---|---|
| `part_age_ms` | under a minute | several minutes: durable progress has stalled and a crash now loses all of it |
| `order.spill_live_bytes` | small and transient | climbing for minutes: the frontier is stalled while other ranges finish; disk exposure |
| `order.nodes`, `order.markers` | tens to hundreds | tens of thousands and rising: a deep speculative tree behind a slow frontier |
| `objects` against `encoded`, both per second | similar | listed far ahead of encoded: work fetched that cannot be published yet |
| `advance` | rising | flat while `answered` rises: alive but not progressing; `empty_chain_pages` rising: the service answers only empty pages, and the watchdog stops the run (75, `stuck_empty_chain`) after `--stall-timeout` |
| `throttles` | flat | rising steadily: see Throttling |
| `inflight` against `limit` | `limit` near `--workers`, `inflight` near `limit` | `limit` falling: the service throttles; `inflight` far below `limit`: the engine has too little work in parallel |
| `resources.budget_wait_ns` (report) | near 0 with spill on | large: workers blocked on a full row budget |
| `mean_active` against mean in flight (report) | both near `--workers` | active high, in flight low: requests held by the limiter or admission |
| exit 75 on the same output | never twice | a second 75 in a row needs a person |

## Throttling

By default (S3 and GCS) every HTTP attempt (pages, probes, retries, requeues) takes a permit from an adaptive
limit. It starts at `--workers`, is multiplied by 0.7 on each throttle answer (S3 429, 503, `SlowDown`; GCS 429
and rate-limit reasons; floor 1), and grows by one per second after 10 s without one. Growth is held while
attempt latency is above twice its baseline or three or more timeouts fell in the last 10 s.

- `--limiter=false` keeps a fixed `--workers` requests in flight.
- `--limiter-slow-start` starts the limit at 4 and doubles it each second up to `--workers` until the first
  throttle answer or timeout.
- `--request-rate N` caps all HTTP attempts at N per second, after the limiter's permit; for a quota shared
  across a fleet.
- Each request is retried up to 9 attempts (backoff 100 ms doubling to 5 s); after that the page is asked
  again (2 s doubling to 60 s) for as long as the run lives. Retry exhaustion never fails a run; only the stall
  watchdog does.

Measured on replay with injected faults (synthetic,
[measurements](measurements.md#throttling-through-the-fault-proxy)): a 5 s storm of 50 % SlowDown drops the limit from 64 to 1 in under 0.1 s, and full concurrency
returned about 68 s after the storm; the run sent 11-38 % fewer requests and took 1.5-3.5x the wall time of a
fixed 64. Under a sustained 20 % throttle rate the limit stays at 1-2 for the whole run (about 23x the wall
time of `--limiter=false` on replay). If a run crawls with `throttles` rising and the report's
`limiter.decreases` high (`limiter.max_depth` near `--workers`: the limit fell to about 1), that is this rule; `--limiter=false` with `--request-rate` is the fixed-rate alternative.

## Stopping and resuming

| Exit | Class | Retry |
|---|---|---|
| 0 | `completed` | done (also `swath resume` on a complete dataset) |
| 1 | `fatal`, `incomplete` | no: a resume will not fix it (access denied, no such bucket, a protocol violation, an invariant) |
| 2 | `usage`, `refused` | no: fix the command or the output directory |
| 74 | `disk_full` | `swath resume` once there is space (or after moving the output to a larger disk) |
| 75 | `stuck` | `swath resume`; `cause` is `stuck_empty_chain`, `stuck_throttle`, `stuck_timeouts` or `stuck_no_rows` |
| 124 | `deadline` | `swath resume`, but only under a separate budget (attempts or total time); past it, a person |
| 130, 143 | `interrupted` (SIGINT), `terminated` (SIGTERM) | `swath resume` |

"Resumable" means an object-store run whose journal is intact. A filesystem run keeps no journal: start it
again in a new or empty directory (`--overwrite` refuses an interrupted filesystem output as not a dataset).

**Signals.** SIGINT and SIGTERM stop the run gracefully: published parts and the journal stay, and
`error.json` and the report are written. Only the first signal counts; the forced exit follows 15 s later. SIGKILL, a panic or an
OOM kill loses nothing durable: the journal is crash-safe and the resume reconciles.

**`swath resume DIR`** replays the command recorded in the journal. Only these may be given again (the last
value wins): `--workers`, `--encoders`, `--row-budget`, `--spill-max`, `--spill-reserve`, `--spill-compress`,
`--part-interval`, `--report-format`, `--report`, `--max-duration`, `--stall-timeout`, and `--profile`,
`--anonymous`, `--request-timeout`, `--gcs-anonymous`, `--gcs-credentials`, `--gcs-request-timeout`,
`--limiter`, `--limiter-slow-start`, `--request-rate`. Any other flag is a usage error. `--max-duration` and the
other runtime values are replayed from the first attempt unless given again; pass a fresh `--report` path each
time.

The resume keeps every published part, verifies each against its journal record (size and MD5: reads every
retained part in full, measured at 247-376 MiB/s cold and 511 MiB/s warm, so 10,000 parts of 64 MiB take
roughly 30-45 minutes; progress shows as `verified_bytes`), then lists again after the last published key.
Rows listed but not yet in a published part are listed again.

**Identity check.** The source, the route (endpoint, addressing, signing region; for GCS the API base and the
billed project), `--fetch-owner`, the schema and the part size are frozen. A resume whose environment or
`--profile` resolves another region or endpoint is refused (exit 2, `refused`) and the directory is left as it
was. Restore the original environment, or start over.

**Refused resumes** and what they mean:

| Message or class | Meaning | Action |
|---|---|---|
| exit 2, identity mismatch | the resume would list another route | fix the environment, or `swath list --overwrite` |
| exit 2, nothing to resume | no journal in the directory | start a fresh `swath list` |
| exit 1, `cause` `protocol` | the service returned an invalid page; the journal is marked unresumable | `swath list --overwrite` |
| exit 1, journal version | written by another build | finish with that build, or `--overwrite` |
| exit 1, part altered or journal corrupt | a retained part or the journal changed on disk | `--overwrite` |
| exit 1, ambiguous tail | the part set aside in `.swath/suspect/` cannot be reproduced: the source changed in that part's range | `--overwrite` to start over, or remove `.swath/suspect/` to accept the source as it is now |

## Overwrite and refresh

`--overwrite` is destructive, not atomic. Under the output lock, before anything is listed, it removes the
previous `manifest.json` and then the rest of the old dataset. From then until the new manifest is installed
the path holds no dataset, and a failed run leaves none.

Where readers must always find a dataset (a periodic refresh), write each run to a new directory and never
overwrite it. Switch readers through a pointer outside the datasets (a generation file, a database row, a
symlink swapped by rename) once the new directory's `manifest.json` exists, and delete an old generation only
when no reader uses it.

## Reading the output

```text
DIR/
  data/part-NNNNN.parquet   key-sorted, disjoint parts, ordinal order == key order (the manifest lists them in order; name order
                            agrees only below part-100000)
  manifest.json             the commit: written last, names every part with exact key bounds and coverage
  run.json                  the run report (after the commit)
  error.json                the outcome of an attempt that did not complete
  .swath/                   private workspace: lock, journal, parts/, spill/, suspect/, progress.json, stack dumps
```

**The manifest is the commit.** Parts appear in `data/` before the run ends, so `data/` alone is not a
dataset: a reader starts only once `manifest.json` exists, and a reader that needs the whole bucket checks
`coverage.complete` true, `coverage.stop_reason` `completed`, `coverage.skipped_entries` 0, `coverage.filters`
empty and `scope.kind` `whole` (fields: [upgrade.md](upgrade.md)).

```bash
swath show DIR            # format, schema, source, scope, coverage, parts, rows, key range
swath show --journal DIR  # an interrupted run: the journal as JSON lines
duckdb -c "SELECT count(*), sum(size) FROM read_parquet('DIR/data/*.parquet')"
duckdb -c "SELECT storage_class, count(*), sum(size) FROM read_parquet('DIR/data/*.parquet') GROUP BY 1"
duckdb -c "SELECT key, size FROM read_parquet('DIR/data/*.parquet') WHERE starts_with(key, 'logs/2026/') ORDER BY key LIMIT 20"
```

The S3 schema is `swath/s3-objects/v1` (`key`, `size`, `last_modified`, `etag`, `storage_class`, `owner_id`,
`owner_display_name`, `checksum_algorithm`, `checksum_type`); GCS is `swath/gcs-objects/v1`. `python3
verify.py DIR` checks a dataset independently with DuckDB. A downstream reader that consumes the dataset relies
only on the manifest's contract ([upgrade.md](upgrade.md)); Java swath's reader does not open format 2.

## Known limits

- **Windows:** a journaled (object-store) run and `swath resume` are refused: there is no directory sync, so
  a power loss could lose acknowledged parts. The output lock holds. Filesystem runs work but are not
  resumable. Built, never run natively.
- **macOS:** builds; not run.
- **Power loss or a kernel crash:** recovery relies on the filesystem honouring `fsync` on files and directories
  (ext4, XFS, APFS do). Process crashes are tested at every crash point; power loss is not.
- **Live scans, not snapshots:** objects added, removed or changed during the run change what it observes. A
  committed dataset is the complete result of a live listing, exactly once per key, not a point-in-time view.
- **Memory:** `--row-budget` bounds rows in flight, not resident memory (see Sizing).
- **Scale:** billions of keys, runs of hours and 256 or more workers are unmeasured ([measurements](measurements.md#known-gaps)).

## When it goes wrong

1. **The exit code and the `--report` file.** `outcome.class` and `outcome.cause`: the provider's failure class
   (`auth`, `access_denied`, `no_such_bucket`, `redirect`, `throttle`, `timeout`, `protocol`, ...), `no_space`, or
   a `stuck_*` cause. `outcome.unrecorded`, when set, says why `error.json` was not updated.
2. **`DIR/error.json`**: the same outcome inside the dataset, current only when its `attempt.id` matches the
   report's. Without a report, only the exit code is current. A refused command that could not record its
   outcome prints it on stderr as `swath: outcome: {...}`.
3. **Goroutine dumps.** At a stall, and at a forced exit, every goroutine's stack and the progress counts go
   whole to `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt` (replaced each time), else to stderr,
   cut at 1 MiB.
4. **`.swath/`**: `lock` (one process per output), `journal` (`swath show --journal`), `parts/` (encoded parts
   not yet published), `spill/` (disposable; removed by the next run), `suspect/` (a part a resume must
   reproduce), `progress.json`. Do not edit the journal or its parts by hand.
5. **Profiles:** `SWATH_CPUPROFILE=FILE` and `SWATH_MEMPROFILE=FILE` write pprof files.

For a report to the maintainers include `swath --version`, the `--report` file (redact bucket names and keys),
the provider and region, and the command line.
