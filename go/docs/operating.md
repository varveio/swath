# Operating swath (Go)

For the person who runs `swath` in production: how to install it, size and run a listing, watch it, stop and
resume it, and read what it produced. Flags and exit codes in full: [cli.md](cli.md). Why it behaves as it
does: [internals/durability.md](internals/durability.md), [internals/ordering.md](internals/ordering.md),
[internals/providers.md](internals/providers.md).

swath lists metadata from local or mounted filesystems, S3, GCS and Azure Blob Storage.
It does not read or modify source file/object contents. Object stores use their listing APIs;
versioned S3 listing uses `ListObjectVersions` instead of `ListObjectsV2`.

## Install and verify

**Go on main is unreleased.** Java v0.3.2 is the latest published release and has a different
CLI and dataset contract. Build Go from source for this guide; Go will replace Java at release.

One static binary, pure Go (`CGO_ENABLED=0`), no runtime to install. Build it with Go 1.26 (the `go` directive
in `go.mod`):

```bash
cd go
./build.sh -o swath                                   # static, stripped, the revision stamped
GOOS=linux  GOARCH=arm64 ./build.sh -o swath-linux-arm64
GOOS=darwin GOARCH=arm64 ./build.sh -o swath-darwin-arm64
./swath --version        # swath-go VERSION (REVISION)
```

`build.sh` runs `CGO_ENABLED=0 go build -trimpath -ldflags '-s -w -X …buildinfo.Version=VER -X
…buildinfo.Revision=REV'` with VER `$SWATH_VERSION` (the release workflow sets its tag's `X.Y.Z`; empty
otherwise) and REV the checkout's HEAD (`-dirty` with local changes in `go/`) or `$SWATH_REVISION` (CI sets its
commit). A plain `go build ./cmd/swath` works too but takes the revision from the Go toolchain's VCS stamp,
which a build in a git worktree may lack (`--version` then shows none).

The module cross-compiles for linux/amd64, linux/arm64, darwin/arm64 and windows/amd64. CI runs the test suite
on every Go change without `-race` on macOS (arm64) and Windows (amd64), and with `-race` on Linux: `internal/listing`
and `cmd/swath` on arm64, the other packages on amd64; a nightly run races the whole suite on amd64
([testing](testing.md#continuous-integration)); measurements are from Linux. A stripped linux/amd64 binary is about 18 MB ([measurements](measurements.md#client-cpu-per-object-and-binary-size): 17.7 MB
with GCS).

`--version` prints the tool name, the version (the release's `X.Y.Z`, `devel` for a source build) and the VCS revision when the
build recorded one (`-dirty` if the tree was modified). Every report carries the same in `build`. Keep the
revision with any run you may need to investigate.

### Release downloads and images

From 0.4 each [GitHub release](https://github.com/varveio/swath/releases) carries one archive per target,
`swath-X.Y.Z-linux-amd64.tar.gz`, `-linux-arm64.tar.gz`, `-darwin-arm64.tar.gz` and `-windows-amd64.zip`, each
holding `swath`, `swath-replay`, `LICENSE`, `NOTICE` and `THIRD_PARTY_NOTICES` (the licenses of every module
linked into the binaries, generated at release by `go/notices.sh`) in a `swath-X.Y.Z-OS-ARCH/` directory, built as
above with the release's version and commit stamped; an SPDX SBOM; and `SHA256SUMS`, signed. Verify before running
([RELEASING.md](../../RELEASING.md#verifying-a-release) has the cosign and attestation commands):

```bash
tar -xzf swath-X.Y.Z-linux-amd64.tar.gz
sha256sum --check --ignore-missing SHA256SUMS
./swath-X.Y.Z-linux-amd64/swath --version    # swath-go X.Y.Z (REVISION)
```

The same release publishes multi-arch (linux/amd64, linux/arm64) images, built from `Dockerfile.go` with the
archives' linux binaries, and the archives' three legal files in `/licenses/`:

| Image | Entrypoint | Base |
|---|---|---|
| `ghcr.io/varveio/swath` | `/opt/swath/swath` | distroless static (CA certificates for HTTPS, zone data); no shell |
| `ghcr.io/varveio/swath-replay` | `/opt/swath-replay/swath-replay` ([replay.md](replay.md#container-image)) | `scratch` |

Tags are `X.Y.Z`, `X.Y` and `latest` (and `X` from 1.0); a release candidate publishes only `X.Y.Z-rc.N`. Pin
the digest for anything that must not move. Both run as `10001:10001` with no home directory, so mount a
directory that user can write for `-o`, and pass credentials through the environment (or mount a file and
point the provider's variable at it, such as `AWS_SHARED_CREDENTIALS_FILE`):

```bash
docker run --rm -v "$PWD/out:/out" ghcr.io/varveio/swath:X.Y.Z \
  list s3://bucket/prefix/ --anonymous --region us-east-1 -o /out/inventory
```

`docker build -f Dockerfile.go -t swath:dev .` builds the lister image from a checkout (`--target swath-replay`
for the replay server); `--build-arg SWATH_REVISION="$(git rev-parse HEAD)"` stamps the commit, as the context
carries no `.git`. The build needs the network: it downloads the modules and generates `THIRD_PARTY_NOTICES`
with go-licenses, which fails the build on a module whose license is not allowed
([license gate](testing.md#license-gate)).

## Credentials

Use the provider's supported credential sources; do not put secrets in shell history.
Azure also accepts a SAS flag or source query and redacts it from records; see the
[Azure credential and namespace reference](internals/providers/azblob.md).
Without `--anonymous`, missing or unusable credentials stop the run before listing; swath never
falls back to unsigned requests. Credential failures normally have cause `auth` (exit 1). During
Azure namespace admission, failed Entra token acquisition can instead leave the namespace unknown:
without `--azblob-flat`, this is `refused`, cause `unsupported` (exit 2). See
[Azure admission](internals/providers/azblob.md#provider-implementation).

**S3.** The AWS SDK's default chain: environment keys, the shared `config` and `credentials` files
(`--profile`, `AWS_PROFILE`), IAM Identity Center (SSO), web identity, `credential_process`, AssumeRole
profiles, container credentials (ECS, EKS Pod Identity), EC2 instance roles (IMDSv2).

- Requests go unsigned only with `--anonymous`, which ignores every credential: a public bucket needs it.
- The chain's first lookup is waited for up to 30 s (a web-identity token file mounted late included). A
  lookup that fails, an empty chain among them, stops the run with cause `auth` before any listing request.
  Later refreshes that fail transiently (an STS 5xx or throttle) are retried with each request.
- Refused before any request as configured but broken: an access key without its secret or the reverse, a
  session token without both keys, `AWS_ROLE_ARN` without `AWS_WEB_IDENTITY_TOKEN_FILE`, a profile that does
  not exist.
- Region: `--region`, else `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region`, else `us-east-1`. A
  wrong region is an error that names the bucket's region. Redirects are not followed. STS calls use the
  environment's or profile's region, not `--region`.
- Requester-pays buckets: `--request-payer`. Endpoint variants: `--fips`, `--dualstack`; S3-compatible stores:
  `--endpoint URL`, addressed path-style unless `--path-style=false`.
- Bearer tokens instead of SigV4 (an S3-compatible endpoint that takes OAuth tokens, such as GCS's XML API):
  `--bearer-token-command 'gcloud auth print-access-token'`, refreshed every 45 m
  (`--bearer-token-refresh-interval`). A failing command fails the run with cause `auth`. The command is never
  recorded, so give it again to `swath resume DIR` ([cli.md](cli.md), "Bearer tokens").
- Not supported: access points and Multi-Region Access Points, Outposts, S3 directory buckets (a name ending
  in `--x-s3` is refused, exit 2).

**GCS.** `--gcs-credentials FILE` (Google credential JSON, including service-account and authorized-user
files), else Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud file, the metadata server on Google Cloud).
Requests go unauthenticated only with `--anonymous` (a public bucket needs it). Credentials that cannot be
found or loaded (no ADC anywhere, a `GOOGLE_APPLICATION_CREDENTIALS` that names a missing file) are refused
(`auth`) before any listing request. The token scope is
`devstorage.read_only`. Requester-pays buckets: `--gcs-user-project PROJECT` bills that project; it is part of
the resume identity.

S3 uses one transport; GCS and Azure Blob use a pool sized by workers. HTTP/2 is available where
the endpoint and transport support it. HTTP/1.1 at an S3 endpoint is endpoint behavior, not a client
guarantee, including for S3-compatible stores. See [pool construction](internals/clients.md#pool-construction).

The report's `source.stats` records `anonymous`, `credentials` (where they came from) and, for S3, `region`.

## Object-store requests

S3, GCS and Azure Blob share `--anonymous`, `--endpoint`, `--request-timeout`, `--limiter`
and `--request-rate`. Without `--anonymous`, missing or unusable credentials stop the run;
see [credentials](#credentials) for the Azure admission exception to cause `auth`. `--anonymous`
ignores configured credentials. Endpoint URLs must use HTTP or HTTPS, have a host, and contain
no user information. Provider pages describe additional endpoint rules and credential diagnostics.

A request has a 30 s timeout by default. Retryable failures get up to nine attempts with
exponential backoff (100 ms base, 5 s maximum). For engine listing calls, retry exhaustion
returns the page to the range engine for requeue. Azure namespace admission runs before the
engine exists: exhausted admission checks can refuse the run when flatness remains unknown,
unless `--azblob-flat` asserts it. `--stall-timeout` and `--max-duration` bound a run that cannot
make progress. Retryable status codes and server-directed waits differ by provider.

The adaptive limiter starts at `--workers` (64 by default). `--request-rate N` adds a
cap on every HTTP attempt, retries and probes included (default 0: no cap). See
[throttling](#throttling) for controller behavior and tuning, and
[request admission](internals/clients.md#request-admission) for implementation details and counters.

Provider support and differences: [S3](internals/providers/s3.md#support-and-differences),
[GCS](internals/providers/gcs.md#support-and-differences),
[Azure Blob](internals/providers/azblob.md#support-and-differences).

## Sizing a run

| Resource | Flag | Default | What to know |
|---|---|---|---|
| Listing concurrency | `--workers` | 64 for an object store, whatever the CPU count; 4 per CPU for a local path, at most 64 | an object-store listing waits on the service, not the CPU; it is also the request limiter's ceiling. A local walk waits on the disk: more workers past a few per CPU only add memory and contention (raise it for a network filesystem) |
| Row budget | `--row-budget` | 128 MiB | bounds the bytes of rows and scheduler metadata in flight (see Memory below). Not a cap on resident memory. After listing it also sizes the segment tail: each segment worker is planned at `resources.drain.segments.per_worker_bytes` of a second budget (`free_bytes`); when `workers_by` is `row_budget`, a budget larger by (workers wanted − `workers`) × `per_worker_bytes` restores the workers the encoders and cores allow |
| Encoders | `--encoders` | GOMAXPROCS, reduced by resource admission | default Parquet counts are limited by the schema's calculated working set and the descriptor limit; an explicit `--encoders N` bypasses these limits with a warning on a shortfall. The default count may rise after listing; an explicit count is never raised. See [encoder admission](internals/ordering.md#the-budget) and [per-schema buffer sizes](internals/parquet-output.md#writer-buffers-and-admission). For `.zst` text files the flag controls parallel compression jobs (see Memory). |
| Spill disk | `--spill-budget` | 0: half the free space on the spill filesystem at start, after the reserve | rows listed ahead of a slow frontier go to `DIR/.swath/spill/`; a text file's to a scratch directory beside it, `.NAME.swath-ATTEMPT-RANDOM/spill/` (stdout's under `$TMPDIR`, as a stream has no directory; `swath du` and `swath diag count` never spill); past the cap, or at the reserve, listing waits instead of failing. The reserve is derived: one part per encoder at full size, at least 1 GiB (report: `resources.spill.reserve_bytes`; `swath diag list --spill-reserve` overrides it) |
| Part size | `--part-size` | 64 MiB | target Parquet bytes per part |

**Disk for the worst case** is the dataset itself plus the spill cap plus the reserve, all on the output's
filesystem (spill lives under the output directory, or beside a text file). The spill cap includes the
durable spill a journaled run keeps for a resume (`DIR/.swath/spill/durable/`): a certified file stays on
disk, charged to the cap, until the published dataset covers its keys, and when the cap is reached the
oldest file already consumed is evicted (its keys are then relisted after a crash); the report's
`resources.spill.durable.retained_bytes` and `peak_retained_bytes` show it. A text file's scratch directory
is its attempt's own (created exclusively, named for the file and the attempt), so two runs writing sibling
files, or the same one, never share or remove each other's; the attempt removes it when it ends, after
success or failure. Only a killed run leaves one behind, with its `.NAME.*.tmp`: delete both. Estimate the dataset from a representative prefix: the
report's `output.output_bytes` over `output.rows_committed`; `resources.free_disk_min_bytes` is the least free space the run saw. If the dataset outgrows the disk the run stops with exit
74 and resumes once there is space. Spill backs off rather than fail when a frame does not fit (the run
waits), but not always: a range's closing trailer that does not fit, or any other spill I/O error, stops the
run (resumable; [ordering](internals/ordering.md#spill)).

**Memory.** A run holds:

- **Rows in flight, bounded by `--row-budget`:** rows listed ahead of the publish frontier and the
  encoders' queues, with the scheduler's metadata (about 150 bytes per waiting range; its two bounds are
  not charged, and no range keeps the page it was cut from alive, see
  [ordering](internals/ordering.md#what-a-waiting-range-retains)). When listing ends the tree holds about
  one budget (spill keeps it there), and the segment tail's queues and frames may take one more beside it:
  about two row budgets in all (`resources.drain.segments.tree_bytes` beside `max_held_bytes`).
- **Pages in flight, outside it:** accepted success bodies are limited to 16 MiB for S3/GCS and
  64 MiB for Azure. These byte limits do not bound the backing buffer capacity or decoded rows and
  strings. Worker requests, seed listings and probes can overlap. Include those allocations, the
  encoders' writers and codec, compression scratch and garbage collector headroom when sizing the host.
  See [body handling](internals/clients.md#what-swath-keeps) and
  [writer buffers](internals/parquet-output.md#writer-buffers-and-admission).
- **A local listing's largest directory, outside it:** a listing reads each directory whole to sort it,
  for each directory read at once; the names and keys grow with their lengths (1M files in one directory,
  names of about 13 bytes: about 140 MiB peak measured). A count (`swath du`, `swath diag count`) does not
  sort, and on Linux reads large directories a slab at a time; the portable engine (macOS, Windows) reads
  each directory whole for a count too.
- **A text file's compressor, outside it:** zstd compresses in parallel jobs, as many as `--encoders`
  (default GOMAXPROCS, lowered to what 16 MiB each of an allowance equal to `--row-budget` carries; one
  job compresses synchronously on the order stage). A job is a 4 MiB input section (four 1 MiB windows:
  sorted listings match between neighboring keys, and a 1 MiB window measured the ratio and speed of
  the 8 MiB default at a seventh of the memory), its output, the overlap the next job takes and its
  encoder; measured at 5 to 13 MiB a job. Gzip is one stream. The report's `output.text.compressor`
  names the jobs and why (`jobs_reason`: `cores`, `row_budget`, `encoders` when asked, `gzip`). A file's
  rows are also formatted by `--encoders` goroutines (`output.text.formatters`) into one ordered stream;
  their queued batches are rows in flight, charged to `--row-budget`, and the chunks they format are about
  the same size.
- **Spill, on disk:** rows ahead of the frontier past the budget, within `--spill-budget`. On tmpfs (a
  common `/tmp`) spill is memory charged to the host or container but not to the process's RSS: the
  report names the filesystem (`resources.spill.fs_type`) and the run warns at start.

A count (`swath du`, `swath diag count`) holds only pages in flight and the ranges queued for a worker: it counts rows as they
arrive and never orders, buffers or spills them (`resources.features.order` false, `max_buffered_bytes` 0, no `spill`).
`swath du` also holds one entry per distinct prefix (about 150 bytes each), capped by `--max-prefixes`.

Read the actual figure from the report's `resources.peak_rss_bytes` on a representative run, and size the
host from that, not from `--row-budget`. Beside it, `resources.peak_heap_live_bytes` (the peak live heap,
sampled every 100 ms) against `invocation.row_budget_bytes` shows how much memory the budget does not see;
`peak_ahead_rows` is the most rows listed and not yet handed on at once (in memory or spilled), and
`frontier_stuck_max_ns` the longest stretch in which nothing was handed on (a held-up frontier).
Linux (VmHWM), macOS (`getrusage`) and Windows (peak working set) measure it with the CPU times; on another
platform `resources.rusage_available` is false and `peak_rss_bytes`, `timings.user_cpu_ms` and
`timings.sys_cpu_ms` are left out (the summary line prints `-`), never reported as zero.

## Running

```bash
swath list s3://bucket/prefix -o /data/inv-2026-11-06 \
  --report /var/run/swath/attempt-1.json --max-duration 6h
```

- **Output directory.** `-o DIR` must be empty or absent. A non-empty directory is refused (exit 2) unless
  `--overwrite`, which replaces only a previous dataset of this tool. For a local source the output may not be
  the listed root or contain it; a new or empty directory inside the root is accepted (the listing skips it),
  but `--overwrite` of an output inside the root is refused.
- **The outcome of record** is the exit code plus the report written to `--report FILE` when the attempt ends,
  on failure too. Give each attempt a fresh path outside the output directory (or remove the old file first).
  The report's `outcome` holds `class`, `exit_code`, `resumable`, `cause`, `complete`, `stop_reason` and, when
  the outcome could not be written to `DIR/error.json`, `unrecorded`; `attempt.id` identifies the attempt.
  `--report-format json` also prints the report on stdout; the default prints one line.
- **`--max-duration`** (default 0, no limit) stops the run with exit 124; an object-store run is resumable.
  The stall watchdog stops a service that answers only empty pages; `--max-duration` is the only stop for one
  that keeps returning rows or pages forever. Give every unattended attempt one.
- **Process deadline.** A stop is graceful; if the run has not stopped 15 s later the process writes its
  outcome and exits, at most 20 s after the stop. A supervisor's own kill deadline belongs above
  `--max-duration` plus 20 s. An attempt killed that way may have no report: treat it as unavailable, never as
  the outcome in an older `error.json`.
- **`--part-interval`** (default 30 s; 0: size only) closes an open part that holds rows once it is that old.
  It bounds how long rows wait in an open part, not the age of the durable recovery point (what a resume
  keeps): a closed part is still encoded, synced and published in order, and when no rows reach the output
  (a stalled frontier) nothing is open to close. `part_age_ms` in the progress file is the time since a part was
  last published; it says nothing about how old the rows in the newest part are, nor how much listed work
  a resume would repeat. With 0, part boundaries do not depend on timing.
  Once listing is done and the rest of a sorted run is written as parallel segments
  (`resources.drain.segments.reason` `engaged`), the interval closes no part: the tail is cut by size and row
  count only ([ordering](internals/ordering.md#segments-parts-written-in-parallel-after-listing)). The tail's
  workers are at most one per two processors (at least 8); with them the tail is CPU-bound on Parquet encoding
  and spill read-back (`resources.drain.segments.write_ns`, `read_ns`, `decode_ns`).
- **`--stall-timeout`** (default 10 min; 0: never) stops a run with no progress and no usable answer from the
  service for that long (exit 75). A warning is printed at half.

## Watching a run

**On a terminal.** swath draws one progress line on stderr, redrawn every second: phase, objects (with rows a
resume recovered), live and average keys/s, parts, requests in flight, elapsed time, API calls and a cost
estimate when a tariff is known. Off a terminal it is off unless asked: `--progress` appends the same record as a plain line every 30 s
(`--progress-interval` sets it, at least 1 s); a log never receives control sequences. `-q` turns the
automatic progress and the warnings off, `--no-progress` turns progress off everywhere. At the end, runs over
1.5 s, runs that wrote parts and runs that did not complete print Java's statistics block on stderr (objects,
time, keys/s, API calls per 1,000 objects, mean and peak in flight, throttles and retries when any, the
estimate, files, bytes and peak RSS); `--stats` forces it (also for a supervisor's log), `--no-stats` drops it.
An `INCOMPLETE (class)` first line names a stop, with the `swath resume DIR` command when it is resumable.
The one-line summary stays on stdout. Color: `--color auto|always|never`, auto honoring `NO_COLOR`.

`DIR/.swath/progress.json` is rewritten every 2 s (atomic rename) while the run holds the output: the live
counts, the request account (`requests`, `retries`, `inflight`, the limiter's `limit`, `cost_usd`) and the order
stage's resources. The report written at the end keeps the curve (`history`, up to 300 samples), the phase times
(`timings.seed_ms`, `listing_ms`, `drain_ms`, `publication_ms`), the request account (`requests`), the
resources and the provider's counters (`source.stats.counters.values`: attempt latency percentiles, the
limiter's steps, the split counters). Both schemas are listed once, under "Progress file" and "Run report" in
[cli.md](cli.md); the counters in [internals/counters.md](internals/counters.md). The signals to read:

**The drain after listing.** On a sorted run the provider can finish long before the last row is handed on:
parallel listing finishes ranges in roughly key-uniform order, so most rows wait ahead of the frontier (spilled)
until the leftmost range ends, and are then replayed in order. `timings.listing_done_at` is when the provider
finished; `listing_finished_at` when the order stage ended; `resources.drain.tail_ns` the interval between, the
part of the run in which only the drain was left. In it idle workers read and decode spilled frames ahead of
the drainer (`resources.drain.read_ahead`) and more encoders are admitted (above). What the drainer did:
`replay_read_ns` and `ahead_wait_ns` (the spill), `order_check_ns`, `sink_write_ns` (the encoders, with
`encoder_wait_ns`), over the whole run, not the tail alone; and in the tail, the workers'
`worker_idle_after_listing_done_ns`. A tail that is most of the run with `sink_write_ns` most of the drainer's
time is encoder-bound; with `replay_read_ns` or `ahead_wait_ns`, spill-bound.

| Signal | Healthy | Act when |
|---|---|---|
| `part_age_ms` | under a minute | several minutes: no part has been published for that long; a crash would list again at least what was listed since |
| `order.spill_live_bytes` | on a sorted run of a large list, most of the rows listed: today's ordered emission spills nearly every row once (a median 96 % of rows on lists of 20M keys or more, [measurements](measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)) | near `resources.spill.max_bytes` or the filesystem's free space: the next frames are refused and listing slows to the drain |
| `order.nodes`, `order.markers` | tens to hundreds | tens of thousands and rising: a deep speculative tree behind a slow frontier |
| `order.ahead_rows`, `resources.peak_ahead_rows` (report) | anywhere up to the rows listed: ranges finish in roughly key-uniform order, so most rows of a large sorted run wait ahead of the frontier until listing nearly ends (a median 71 % at peak on lists of 20M keys or more). The set is bounded by `--spill-budget`, not by `--row-budget`; a count holds none | not by itself; read it with `resources.spill.refused`, `budget_wait_ns` and `frontier_stuck_max_ns` |
| `resources.spill.refused` (report) | 0 | above 0: the spill cap or the disk's reserve refused frames and emitters waited; with a large `budget_wait_ns` the listing ran at the drain's rate (with plain spill frames, before the delta coding, a cap of 2 × the row budget left 56 of 64 large lists unfinished after 900 s) |
| `resources.frontier_stuck_max_ns` (report) | a few attempt round trips: a median 0.28 s and at most 1 s on the campaign's sorted runs of 20M keys or more at 50 ms pages, spill-capped ones included | seconds to minutes: the range at the frontier stopped handing rows on (a slow, retried or hung request); an interrupted run reports its last stall too. A spill-capped run that crawls shows in `budget_wait_ns`, not here |
| `resources.peak_heap_live_bytes` against `invocation.row_budget_bytes` (report) | a few times the budget plus the pages in flight | far beyond: memory the budget does not see |
| `resources.spill.fs_type` (report) | a disk filesystem | `tmpfs`: spill is memory; write the output (for stdout, point `TMPDIR`) to a disk |
| `output.text.sink_wait_ns` against `timings.listing_ms` (report) | a fraction | near it: the order stage spends the run handing rows on (one formatter: formatting them; more: their queue full); rows wait behind it, spill grows and workers idle. More `--encoders`, if the budget carries them; `output_wait_ns` near the listing time instead: the compressor or the disk is the limit |
| `objects` against `encoded`, both per second | similar | listed far ahead of encoded: work fetched that cannot be published yet |
| `advance` | rising | flat while `answered` rises: alive but not progressing; `empty_chain_pages` rising: the service answers only empty pages, and the watchdog stops the run (75, `stuck_empty_chain`) after `--stall-timeout` |
| `throttles` | flat | rising steadily: see Throttling |
| `inflight` against `limit` | `limit` near `--workers`, `inflight` near `limit` | `limit` falling: the service throttles; `inflight` far below `limit`: the engine has too little work in parallel |
| `resources.budget_wait_ns` (report) | near 0 with spill on | large: workers blocked on a full row budget |
| `mean_active` against mean in flight (report) | both near `--workers` | active high, in flight low: requests held by the limiter or admission |
| `inflight.p99_ns` against `inflight.p50_ns` (report) | a few times p50 | p99 tens of times p50: a tail of slow attempts (timeouts, a throttled route) |
| `inflight.latency_overflow` (report) | 0 | above 0: attempts of 18 min 20 s or longer; a percentile among them reads 9223372036854775807, open-ended, not a measured bound |
| `idle_demand_ns` against `busy_ns` (report) | small | comparable: workers sat idle with work wanted; the split policy could not hand out work (see the split counters) |
| exit 75 on the same output | never twice | a second 75 in a row needs a person |

## Throttling

By default, S3, GCS and Azure Blob share an adaptive request limit that starts at `--workers`,
reduces concurrency under sustained throttling and recovers as answers improve.
[Request admission](internals/clients.md#request-admission) owns its window, growth rules,
timeout and latency brakes, and counters.

- `--limiter=false` removes that limit. Seed listings and structure probes can run beside the
  workers, so more than `--workers` requests can be in flight.
- `swath diag list --limiter-slow-start` enables an experimental ramp from a small initial limit;
  it is off by default.
- `--request-rate N` adds an aggregate rate cap for a quota shared across a fleet. Reservations
  are spaced; scheduling delays can bunch actual sends.
- Exhausted engine listing calls requeue while the run lives; admission failures are described
  under [object-store requests](#object-store-requests). A stall watchdog or deadline can stop
  a run that keeps failing.

Measured on the synthetic S3 fake ([measurements](measurements.md#red-team-fixes-2026-11-07)): 1 % and 5 % random
throttle answers no longer move the limit, and a 300 requests/s cap holds it at the cap. Storms and sustained
throttling have been measured only under the earlier per-answer rule ([history](measurements.md#throttling-through-the-fault-proxy)).
If a run crawls with `throttles` rising and the report's
`limiter.decreases` high (`limiter.max_depth` near `--workers`: the limit fell to about 1), use
`--limiter=false` with `--request-rate` for a fixed-rate alternative. These measurements do not establish
fit for live provider throttling; [request admission](internals/clients.md#request-admission) describes the tuning limits.

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
value wins): `--workers`, `--encoders`, `--row-budget`, `--spill-budget`, the diagnostic
`--spill-reserve`, `--spill-compress` and `--spill-plain`, `--part-interval`, `--report-format`, `--report`,
`--max-duration`, `--stall-timeout`, `--seed-from`, the terminal flags, and the operational ones: the shared
`--anonymous`, `--request-timeout`, `--limiter`, `--limiter-slow-start` and `--request-rate`, and each
provider's credential flags (`--azblob-sas`, `--gcs-credentials`, `--profile`, `--bearer-token-command`,
`--bearer-token-refresh-interval`) (`swath resume --help` prints the list). Any other flag is a usage error. `--max-duration` and the
other runtime values are replayed from the first attempt unless given again; pass a fresh `--report` path each
time.

The resume keeps every published part, verifies each against its journal record (size and SHA-256: reads every
retained part in full, measured at 247-376 MiB/s cold and 511 MiB/s warm, so 10,000 parts of 64 MiB take
roughly 30-45 minutes; progress shows as `verified_bytes`), then continues after the last published key K.
Rows listed but not yet in a published part are listed again, except those in a **certified spill file**:
a range that spilled ahead of the frontier sealed its file with a certificate (every row of a key interval,
as listed), and the resume replays each certificate it can validate instead of listing it, then lists only
the gaps between them ([internals/ordering.md](internals/ordering.md#durable-spill)). The report says what it
reused: `resources.spill.durable.resume` (`accepted`, `replayed_rows`, `gaps`, `requests_saved_est`,
`rejected` by reason).

**Retained observation.** A replayed certificate serves its rows as the interrupted attempt listed them: a key
deleted from the bucket since stays in the dataset, one added inside a certified interval is missed, and
metadata and filtering are as observed then. The gaps, and every key a run lists, reflect the bucket as it is
during the resume. On a static source the dataset is byte-identical to an uninterrupted run either way. A run
that must see the bucket as of the resume starts over with `--overwrite` (or resumes after removing
`DIR/.swath/spill/durable/`, which relists everything above K). Durable spill needs a journal of version 8 or
later (this build writes and resumes version 9 only), spill on, `--spill-budget` of at least
16 MiB, and a parallel range-engine provider that declares coverage fences (S3, GCS and Azure Blob;
not `--sequential`).

**Identity check.** The source, the route (endpoint, addressing, signing region; for GCS the API base and the
billed project), `--fetch-owner`, the schema and the part size are frozen. A resume whose environment or
`--profile` resolves another region or endpoint is refused (exit 2, `refused`) and the directory is left as it
was. Restore the original environment, or start over.

**Refused resumes** and what they mean:

| Message or class | Meaning | Action |
|---|---|---|
| exit 2, identity mismatch | the resume would list another route | fix the environment, or `swath list --overwrite` |
| exit 2, nothing to resume | no journal in the directory | start a fresh `swath list` |
| exit 1, another swath process owns this output directory | a process still holds the output lock: the killed run has not exited yet, or a wrapper was killed and swath under it lives on. The resume is refused, not queued, and changes nothing | wait for that process to exit (stop a wrapper's whole process group, not the wrapper alone), then resume |
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

### Seeding a refresh from the previous dataset

A scheduled relisting of the same bucket should pass the previous generation as `--seed-from`:

```bash
swath list s3://bucket/logs/ -o /data/logs-2026-11-08 --seed-from /data/logs-2026-11-07
```

The previous dataset's parts and key page index divide the scope into ranges of about equal expected keys, so the
run skips the delimiter seed and most early splits, and its requests approach one page per 1,000 keys. The prior
must be the same source and scope: the manifest's source kind, bucket/container and prefix are compared
with the run's (exit 2 on a mismatch; a dataset of `logs/` does not seed `logs/2026/`). The account URI
is not compared for Azure; verify that the prior belongs to the same Azure account yourself. Endpoint
and region are not checked either. A bad prior is refused before listing pages, but Azure namespace
admission requests can already have run. It cannot be the output directory, and `--overwrite` of the prior's
own directory is refused for that reason: write each generation to a new directory, as above. A bucket that
changed shape since is still listed whole and in order; the run report's `seed.from.error.grown` and
`seed.from.error.shrunk` count the ranges whose keys moved by more than a factor of 2 (adaptive splits covered
them), and `seed.from.expected_keys` against the source counter `objects` is the growth. A cut file (one sorted key per line,
optionally a tab and a count) seeds from anything else that knows the keys, but carries no identity; prefer the
dataset.

## Reading the output

```text
DIR/
  data/part-NNNNN.parquet   key-sorted, disjoint parts, name order == key order; at most 100,000 parts
                            (exit 1, `cause` `part_limit` past that: start over with a larger `--part-size`)
  manifest.json             the commit: written last, names every part with exact key bounds and coverage
  run.json                  the run report (after the commit)
  error.json                the outcome of an attempt that did not complete
  .swath/                   private workspace: lock, journal, parts/, spill/, suspect/, progress.json, stack dumps
```

**The manifest is the commit.** Parts appear in `data/` before the run ends, so `data/` alone is not a
dataset: a reader starts only once `manifest.json` exists, and a reader that needs the whole bucket checks
`coverage.complete` true, `coverage.stop_reason` `completed`, `coverage.skipped_entries` 0, `coverage.filters`
empty and `scope.kind` `whole` (fields: [dataset.md](dataset.md#the-swath-extension)).

```bash
swath show DIR            # SeekParquet format, swath schema and metadata version, coverage, parts, rows, key range
swath show --journal DIR  # an interrupted run: the journal as JSON lines
# In DuckDB, substitute the exact S3 part paths from manifest.json:
duckdb -c "SELECT count(*), sum(size) FROM read_parquet(['DIR/data/part-00000.parquet'], union_by_name = true)"
duckdb -c "SELECT storage_class, count(*), sum(size) FROM read_parquet(['DIR/data/part-00000.parquet'], union_by_name = true) GROUP BY 1"
```

Use every part listed in the manifest, not a directory glob. The query above shows a one-part S3 dataset.

The [schema registry](dataset.md#schema-registry) owns the columns, nullability and metadata
semantics for S3, GCS, Azure Blob and local datasets. Read optional columns by name across parts
(`union_by_name = true` in DuckDB). Listed ETags are not portable content hashes; check the
provider-specific hash caveats there before comparing inventories.

By default, object-store datasets list current objects only; `--all-versions` selects S3 versions
or GCS generations. In default GCS mode, generation metadata describes the live object, not its
history. Java swath's reader does not open Go datasets.

### Verify a dataset

`python3 verify.py DIR` independently checks S3, GCS, Azure Blob and local datasets with DuckDB,
including manifest part bounds, footer stamps and SHA-256 digests. For Azure, it also checks the
`azblob` source kind and every part's column types and nullability, including UTC microsecond timestamps.

## Listing every version

```bash
swath list --all-versions s3://BUCKET[/PREFIX] -o DIR   # every version and delete marker (ListObjectVersions)
swath list --all-versions gs://BUCKET[/PREFIX] -o DIR   # every generation, live and noncurrent (versions=true)
```

**Rows.** One row per S3 version or delete marker, or per GCS generation, under the
[version schemas](dataset.md#schema-registry). A delete marker's absent metadata is null, never 0.
`version_id` is verbatim: `"null"` is a real ID (an object written while versioning was off). The current
objects are `is_latest AND NOT is_delete_marker` (S3) or `time_deleted IS NULL` (GCS); a listing of the current
objects alone is the default mode, which is cheaper.

**Order.** Keys ascend; a key's rows are consecutive, in the service's order within the key: S3 newest first,
versions and delete markers interleaved; GCS by generation, ascending. S3 can list two entries of a key with the
same `last_modified` (to the second), so that time does not rebuild S3's order: the file order does.
In an unfiltered, complete listing, the first row of an S3 key's group is the one with `is_latest`, and a
delete marker there means the object is deleted. On GCS, the last row of a name's group is the newest
generation; it is live only if `time_deleted` is null, and a GCS name may have no live generation. A filter can
remove a key's latest version, so in a filtered dataset read `is_latest` and `time_deleted` per row instead of
by position.

**Parts never split a key.** A part's size target, `--part-rotation-max-rows` and `--part-interval` cut at the
next key, so a part can pass them by one key's history; parts stay key-disjoint, and a resume never lists half a
key again. The interval no longer bounds how long a part stays open while one key's rows are still arriving.

**Parallelism.** Ranges split only between keys, so one key with a very long history is listed by one worker,
page after page, while the rest of the bucket proceeds (`groups.open_pages`, `split.skipped.group_open` in the
report). S3's versions call is slower than ListObjectsV2: about 1.85 times the time per request at the median of
our measurements, and pages of old versions are slow to first byte; history multiplies the pages besides. GCS
lists generations at the cost of live objects.

**Permissions and refusals.** S3 needs `s3:ListBucketVersions` (`s3:ListBucket` alone answers 403, and swath
never falls back to a current-object listing); GCS needs the same `storage.objects.list`. Text output adds
`VersionId` (a GCS generation in decimal), `IsLatest` (GCS: no `time_deleted`) and, on S3, `IsDeleteMarker`, and
omits a delete marker's `Size` and `ETag`. `--min-size` and `--max-size` never match a delete marker. A resume
continues in the mode its run started in; a run in the other mode is refused on that output. Directory buckets have
no versions and stay refused.

The run report's version counts describe this attempt after filtering: S3 `versions` and
`delete_markers` sum to emitted rows; GCS `versions` includes `noncurrent` generations.
`distinct_keys` counts surviving names, and `live_objects` counts live versions among those rows.
Retained rows from an earlier attempt are excluded. `groups.overfetch_rows` counts rows above a
range end fetched to finish its last group. Table output includes the version ID and latest state,
and for S3 the delete-marker state, in wire order.

## Known limits

- **Windows:** a journaled (object-store) run and `swath resume` are refused: there is no directory sync, so
  a power loss could lose acknowledged parts. The output lock holds. Filesystem runs work but are not
  resumable. The test suite runs there in CI; nothing is measured there.
- **macOS:** the test suite runs there in CI; nothing is measured there. APFS refuses names that are not
  UTF-8, so a filesystem run never meets one.
- **Power loss or a kernel crash:** recovery relies on the filesystem honoring `fsync` on files and directories
  (ext4, XFS, APFS do). Process crashes are tested at every crash point; power loss is not.
- **Live scans, not snapshots:** objects added, removed or changed during the run change what it observes. A
  committed dataset is the complete result of a live listing, exactly once per key (with `--all-versions`, once
  per version), not a point-in-time view.
- **Versioned listing** (`--all-versions`) was checked against an independent serial listing on small public
  buckets (seven S3, one GCS; up to 40,000 versions, pages of 1 to 1,000 entries, interrupted and resumed runs);
  large versioned buckets are unmeasured.
- **Memory:** `--row-budget` bounds rows in flight, not resident memory (see Sizing).
- **Scale:** key lists of up to 593M keys were measured through a key-list fake at 50 ms, and one live bucket
  of 1.09 billion objects at 256 and 1,024 workers; listings past that and runs of hours are unmeasured
  ([measurements](measurements.md#known-gaps)).

## When it goes wrong

1. **The exit code and the `--report` file.** `outcome.class` and `outcome.cause`: the provider's failure class
   (`auth`, `access_denied`, `throttle`, `timeout`, `parse`, `protocol`, `unsupported`), `no_space`, or
   a `stuck_*` cause. `outcome.unrecorded`, when set, says why `error.json` was not updated.
2. **`DIR/error.json`**: the same outcome inside the dataset, current only when its `attempt.id` matches the
   report's. Without a report, only the exit code is current. A refused command that could not record its
   outcome prints it on stderr as `swath: outcome: {...}`.
3. **Goroutine dumps.** At a stall, and at a forced exit, every goroutine's stack and the progress counts go
   whole to `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt` (replaced each time), else to stderr,
   cut at 1 MiB.
4. **`.swath/`**: `lock` (one process per output), `journal` (`swath show --journal`), `parts/` (encoded parts
   not yet published), `spill/` (disposable spill, removed by the next run, and `spill/durable/`: the
   certified spill files and their `ledger`, kept for the resume and removed when the dataset commits),
   `suspect/` (a part a resume must reproduce), `progress.json`. Do not edit the journal or its parts by
   hand; removing `spill/durable/` only costs a resume the relisting it would have saved.

**What a kill leaves on disk.** A SIGKILL (or an OOM kill, a power loss) leaves the published parts, the
journal, encoded parts not yet published, the disposable spill (removed by the next run) and the durable
spill: certified files with their ledger records, and files whose record never reached the ledger. The
resume validates every recorded file (size, checksums, every frame, its certificate) and unlinks every
other file; a missing or damaged one is relisted, never an error.
5. **Profiles:** `SWATH_CPUPROFILE=FILE` and `SWATH_MEMPROFILE=FILE` write pprof files.

For a report to the maintainers include `swath --version`, the `--report` file (redact bucket names and keys),
the provider and region, and the command line.
