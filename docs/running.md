# Running swath

How to size, run, watch, stop and resume a large listing, and keep a refreshed inventory. A short checklist
for whoever schedules runs is [production.md](production.md); what each exit code means is
[reference/exit-codes.md](reference/exit-codes.md).

```bash
swath list s3://bucket/prefix/ -o /data/inv-2026-11-10 \
  --seed-from /data/inv-2026-11-09 \
  --report /var/log/swath/inv-2026-11-10.attempt-1.json \
  --max-duration 6h
```

## Sizing a run

| Resource | Flag | Default | What to know |
|---|---|---|---|
| Listing concurrency | `--workers` | 64 for an object store, whatever the CPU count; 4 per CPU for a local path, at most 64 | an object-store listing waits on the service, not the CPU; it is also the request limiter's ceiling. A local walk waits on the disk: more workers past a few per CPU only add memory and contention (raise it for a network filesystem) |
| Sort buffer | `--sort-buffer` | 1/16 of memory, at most 1 GiB; 128 MiB when no limit is found | memory is the least of physical memory, the cgroup's limit (Linux, its ancestors' included) and `GOMEMLIMIT`; `invocation.sort_buffer` records which, and why the budget is what it is. It bounds the bytes of rows and scheduler metadata in flight (see Memory below). Not a cap on resident memory. After listing it also sizes the parallel tail: when `resources.drain.segments.workers_by` is `row_budget`, a larger budget buys more tail workers ([how](internals/ordering.md#segments-parts-written-in-parallel-after-listing)) |
| Encoders | `--encoders` | GOMAXPROCS, reduced by resource admission | default Parquet counts are limited by an encoder allowance of 1/16 of memory (at most 1 GiB, whatever `--sort-buffer` says), each encoder priced at the schema's calculated working set plus, writing to a bucket, its upload buffer (a GCS chunk, an S3 part or an Azure block), and by the descriptor limit; an explicit `--encoders N` bypasses these limits with a warning on a shortfall. The default count may rise after listing; an explicit count is never raised. See [encoder admission](internals/ordering.md#the-budget) and [per-schema buffer sizes](internals/parquet-output.md#writer-buffers-and-admission). For `.zst` text files the flag controls parallel compression jobs (see Memory). |
| Spill disk | `--spill-budget` | 0: half the free space on the spill filesystem at start, after the reserve | rows listed ahead of a slow frontier go to `DIR/.swath/spill/`; a text file's to a scratch directory beside it, `.NAME.swath-ATTEMPT-RANDOM/spill/` (stdout's in `$TMPDIR/swath-ATTEMPT-RANDOM/spill/`, as a stream has no directory; a listed tree holding either leaves it out; `swath du` and `swath diag count` never spill); past the cap, or at the reserve, listing waits instead of failing. The reserve is derived: one part per encoder at full size, at least 1 GiB (report: `resources.spill.reserve_bytes`; `swath diag list --spill-reserve` overrides it) |
| Part size | `--part-size`, `--part-rotation-max-rows` | 64 MiB; no row cap | target Parquet bytes per part; with `--part-rotation-max-rows N` a part also closes at N rows. Smaller parts suit readers that load whole parts, larger ones mean fewer files. A dataset holds at most 100,000 parts: a small part size on a very large listing can reach that (exit 1, `cause` `part_limit`; start over with a larger `--part-size`) |

### Disk

The worst case is the dataset itself plus the spill cap plus the reserve, all on the output's
filesystem (spill lives under the output directory, or beside a text file). The spill cap includes the
durable spill a journaled run keeps for a resume (`DIR/.swath/spill/durable/`): a certified file stays on
disk, charged to the cap, until the published dataset covers its keys, and when the cap is reached the
oldest file already consumed is evicted (its keys are then relisted after a crash); the report's
`resources.spill.durable.retained_bytes` and `peak_retained_bytes` show it. A text file's scratch directory,
and a stdout stream's under `$TMPDIR`, is its attempt's own (created exclusively, named for the file and the
attempt), so two runs writing sibling files, the same one, or streams never share or remove each other's; the attempt removes it when it ends, after
success or failure. Only a killed run leaves one behind, with its `.NAME.*.tmp`: delete both. Estimate the dataset from a representative prefix: the
report's `output.output_bytes` over `output.rows_committed`; `resources.free_disk_min_bytes` is the least free space the run saw. If the dataset outgrows the disk the run stops with exit
74 and resumes once there is space. Spill backs off rather than fail when a frame does not fit (the run
waits), but not always: a range's closing trailer that does not fit, or any other spill I/O error, stops the
run (resumable; [ordering](internals/ordering.md#spill)).

### Memory

swath sets no total-memory limit. `--sort-buffer` bounds the rows buffered for sorted output only; how many
writers run is admitted against an allowance of their own, 1/16 of the memory found (at most 1 GiB) whatever
`--sort-buffer` says ([encoder admission](internals/ordering.md#the-budget)), and `--encoders N` sets fewer.
`GOMEMLIMIT` is a soft target for Go-managed memory, not a limit on RSS; an OS or container limit can stop the
process when memory cannot be reclaimed. A run holds:

- **Rows in flight, bounded by `--sort-buffer`:** rows listed ahead of the publish frontier and the
  encoders' queues, with the scheduler's metadata (about 150 bytes per waiting range; its two bounds are
  not charged, and no range keeps the page it was cut from alive, see
  [ordering](internals/ordering.md#what-a-waiting-range-retains)). When listing ends the tree holds about
  one budget (spill keeps it there), and the segment tail's queues and frames may take one more beside it:
  about two sort buffers in all (`resources.drain.segments.tree_bytes` beside `max_held_bytes`).
- **Pages in flight, outside it:** accepted success bodies are limited to 16 MiB for S3/GCS and
  64 MiB for Azure. These byte limits do not bound the backing buffer capacity or decoded rows and
  strings. With the default page policy, Azure Blob asks for 5,000 entries a page with a counted
  `--seed-from` prior, against 1,000 otherwise: about five times the memory per page in flight, beside
  the same sort buffer. `swath diag` commands can override the page size with `--page-size`. Worker requests,
  seed listings and probes can overlap. Include those allocations, the
  encoders' writers and codec, compression scratch and garbage collector headroom when sizing the host.
  See [body handling](internals/clients.md#what-swath-keeps) and
  [writer buffers](internals/parquet-output.md#writer-buffers-and-admission).
- **A local listing's largest directory, outside it:** a listing reads each directory whole to sort it,
  for each directory read at once; the names and keys grow with their lengths (1M files in one directory,
  names of about 13 bytes: about 140 MiB peak measured). A count (`swath du`, `swath diag count`) does not
  sort, and on Linux reads large directories a slab at a time; the portable engine (macOS, Windows) reads
  each directory whole for a count too.
- **A `.zst` text file's compressor, outside it:** as many parallel jobs as `--encoders`, about 5 to 13 MiB
  each; lower `--encoders` to lower it. Gzip is one stream. The report's `output.text.compressor` names the
  jobs and why (`jobs_reason`: `cores`, `encoder_allowance`, `encoders` when asked, `gzip`); the rows being
  formatted for the file are rows in flight, charged to `--sort-buffer`
  ([how](internals/run-lifecycle.md#encoder-sizing)).
- **Spill, on disk:** rows ahead of the frontier past the budget, within `--spill-budget`. On tmpfs (a
  common `/tmp`) spill is memory charged to the host or container but not to the process's RSS: the
  report names the filesystem (`resources.spill.fs_type`) and the run warns at start.

A count (`swath du`, `swath diag count`) holds only pages in flight and the ranges queued for a worker: it counts rows as they
arrive and never orders, buffers or spills them (`resources.features.order` false, `max_buffered_bytes` 0, no `spill`).
`swath du` also holds one entry per distinct prefix (about 150 bytes each), capped by `--max-prefixes`.

Read the actual figure from the report's `resources.peak_rss_bytes` on a representative run, and size the
host from that, not from `--sort-buffer`. Beside it, `resources.peak_heap_live_bytes` (the peak live heap,
sampled every 100 ms) against `invocation.sort_buffer_bytes` shows how much memory the budget does not see;
`peak_ahead_rows` is the most rows listed and not yet handed on at once (in memory or spilled), and
`frontier_stuck_max_ns` the longest stretch in which nothing was handed on (a held-up frontier).
Linux (VmHWM), macOS (`getrusage`) and Windows (peak working set) measure it with the CPU times; on another
platform `resources.rusage_available` is false and `peak_rss_bytes`, `timings.user_cpu_ms` and
`timings.sys_cpu_ms` are left out (the summary line prints `-`), never reported as zero.

## Limits and the outcome of record

- **Output directory.** `-o DIR` must be empty or absent. A non-empty directory is refused (exit 2) unless
  `--overwrite`, which replaces only a previous dataset of this tool. For a local source the output may not be
  the listed root or contain it; a new or empty directory inside the root is accepted (the listing skips it),
  but `--overwrite` of an output inside the root is refused, and a non-empty one is refused saying so: write
  to a new directory outside the root.
- **The outcome of record** is the exit code plus the report written to `--report FILE` when the attempt ends,
  on failure too. Give each attempt a fresh path outside the output directory (or remove the old file first).
  The report's `outcome` holds `class`, `exit_code`, `resumable`, `cause`, `complete`, `stop_reason` and, when
  the outcome could not be written to `DIR/error.json`, `unrecorded`; `attempt.id` identifies the attempt.
  Its schema: [reference/run-report.md](reference/run-report.md).
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

## Cost

Object stores charge per listing request. An S3 or GCS request returns up to 1,000 keys, an Azure Blob one up
to 5,000. A run sends about one request per page of keys, plus the seed's delimiter listings, the probes that
pick split points, and retries; a run seeded from a previous dataset (`--seed-from`) comes close to one per
page, and on Azure Blob asks for pages of 5,000 with a counted prior and the default page policy. The run
report counts every HTTP attempt in `requests.http_attempts`, and on AWS S3 and GCS's own endpoints estimates
their cost in `cost.cost_usd` at the provider's reference list rate (0.005 USD per 1,000 requests); a custom endpoint and
Azure Blob have no estimate. The progress line shows the same estimate as the run goes. Pricing varies by
provider, region and time: check the provider's current price for a budget. A resume lists again what was
not yet published, except what it replays from certified spill, so an interrupted run costs more than an
uninterrupted one.

## Watching a run

**On a terminal.** swath draws one progress line on stderr, redrawn every second: phase, objects (with rows a
resume recovered), live and average keys/s, parts, requests in flight, elapsed time, API calls and a cost
estimate when a tariff is known. Off a terminal it is off unless asked: `--progress` appends the same record as a plain line every 30 s
(`--progress-interval` sets it, at least 1 s); a log never receives control sequences. `-q` turns the
automatic progress and the warnings off, `--no-progress` turns progress off everywhere. At the end, runs over
1.5 s, runs that wrote output and runs that did not complete print the statistics on stderr (objects,
time, keys/s, API calls, mean and peak in flight, throttles and retries when any, the estimate, the parts or
file written, bytes and peak RSS); `--stats` forces it (also for a supervisor's log), `--no-stats` drops it.
An `INCOMPLETE (class)` first line names a stop, with the `swath resume DIR` command when it is resumable.
stdout carries only rows, totals or `--report -`. Color: `--color auto|always|never`, auto honoring `NO_COLOR`.

`DIR/.swath/progress.json` is rewritten every 2 s (atomic rename) while the run holds the output: the live
counts, the request account (`requests`, `retries`, `inflight`, the limiter's `limit`, `cost_usd`) and the order
stage's resources. The report written at the end keeps the curve (`history`, up to 300 samples), the phase times
(`timings.seed_ms`, `listing_ms`, `drain_ms`, `publication_ms`), the request account (`requests`), the
resources and the provider's counters (`source.stats.counters.values`: attempt latency percentiles, the
limiter's steps, the split counters). Both schemas are in
[reference/run-report.md](reference/run-report.md); the counters in [internals/counters.md](internals/counters.md). The signals to read:

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
| `order.spill_live_bytes` | on a sorted run of a large list, most of the rows listed: today's ordered emission spills nearly every row once (a median 96 % of rows on lists of 20M keys or more, [measurements](internals/measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)) | near `resources.spill.max_bytes` or the filesystem's free space: the next frames are refused and listing slows to the drain |
| `order.nodes`, `order.markers` | tens to hundreds | tens of thousands and rising: a deep speculative tree behind a slow frontier |
| `order.ahead_rows`, `resources.peak_ahead_rows` (report) | anywhere up to the rows listed: ranges finish in roughly key-uniform order, so most rows of a large sorted run wait ahead of the frontier until listing nearly ends (a median 71 % at peak on lists of 20M keys or more). The set is bounded by `--spill-budget`, not by `--sort-buffer`; a count holds none | not by itself; read it with `resources.spill.refused`, `budget_wait_ns` and `frontier_stuck_max_ns` |
| `resources.spill.refused` (report) | 0 | above 0: the spill cap or the disk's reserve refused frames and emitters waited; with a large `budget_wait_ns` the listing ran at the drain's rate (with plain spill frames, before the delta coding, a cap of 2 × the row budget left 56 of 64 large lists unfinished after 900 s) |
| `resources.frontier_stuck_max_ns` (report) | a few attempt round trips: a median 0.28 s and at most 1 s on the campaign's sorted runs of 20M keys or more at 50 ms pages, spill-capped ones included | seconds to minutes: the range at the frontier stopped handing rows on (a slow, retried or hung request); an interrupted run reports its last stall too. A spill-capped run that crawls shows in `budget_wait_ns`, not here |
| `resources.peak_heap_live_bytes` against `invocation.sort_buffer_bytes` (report) | the budget plus the encoders' allowance and the pages in flight | far beyond: memory the budget does not see |
| `resources.spill.fs_type` (report) | a disk filesystem | `tmpfs`: spill is memory; write the output (for stdout, point `TMPDIR`) to a disk |
| `output.text.sink_wait_ns` against `timings.listing_ms` (report) | a fraction | near it: the order stage spends the run handing rows on (one formatter: formatting them; more: their queue full); rows wait behind it, spill grows and workers idle. More `--encoders`, if the budget carries them; `output_wait_ns` near the listing time instead: the compressor or the disk is the limit |
| `rows` against `encoded`, both per second | similar | listed far ahead of encoded: work fetched that cannot be published yet |
| `advance` | rising | flat while `answered` rises: alive but not progressing; `empty_chain_pages` rising: the service answers only empty pages, and the watchdog stops the run (75, `stuck_empty_chain`) after `--stall-timeout` |
| `throttles` | flat | rising steadily: see Throttling |
| `inflight` against `limit` | `limit` near `--workers`, `inflight` near `limit` | `limit` falling: the service throttles; `inflight` far below `limit`: the engine has too little work in parallel |
| `resources.budget_wait_ns` (report) | near 0 with spill on | large: workers blocked on a full sort buffer |
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

- `--no-adaptive-limit` removes that limit. Seed listings and structure probes can run beside the
  workers, so more than `--workers` requests can be in flight.
- `swath diag list --adaptive-limit-slow-start` enables an experimental ramp from a small initial limit;
  it is off by default.
- `--request-rate N` adds an aggregate rate cap for a quota shared across a fleet. Reservations
  are spaced; scheduling delays can bunch actual sends.
- Exhausted engine listing calls requeue while the run lives; admission failures are described
  under [object-store requests](sources.md#object-store-requests). A stall watchdog or deadline can stop
  a run that keeps failing.

Measured on the synthetic S3 fake ([measurements](internals/measurements.md#red-team-fixes-2026-11-07)): 1 % and 5 % random
throttle answers no longer move the limit, and a 300 requests/s cap holds it at the cap. Storms and sustained
throttling have been measured only under the earlier per-answer rule ([history](internals/measurements.md#throttling-through-the-fault-proxy)).
If a run crawls with `throttles` rising and the report's
`limiter.decreases` high (`limiter.max_depth` near `--workers`: the limit fell to about 1), use
`--no-adaptive-limit` with `--request-rate` for a fixed-rate alternative. These measurements do not establish
fit for live provider throttling; [request admission](internals/clients.md#request-admission) describes the tuning limits.

## Stopping and resuming

A journaled dataset run of an object store that stops with exit 74, 75, 124, 130 or 143 can be resumed; what a
supervisor does with each code is in [reference/exit-codes.md](reference/exit-codes.md#what-a-supervisor-does-with-each-code).

"Resumable" means an object-store run whose journal is intact. Filesystem and Azure HNS tree runs keep no
journal: start them again in a new or empty directory (`--overwrite` refuses interrupted output that is not a dataset).

**Signals.** SIGINT and SIGTERM stop the run gracefully: published parts and the journal stay, and
`error.json` and the report are written. Only the first signal counts; the forced exit follows 15 s later. SIGKILL, a panic or an
OOM kill loses nothing durable: the journal is crash-safe and the resume reconciles.

**The workspace.** `DIR/.swath/` holds `lock` (one process per output), `journal` (`swath show --journal`),
`parts/` (encoded parts not yet published), `spill/` (disposable spill, removed by the next run, and
`spill/durable/`: the certified spill files and their `ledger`, kept for the resume and removed when the
dataset commits), `suspect/` (a part a resume must reproduce) and `progress.json`. Do not edit the journal or
its parts by hand; removing `spill/durable/` only costs a resume the relisting it would have saved.

A SIGKILL (or an OOM kill, a power loss) leaves the published parts, the journal, encoded parts not yet
published, the disposable spill and the durable spill: certified files with their ledger records, and files
whose record never reached the ledger. The resume validates every recorded file (size, checksums, every frame,
its certificate) and unlinks every other file; a missing or damaged one is relisted, never an error.

**`swath resume DIR`** replays the command recorded in the journal. Only the resource, request, credential
and reporting options that `swath resume --help` lists may be given again, the last value winning
([swath-resume](commands/swath-resume.md)); `--max-duration` and the other runtime values are replayed from the
first attempt unless given again. Pass a fresh `--report` path each time.

The resume keeps every published part, verifies each against its journal record (size and SHA-256: reads every
retained part in full, measured at 247-376 MiB/s cold and 511 MiB/s warm on a 4-vCPU VM
([measurements](internals/measurements.md#recovery-and-part-count)), so 10,000 parts of 64 MiB take
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
billed project), `--s3-fetch-owner`, the schema and the part size are frozen. A resume whose environment or
`--s3-profile` resolves another region or endpoint is refused (exit 2, `refused`) and the directory is left as it
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

A scheduled relisting of the same bucket should pass the previous generation as `--seed-from` (S3, GCS and Azure Blob):

```bash
swath list s3://bucket/logs/ -o /data/logs-2026-11-08 --seed-from /data/logs-2026-11-07
```

The previous dataset's parts and key page index divide the scope into ranges of about equal expected keys, so the
run skips the delimiter seed and most early splits. With the default page policy, its requests approach one
per 1,000 keys on S3/GCS and one per 5,000 keys on Azure Blob with a counted prior. The prior
must be the same source and scope: the manifest's source kind, bucket/container and prefix are compared
with the run's (exit 2 on a mismatch; a dataset of `logs/` does not seed `logs/2026/`). The account URI
is not compared for Azure; verify that the prior belongs to the same Azure account yourself. Endpoint
and region are not checked either. A bad prior is refused before listing pages, but Azure namespace
admission requests can already have run. It cannot be the output directory, and `--overwrite` of the prior's
own directory is refused for that reason: write each generation to a new directory, as above. A bucket that
changed shape since is still listed whole and in order; the run report's `seed.from.error.grown` and
`seed.from.error.shrunk` count the ranges whose keys moved by more than a factor of 2 (adaptive splits covered
them), and `seed.from.expected_keys` against the source counter `objects` is the growth.

A cut file seeds from anything else that knows the keys (an S3 Inventory, a prefix list), but carries no
identity; prefer the dataset. A cut file holds one key per line, ascending bytewise without repeats, each
inside the scope, optionally followed by a tab and the keys expected since the previous cut (on every line or
none), at most 262,144 lines and 16 MiB (thin a larger one: every nth line, its count the sum of those it
replaces). An unsorted or repeated cut, a cut outside the scope, a cut file past those bounds, `--seed-from`
on a local tree or an Azure account with a hierarchical namespace (listed directory by directory, not in key
ranges), and a prior that is the output directory are refused (exit 2).

The prior changes only how the work is first divided, into ranges of about equal expected keys
([warm start](internals/range-engine.md)); the splits stay adaptive and the rows are the same. `swath resume` reads `--seed-from`
again and may change or drop it (`--seed-from=`); a resume whose prior is unreadable warns and lists without
it. The report's `invocation.prior` names what was used.

## Reading the output

A reader starts only once `manifest.json` exists and reads the parts it lists (never a glob of `data/`). An
application that needs the whole source checks the coverage: `coverage.complete` true, `coverage.stop_reason`
`completed`, `coverage.skipped_entries` 0, `coverage.filters` empty, `scope.kind` `whole`, the `source` it
expects, any `coverage.exclusions` accepted explicitly, and a missing field read as unknown
([dataset.md](dataset.md#the-swath-extension)). [getting-started.md](getting-started.md#6-query-it-with-duckdb)
shows a DuckDB query over the manifest's part list. Listed ETags are not portable content hashes; check the
provider's caveats in the [schema registry](dataset.md#schema-registry) before comparing inventories.

```text
DIR/
  data/part-NNNNN.parquet   key-sorted, disjoint parts, name order == key order; at most 100,000 parts
  manifest.json             the commit: written last, names every part with exact key bounds and coverage
  run.json                  the run report (after the commit)
  error.json                the outcome of an attempt that did not complete
  .swath/                   private workspace: lock, journal, parts/, spill/, suspect/, progress.json, stack dumps
```

By default, object-store datasets list current objects only; `--all-versions` selects S3 versions, GCS
generations or every Azure Blob entry (below). The 0.3 release's reader does not open these datasets ([upgrade.md](upgrade.md)).

### Verify a dataset

`python3 verify.py DIR` (at the repository root) independently checks S3, GCS, Azure Blob and local datasets with DuckDB,
including manifest part bounds, footer stamps and SHA-256 digests. For Azure, it also checks the `azblob`
source kind and every part's column types and nullability, including UTC microsecond timestamps.

## Listing every version

```bash
swath list --all-versions s3://BUCKET[/PREFIX] -o DIR   # every version and delete marker (ListObjectVersions)
swath list --all-versions gs://BUCKET[/PREFIX] -o DIR   # every generation, live and noncurrent (versions=true)
swath list --all-versions az://ACCOUNT/CONTAINER[/PREFIX] -o DIR  # versions, snapshots, soft-deleted blobs
```

**Rows.** One row per S3 version or delete marker, per GCS generation, or per Azure Blob entry (a version,
a snapshot, a soft-deleted blob, or the current blob), under the [version schemas](dataset.md#schema-registry). A delete marker's absent metadata is null, never 0.
`version_id` is verbatim: `"null"` is a real ID (an object written while versioning was off). The current
objects are `is_latest AND NOT is_delete_marker` (S3), `time_deleted IS NULL` (GCS) or `is_latest` (Azure Blob);
a listing of the current objects alone is the default mode, which is cheaper.

**Order.** Keys ascend; a key's rows are consecutive, in the service's order within the key: S3 newest first,
versions and delete markers interleaved; GCS by generation, ascending; Azure Blob as the service lists a name's
entries, keyed by their time within the name: snapshots and versions oldest first, the current blob last. S3 can list two entries of a key with the
same `last_modified` (to the second), so that time does not rebuild S3's order: the file order does.
In an unfiltered, complete listing, the first row of an S3 key's group is the one with `is_latest`, and a
delete marker there means the object is deleted. On GCS, the last row of a name's group is the newest
generation; it is live only if `time_deleted` is null, and a GCS name may have no live generation. On Azure Blob
a name's `is_latest` row, when it has one, is its last; a versioned blob that was deleted has only previous
versions, and a soft-deleted one only `is_deleted` rows. A filter can
remove a key's latest version, so in a filtered dataset read `is_latest` and `time_deleted` per row instead of
by position.

**Parts never split a key.** A part's size target, `--part-rotation-max-rows` and `--part-interval` cut at the
next key, so a part can pass them by one key's history; parts stay key-disjoint, and a resume never lists half a
key again. The interval no longer bounds how long a part stays open while one key's rows are still arriving.

**Parallelism.** Ranges split only between keys, so one key with a very long history is listed by one worker,
page after page, while the rest of the bucket proceeds (`groups.open_pages`, `split.skipped.group_open` in the
report). S3's versions call is slower than ListObjectsV2: about 1.85 times the time per request at the median of
our measurements, and pages of old versions are slow to first byte; history multiplies the pages besides. GCS
lists generations at the cost of live objects. Azure Blob lists every entry with the same call and the same
format (Arrow where the account has it): each request costs a listing operation, as always, and the entries
multiply the pages; Microsoft recommends fewer than 1,000 versions per blob for listing performance.

**Permissions and refusals.** S3 needs `s3:ListBucketVersions` (`s3:ListBucket` alone answers 403, and swath
never falls back to a current-object listing); GCS needs the same `storage.objects.list`; Azure Blob the same
permission to list (a SAS with `l`, anonymous access to a public container). Text rows carry the
versions schema's columns: S3's `version_id`, `is_latest` and `is_delete_marker` (a delete marker's `size` and
`etag` null), GCS's `generation` and `time_deleted` (null on the live generation), Azure Blob's `version_id`,
`is_latest`, `snapshot`, `is_deleted`, `deleted_time` and `remaining_retention_days`; the table shows the version
ID (empty for an Azure snapshot), `latest` and, on S3, `delete_marker`. `--min-size` and `--max-size` never match a delete marker. A resume
continues in the mode its run started in; a run in the other mode is refused on that output. Directory buckets have
no versions and stay refused.

The run report's version counts describe this attempt after filtering: S3 `versions` and
`delete_markers` sum to emitted rows; GCS and Azure Blob `versions` include the `noncurrent` rows (on Azure every
row that is not a name's latest: previous versions, snapshots, soft-deleted blobs).
`distinct_keys` counts surviving names, and `live_objects` counts live versions among those rows.
Retained rows from an earlier attempt are excluded. `groups.overfetch_rows` counts rows above a
range end fetched to finish its last group. Table output includes the version ID and latest state,
and for S3 the delete-marker state, in wire order.

## Known limits

- **Azure HNS:** experimental tree listing requires a directory scope and does not support resume,
  `--all-versions` or `--sequential`; see [Azure Blob Storage](sources.md#azure-blob-storage).
- **Windows:** journaled object-store dataset runs, recovery and bucket commit cleanup through resume
  are refused for local and bucket output: directory sync is unavailable, so a power loss could lose
  acknowledged work. The output lock holds. Filesystem runs work without resume; an already committed,
  finalized dataset takes the CLI's no-work path. See [platforms](install.md#platforms). The test suite
  runs there in CI; nothing is measured there.
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
- **Memory:** `--sort-buffer` bounds rows in flight, not resident memory (see Sizing).
- **Scale:** key lists of up to 593M keys were measured through a key-list fake at 50 ms, and one live bucket
  of 1.09 billion objects at 256 and 1,024 workers; listings past that and runs of hours are unmeasured
  ([measurements](internals/measurements.md#known-gaps)).

When a run fails: [troubleshooting.md](troubleshooting.md).
