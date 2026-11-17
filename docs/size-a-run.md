# Size a run

How much disk and memory a large listing needs, and what its requests cost. What a running listing shows, and
the flags to change when its report says so, are in [Watch and tune a run](watch-a-run.md).

## Resources and their flags

| Resource | Flag | Default | What to know |
|---|---|---|---|
| Listing concurrency | `--workers` | 64 for an object store, whatever the CPU count; 4 per CPU for a local path, at most 64 | an object-store listing waits on the service, not the CPU; it is also the request limiter's ceiling ([how many](#workers)). A local walk waits on the disk: more workers past a few per CPU only add memory and contention (raise it for a network filesystem) |
| Sort buffer | `--sort-buffer` | 1/16 of memory, at most 1 GiB; 128 MiB when no limit is found; at least 128 KiB (less is refused, exit 2) | memory is the least of physical memory, the cgroup's limit (Linux, its ancestors' included) and `GOMEMLIMIT`; `diagnostics.sort_buffer` records which, and why the budget is what it is. It bounds the bytes of rows and scheduler metadata in flight (see Memory below). Not a cap on resident memory. After listing it also sizes the parallel tail: when `diagnostics.resources.drain.segments.workers_by` is `row_budget`, a larger budget buys more tail workers ([how](internals/ordering.md#segments-parts-written-in-parallel-after-listing)) |
| Encoders | `--encoders` | GOMAXPROCS, reduced by resource admission | default Parquet counts are limited by an encoder allowance of 1/16 of memory (at most 1 GiB, whatever `--sort-buffer` says), each encoder priced at the schema's calculated working set plus, writing to a bucket, its upload buffer (a GCS chunk, an S3 part or an Azure block), and by the descriptor limit; an explicit `--encoders N` bypasses these limits with a warning on a shortfall. The default count may rise after listing; an explicit count is never raised. See [encoder admission](internals/ordering.md#the-budget) and [per-schema buffer sizes](internals/parquet-output.md#writer-buffers-and-admission). For `.zst` text files the flag controls parallel compression jobs (see Memory). |
| Spill disk | `--spill-budget` | 0: the spill filesystem's free space after the reserve for a dataset in a bucket, half of it otherwise (the output, or a stream's other programs in `$TMPDIR`, share it), following the disk | rows listed ahead of a slow frontier go to `DIR/.swath/spill/`; a text file's to a scratch directory beside it, `.NAME.swath-ATTEMPT-RANDOM/spill/` (stdout's in `$TMPDIR/swath-ATTEMPT-RANDOM/spill/`, as a stream has no directory; a listed tree holding either leaves it out; `swath du` never spills); past the cap, or at the reserve, listing waits instead of failing. The default cap is re-derived from the free space when it would refuse a frame, and every 2 s while it holds the run back, so a filesystem grown under the running process is used (report: `diagnostics.resources.spill.max_by`, `start_max_bytes`, `max_bytes`, `cap_raises`); a `--spill-budget` is fixed. The reserve is derived: one part per encoder at full size, at least 1 GiB (report: `diagnostics.resources.spill.reserve_bytes`) |
| Part size | `--part-size`, `--part-max-rows` | 64 MiB; no row cap | target Parquet bytes per part; with `--part-max-rows N` a part also closes at N rows. Smaller parts suit readers that load whole parts, larger ones mean fewer files. A dataset holds at most 100,000 parts: a small part size on a very large listing can reach that (exit 1, `cause` `part_limit`; start over with a larger `--part-size`) |

## Workers

Start at the default, 64. When a listing at 64 takes longer than you want, the store does not throttle and the
machine has cores and memory to spare, try 256. What more workers buy depends on the machine, the store and
the output, so measure your own run: in the runs below a count (`swath du`) kept gaining above 256 on 16 and
32 vCPUs, while a sorted dataset, whose output sets its pace, gained less; on 32 vCPUs the 0.4.0 release
took longer at 2,048 workers than at 1,024. On a machine short of
CPU or memory, keep the default or lower it: the ordering, spill and encoders of a sorted run are its limit
there, not the listing. Lower it, too, when the store throttles at 64 (`progress.json`'s `limit` falls below
`--workers`: [throttling](watch-a-run.md#throttling)).

The rule rests on these runs, each an observation on the stated machine and day, not a rate:

- At 64 workers a public bucket of 37 million objects was listed into a sorted dataset in 61 seconds on a
  4-vCPU VM, with 52 of the 64 requests in flight on average
  ([measurements](internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15)).
- A live bucket of 1.09 billion objects, listed from one 32-vCPU VM in another cloud. On a build of
  2026-11-07, at 64 workers neither a count nor a sorted dataset finished within 40 minutes, and the count took
  177 s at 1,024 workers; that build's sorted runs at 256 and 1,024 workers, and where its output set the
  pace, are in the [measurements](internals/measurements.md#a-billion-live-objects-sentinel-cogs-2026-11-07).
  On the 0.4.0 release (2026-11-17) the sorted dataset took 289 s at 1,024 workers and 338 s at 2,048, and the
  count 188 s at 1,024
  ([measurements](internals/measurements.md#a-billion-live-objects-on-the-040-release-against-032-sentinel-cogs-2026-11-17));
  no 256-worker run of the release is measured.
- Through a test service answering in 50 ms, on lists of 20 million keys or more on a 16-vCPU machine, a count
  took 0.31 of its 64-worker time at 256 workers and 0.27 at 512, with 10 % more requests. A sorted dataset of
  143 million keys on 8 vCPUs took 258 s at 64 workers and 170 s at 256. On one core, sorted datasets of 130 to
  145 million keys were bound by the core and the disk
  ([measurements](internals/measurements.md#workers-one-core-and-the-study-harness-through-the-fake)).

## Disk

The worst case is the dataset itself plus the spill cap plus the reserve, all on the output's
filesystem (spill lives under the output directory, or beside a text file). A sorted first listing of a large
bucket spills most of its rows once; a relist seeded from the previous dataset spills a fraction of that
([disk sizing](#disk-sizing)). The spill cap includes the
durable spill a journaled run keeps for a resume (`DIR/.swath/spill/durable/`): a certified file stays on
disk, charged to the cap, until the published dataset covers its keys, and when the cap is reached the
oldest file already consumed is evicted (its keys are then relisted after a crash); the report's
`diagnostics.resources.spill.durable.retained_bytes` and `peak_retained_bytes` show it. A text file's scratch directory,
and a stdout stream's under `$TMPDIR`, is its attempt's own (created exclusively, named for the file and the
attempt), so two runs writing sibling files, the same one, or streams never share or remove each other's; the attempt removes it when it ends, after
success or failure. Only a killed run leaves one behind, with its `.NAME.*.tmp`: delete both. Estimate the dataset from a representative prefix: the
report's `output.output_bytes` over `output.rows_committed`; `diagnostics.resources.free_disk_min_bytes` is the least free space the run saw. If the dataset outgrows the disk the run stops with exit
74 and resumes once there is space. Spill backs off rather than fail when a frame does not fit (the run
waits), but not always: a range's closing trailer that does not fit, or any other spill I/O error, stops the
run (resumable; [ordering](internals/ordering.md#spill)).

### Disk sizing

- **The dataset:** about 30 bytes per row at the default part size, measured on a live listing of 1.09
  billion objects (488 parts, 32 GB;
  [measurements](internals/measurements.md#a-billion-live-objects-sentinel-cogs-2026-11-07)). Measure your own from a representative
  prefix: `output.output_bytes` over `output.rows_committed`.
- **Spill per row:** frames are prefix-coded and compressed (zstd) by default: 25 to 53 bytes per row on production
  keys with real ETags in a [benchmark](internals/measurements.md#delta-coded-spill-frames), and about 30 bytes
  per row on the live listing above (32 GB for 1.08 billion spilled rows). At the peak, a median 71 % of the
  rows were listed ahead of the output on lists of 20M keys or more
  ([measurements](internals/measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)),
  so live spill can come close to the
  dataset's own size. On listings of a billion objects and more it reaches it: 96 to 99 % of the rows were
  ahead of the output at the peak, at 18 to 22 bytes per row
  ([measurements](internals/measurements.md#spill-at-billion-scale-and-a-spill-cap-too-small-2026-11-13)).
- **Peak spill** of a first listing (no prior, or a cut file without counts) ≈ 0.7 × rows × spill bytes per
  row; above a billion objects plan for every row, rows × spill bytes per row. The default spill budget of a
  dataset on disk is half of (free space − the reserve), so the free space needed is about the dataset plus
  twice the peak spill plus the reserve (`diagnostics.resources.spill.reserve_bytes`: one part per encoder, at least 1
  GiB). The default follows the disk: a filesystem that turns out small can be grown under the running process
  instead of restarting it.
- **A relist seeded from the previous dataset** (or a counted cut file) keeps the rows ahead of the output near
  its frontier window of at most 8 million rows instead of the bucket ([warm start](resume-and-refresh.md#warm-start-for-repeat-listings)),
  so its spill is a small fraction of a first listing's. The window is not a storage bound: a region that grew
  since the prior, keys added past its last key and certified files awaiting retirement still spill. Size the
  disk for the dataset and that growth, and take the spill a seeded run needs from its report's
  `diagnostics.resources.spill.peak_bytes`.
- **A bucket of unknown size:** nothing reports the object count before the listing, so the first listing of
  a bucket is the one that can outgrow its disk. Give it room to spare or a disk that can be grown online (the
  default spill cap follows it), bound it with `--max-duration`, and size later runs from its manifest's row
  count.
- **A dataset in a bucket:** the work directory needs no room for the dataset. The default spill budget is its
  free space after the 1 GiB reserve, and follows the disk: on a disk that can be grown online, start modest
  and grow it if spill fills it (`diagnostics.resources.spill.cap_raises` counts the rises). Set `--spill-budget` when the
  disk is shared, and size it as the budget plus the reserve plus the metadata, which grows with the parts (a claim receipt of
  under 1 KiB and a journal record each; the candidate manifest about 1 KiB a part, at most about 1.1 GiB at
  the 100,000-part limit). Memory grows instead by one upload buffer (an 8 MiB GCS chunk, S3 part or Azure block) per
  encoder, admitted with its writer. A smaller spill budget trades disk for listing that waits on the output; measure a representative
  run's `diagnostics.resources.spill.peak_bytes` before shrinking the disk.
- **Example, 200 million objects:** a 6 GB dataset; peak spill 4.2 GB at 30 B per row or 7.4 GB at 53 B per row;
  so 15 to 22 GB free. Both inputs are assumptions until a representative prefix measures them. With less
  disk frames are refused and listing slows to the output's pace ([the first bad
  pattern](watch-a-run.md#three-bad-patterns)): a run a little short of disk still completes, and a run far short of it may not finish in
  any useful time; it says so after 2 minutes (`spill_pressure`), and `--spill-wait-timeout` stops it
  (resumable for a run with a journal; text output and a local tree run again). Set `--spill-budget` explicitly when you know the room.

## Memory

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
  about two sort buffers in all (`diagnostics.resources.drain.segments.tree_bytes` beside `max_held_bytes`).
- **Pages in flight, outside it:** accepted success bodies are limited to 16 MiB for S3/GCS and
  64 MiB for Azure. These byte limits do not bound the backing buffer capacity or decoded rows and
  strings. With the default page policy, Azure Blob asks for 5,000 entries a page with a counted
  `--seed-from` prior, against 1,000 otherwise: about five times the memory per page in flight, beside
  the same sort buffer. Worker requests,
  seed listings and probes can overlap. Include those allocations, the
  encoders' writers and codec, compression scratch and garbage collector headroom when sizing the host.
  See [body handling](internals/clients.md#what-swath-keeps) and
  [writer buffers](internals/parquet-output.md#writer-buffers-and-admission).
- **A local listing's largest directory, outside it:** a listing reads each directory whole to sort it,
  for each directory read at once; the names and keys grow with their lengths (1M files in one directory,
  names of about 13 bytes: about 140 MiB peak measured). A count (`swath du`) does not
  sort, and on Linux reads large directories a slab at a time; the portable engine (macOS, Windows) reads
  each directory whole for a count too.
- **A `.zst` text file's compressor, outside it:** as many parallel jobs as `--encoders`, about 5 to 13 MiB
  each; lower `--encoders` to lower it. Gzip is one stream. The report's `diagnostics.text.compressor` names the
  jobs and why (`jobs_reason`: `cores`, `encoder_allowance`, `encoders` when asked, `gzip`); the rows being
  formatted for the file are rows in flight, charged to `--sort-buffer`
  ([how](internals/run-lifecycle.md#encoder-sizing)).
- **Spill, on disk:** rows ahead of the frontier past the budget, within `--spill-budget`. On tmpfs (a
  common `/tmp`) spill is memory charged to the host or container but not to the process's RSS: the
  report names the filesystem (`diagnostics.resources.spill.fs_type`), and a run that spills there warns once, at its first
  spill (one that never spills, most runs, says nothing).

A count (`swath du`) holds only pages in flight and the ranges queued for a worker: it counts rows as they
arrive and never orders, buffers or spills them (`diagnostics.resources.features.order` false, `peak_buffered_bytes` 0, no `spill`).
`swath du` also holds one entry per distinct prefix (about 150 bytes each), capped by `--max-prefixes`.

Read the actual figure from the report's `process.peak_rss_bytes` on a representative run, and size the
host from that, not from `--sort-buffer`. Beside it, `diagnostics.resources.peak_heap_live_bytes` (the peak live heap,
sampled every 100 ms) against `invocation.sort_buffer_bytes` shows how much memory the budget does not see;
`peak_ahead_rows` is the most rows listed and not yet handed on at once (in memory or spilled), and
`max_frontier_stuck_ns` the longest stretch in which nothing was handed on (a held-up frontier).
Linux (VmHWM), macOS (`getrusage`) and Windows (peak working set) measure it with the CPU times; on another
platform `diagnostics.resources.rusage_available` is false and `process.peak_rss_bytes`, `timings.user_cpu_ms` and
`timings.sys_cpu_ms` are left out (the summary line prints `-`), never reported as zero.

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
