# Running swath in production

A short guide for whoever schedules and watches swath. The full reference is [operating.md](operating.md);
every flag is in [cli.md](cli.md). Numbers below are observations on the stated runs
([measurements.md](measurements.md)), not limits or promises.

## The nightly command

```bash
swath list s3://bucket/prefix/ -o /data/inv/2026-11-08 \
  --seed-from /data/inv/2026-11-07 \
  --report /var/log/swath/inv-2026-11-08.attempt-1.json \
  --max-duration 6h
```

- `-o DIR` writes a sorted Parquet dataset. `DIR` must be new or empty. Write each night to a new directory and
  point readers at it once its `manifest.json` exists ([operating.md](operating.md#overwrite-and-refresh)).
- `--seed-from` names the previous night's dataset (warm start, below). Leave it out on the first night.
- `--report FILE` is the outcome of record. Use a fresh path for each attempt, outside the output directory.
- `--max-duration` stops a run that takes too long. Give every unattended run one. Set the supervisor's own
  kill deadline above `--max-duration` plus 20 s.

What the supervisor does with the exit code:

| Exit | Do |
|---|---|
| 0 | done |
| 130, 143 (signals) | `swath resume DIR --report NEW-FILE` |
| 75 (stuck) | resume once; a second 75 in a row on the same output needs a person |
| 74 (disk full) | free space or move the directory to a larger disk, then `swath resume DIR` |
| 124 (deadline) | resume only within a separate budget of attempts or total time; past it, a person |
| 1, 2 | a person: a resume will not fix it |

## Three flags on a small runner

On a 1-vCPU machine with 1 to 2 GB of memory the defaults are the place to start. Change these three only
when the report says so.

- **`--workers`** (default 64). Listing waits on request round trips, not the CPU. On one core the sorted
  output (ordering, spill and one Parquet encoder) is the limit, so more workers do not help there. Raise it
  (128, 256) only on a machine with spare cores and a service that does not throttle. Lower it if the service
  throttles at 64.
- **`--row-budget`** (default 128 MiB). The bytes of rows held in memory between listing and encoding. It is
  not a memory cap: the process's peak is a few times the budget plus the pages in flight. At the defaults,
  sorted Parquet over the 251 production key lists through a 50 ms fake peaked at a median 0.46 GB and at most
  0.78 GB RSS (`head1`, `7b05605`) and 0.90 GB (`e2b`, `7fb447f8`, on ooni-data, 15.5M keys); six giants of
  267M to 523M keys peaked at 0.6 to 0.7 GB (`huge2`, `bf5e6d52`). Those runs were on machines of 16 to 32
  vCPU, and no run has been measured on a 1.8 GB machine. If `resources.peak_rss_bytes` comes near the machine's memory,
  lower the budget (64 MiB). If `resources.budget_wait_ns` is large and memory is free, raise it.
- **`--spill-budget`** and the disk. Rows that are listed ahead of the point the output has reached go to
  disposable spill files on the output's filesystem. By default spill may use half the free space at the
  start, minus a reserve of at least 1 GiB. Set `--spill-budget` when the filesystem is shared. Size the disk
  as described below.

## Warm start for repeat listings

`--seed-from PREVIOUS-DATASET` divides the bucket into ranges from the previous dataset's key distribution.
The run then sends no delimiter requests and makes far fewer early splits. On 251 production key lists
served through a fake at 50 ms, a seeded run took a median 0.76x the cold run's wall time and sent 1.02 requests
per 1,000 keys (`warm1`, 2026-11-07). A prior that is 45 % out of date still listed at 1.11 to 1.15x the
64-worker ideal (`stale4`, 2026-11-07). The prior must be the same bucket and prefix, or the run is refused
(exit 2). The output is the same with or without a prior.

## A healthy run report

Five fields to check in the `--report` file of a sorted listing of a million keys or more:

| Field | Normal |
|---|---|
| `outcome.class` | `completed` (exit 0), with `outcome.complete` true |
| `efficiency.http_attempts_per_1k_rows` | 1.0 to 1.4. On lists under a million keys, up to about 2 (the seed and first splits are a large share of a few hundred requests) |
| `resources.spill.refused` | 0 |
| `resources.budget_wait_ns` | 0 or close to it |
| `resources.frontier_stuck_max_ns` | a few request round trips: a median 0.28 s and at most 1 s on sorted runs of the 64 lists of 20M keys or more through a 50 ms fake (candidate 4, run `c4big`: its resumed and spill-capped arms) |

Spill is normal. A sorted run of a large bucket spills almost every row once (a median 96 % of rows on the 64
lists of 20M keys or more: candidate 4, run `c4big`, plain frames), because ranges finish in roughly
key-uniform order and the output waits for the leftmost one.

## Three bad patterns

**Spill refused, workers waiting.** `resources.spill.refused` is above 0 and `resources.budget_wait_ns` is
large. `resources.free_disk_min_bytes` is close to `resources.spill.reserve_bytes`, or the spill reached
`resources.spill.max_bytes`. Listing then runs at the output's pace. Give it disk: a larger filesystem for
the output, or a larger `--spill-budget` when the filesystem has the room. An interrupted run resumes with the
new value: `swath resume DIR --spill-budget 200GiB`.

**A stuck frontier.** `resources.frontier_stuck_max_ns` is seconds to minutes. In `progress.json`, `objects`
rises while `encoded` and `parts` stay flat and `part_age_ms` grows. One range at the front of the output is
slow: a slow, retried or hung request. Check `inflight.p99_ns` against `inflight.p50_ns` and the retries in
the report's `source.stats.counters.values`. After `--stall-timeout` (10 min) of no progress the run stops
with exit 75 and writes every goroutine's stack to `DIR/.swath/stacks-stuck.txt`. Resume it, and keep that file
if it happens again.

**Throttling.** `requests.throttles` rises, the counter `limiter.decreases` is above 0, and `progress.json`'s
`limit` falls below `--workers`. A background under 15 % of answers is tolerated without a cut. For a quota
shared with other clients, set `--request-rate N` below it. If a run crawls with the limit near 1 under
sustained throttling, use `--limiter=false --request-rate N` ([operating.md](operating.md#throttling)).

## Resume after a kill

`swath resume DIR` continues an S3 or GCS run after a signal, a crash, an OOM kill or a full disk. A local
directory listing keeps no journal and starts over.

- **Kept:** every published part, verified by size and SHA-256 before listing continues. Verification read
  247 to 376 MiB/s with a cold page cache and 511 MiB/s warm on a 4-vCPU VM
  ([measurements](measurements.md#recovery-and-part-count)), so 10,000 parts of 64 MiB take roughly 30 to 45
  minutes.
- **Listed again:** everything after the last published key. On a large sorted run that is most of the
  listing: a kill at 40 % listed 90 to 100 % of the keys again on every list of 20M keys or more (candidate 3,
  `c48ea228`, run `cand3`), because the output waits for the leftmost range while the rest is spilled.

The resume replays the recorded command. Only the resource, reporting and request flags may change
([cli.md](cli.md)). Give each attempt a fresh `--report` path.

## Where things are

```text
DIR/data/part-NNNNN.parquet   the parts, in key order
DIR/manifest.json             the commit: readers start only once it exists
DIR/run.json                  the run report, after the commit
DIR/error.json                the outcome of an attempt that did not complete
DIR/.swath/                   lock, journal, parts being published, spill/, progress.json
```

Spill lives under `DIR/.swath/spill/`, on the output's filesystem, and is removed when the run ends. A text
file's spill goes in a scratch directory beside it, `.NAME.swath-ATTEMPT-RANDOM/`. Text on stdout spills under
`$TMPDIR`. On tmpfs spill uses memory: the report's `resources.spill.fs_type` names the filesystem and the
run warns at start.

## Disk sizing

- **The dataset:** about 30 bytes per row at the default part size (the live 1.09-billion-object listing of
  run `cogslive1`, `bf5e6d52`, 256 workers, wrote 488 parts, 32 GB). Measure your own from a representative
  prefix: `output.output_bytes` over `output.rows_committed`.
- **Spill per row:** frames are prefix-coded and compressed (zstd) by default: 25 to 53 bytes per row on production
  keys with real ETags in the frame benchmark, and about 30 bytes per row on the live listing above (`cogslive1`:
  32 GB for 1.08 billion spilled rows). At the peak, a median 71 % of the rows were listed ahead of the output
  on lists of 20M keys or more (candidate 4, run `c4big`, plain frames), so live spill can come close to the
  dataset's own size.
- **Peak spill** ≈ 0.7 × rows × spill bytes per row. The default spill budget is half of (free space at the
  start − the 1 GiB reserve), so the free space needed is about the dataset plus twice the peak spill plus
  1 GiB.
- **Example, 200 million objects:** a 6 GB dataset; peak spill 4.2 GB at 30 B per row or 7.4 GB at 53 B per row;
  so 15 to 22 GB free. Both inputs are assumptions until a representative prefix measures them. With less
  disk the run still completes, but frames are refused and listing slows to the output's pace (the first bad
  pattern above); set `--spill-budget` explicitly when you know the room.
