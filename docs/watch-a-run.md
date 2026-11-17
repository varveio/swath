# Watch and tune a run

While a listing runs, swath shows its progress on the terminal and in a file beside the output, and when it
ends it says what it did. This page says which values to look at, and what to do when a run is throttled,
short of memory or disk, or stuck. How much disk and memory to give a run before it starts is in
[Size a run](size-a-run.md).

## Is the run making progress

On a terminal swath draws one progress line on stderr and redraws it every second. This listing of a large
public bucket stops itself after five seconds:

```bash
swath list s3://noaa-gestofs-pds/ --anonymous --s3-region us-east-1 -o noaa-all --max-duration 5s
```

At three seconds the line read:

```text
  listing · 2,189,002 objects · 73.6 TiB · 3.0s elapsed · 901,176 keys/s (avg 729,667) · 0 parts · 64 in flight · 2,258 API calls · ~$0.011 (est. @ $0.005/1k LIST)
```

From the left: what the run is doing (`listing`; `verifying` while a resume checks the parts it keeps), the
objects listed so far (a local directory's are `entries`; a resume adds `(+N recovered)`), their total size,
the time elapsed, keys a second now and on average, the parts published, requests in flight, API calls made,
and an estimate of their cost where the provider's list price is known (not for Azure Blob or a custom
endpoint). A narrow terminal drops fields from the right. There is no percentage: swath cannot know how many
objects are left.

A run is making progress while the objects and API calls rise. The parts can stay at 0 for a long time, as
here: `swath list` writes its rows in key order, so the output waits until the start of the key space has been
listed, and the rows listed ahead of it wait in memory and then on disk meanwhile
([short of disk](#disk)). `swath du` counts without ordering and never waits so.

Where the line appears:

- Off a terminal (a log file, a pipe) there is no line unless you ask: `--progress` appends the same record as
  a plain line every 30 s, and `--progress-interval 10s` sets the interval (at least 1 s) and implies
  `--progress`. A log never receives control sequences.
- `-q` turns the automatic line and the warnings off; `--no-progress` turns progress off everywhere.
- While `swath list` writes rows to the same terminal there is no line, and `--progress` is refused: send the
  rows to a file or a dataset. `swath du` draws the line while it lists and erases it before its table.
- `--color auto|always|never` colors the line and the closing lines; `auto` honors `NO_COLOR`.

### The progress file

A dataset run (`-o DIR`) also rewrites `DIR/.swath/progress.json` every 2 seconds, for a script or a
supervisor. Read during the run above:

```bash
jq '{state, rows_fetched, object_bytes, parts_written, part_age_ms, advance, throttles, inflight, limit}' \
  noaa-all/.swath/progress.json
```

```json
{
  "state": "listing",
  "rows_fetched": 2189002,
  "object_bytes": 80890956524487,
  "parts_written": 0,
  "part_age_ms": 3000,
  "advance": 2247006,
  "throttles": 0,
  "inflight": 64,
  "limit": 64
}
```

| Field | What it says |
|---|---|
| `state` | `listing`; `draining` once listing is done and the rows it holds are being written; `publishing`; then `done` or `failed` |
| `rows_fetched`, `object_bytes` | rows listed so far, and the objects' total size |
| `parts_written`, `part_age_ms` | parts published, and the time since the last one was published (or since the start) |
| `advance` | rises whenever rows are listed or written, parts published or a resume's parts verified: flat means nothing moves ([stuck](#is-the-run-stuck)) |
| `throttles`, `limit`, `inflight` | throttle answers so far, the request limit (it starts at `--workers` and falls when the service throttles), and requests in flight: [throttling](#throttling) |
| `order.spill_bytes`, `order.spill_max_bytes`, `order.spill_pressure_since` | rows waiting on disk for the output, their cap, and since when listing has waited on a full cap (present only while it does): [short of disk](#disk) |

Every field is in [Run report and progress file](reference/run-report.md#progress-file). The directory above
holds a resumable run; delete it when you are done (`rm -r noaa-all`).

<a id="a-healthy-run-report"></a>

## When the run ends

swath prints its closing lines on stderr. With `--stats` it adds the run's statistics:

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 -o stofs --stats
```

```text
  complete: 5,252 objects, 383 GiB in 1.0s → stofs (1 part, 121 KiB)
  next: swath show stofs · swath du --from-dataset stofs
  32 API calls · in flight avg 3.15 · peak 11
  <$0.001 (est. @ $0.005/1k LIST)
  peak RSS 61.7 MiB
```

The first line is the verdict, what was listed and its size, the time (and from 2 s the rate), and the output
written. `next:` is what to run next: `swath show` and `swath du --from-dataset` for a committed dataset,
`swath resume DIR` for a run that can resume. The statistics are the requests with the mean and peak in flight,
`throttled N · retried N` when there were any, the cost estimate, and the process's peak memory. The closing
lines print without `--stats` for runs over 1.5 s, runs that write output and runs that do not complete;
`--no-stats` and `-q` drop them.

The five-second run above ended this way:

```text
  stopped: --max-duration reached; 0 saved, 3,897,746 listed in 5.0s · 778,926 keys/s → noaa-all
  next: swath resume noaa-all
swath: deadline: --max-duration reached (swath resume noaa-all --max-duration 0 continues it without a deadline; a plain resume keeps the recorded one)
```

`saved` is the rows in published parts, which a resume keeps; `listed` is what this run listed. The verdicts
and what to do:

| Verdict | Exit | What to do |
|---|---|---|
| `complete` | 0 | nothing |
| `partial: N unreadable entries` | 3 | [Entries could not be read](troubleshooting.md#entries-could-not-be-read) |
| `stopped: --max-duration reached` | 124 | `swath resume DIR`, or run it again |
| `stopped: no progress within --stall-timeout` | 75 | [Is the run stuck](#is-the-run-stuck) |
| `stopped: the endpoint cannot be reached` | 75 | [The endpoint cannot be reached](troubleshooting.md#the-endpoint-cannot-be-reached) |
| `stopped: --spill-wait-timeout reached`, `stopped: the disk is full` | 74 | [Short of disk](#disk) |
| `interrupted` (Ctrl-C), `interrupted: SIGTERM`, `interrupted: SIGHUP` | 130, 143, 129 | `swath resume DIR` |
| `failed`, or an error line alone | 1 | the error says why: [Troubleshooting](troubleshooting.md) |

Only a dataset run of an object store keeps the journal a resume needs; a local directory, a text file and
`swath du` are run again. Every code is in [Exit codes](reference/exit-codes.md).

**The run report.** A committed dataset has its report in `DIR/run.json`; `--report FILE` writes it for any run,
on failure too. A few fields answer whether the run was healthy:

```bash
jq '{class: .outcome.class, complete: .outcome.complete, rows: .output.rows_committed,
     requests: .requests.http_attempts, throttles: .requests.throttles,
     peak_rss: .process.peak_rss_bytes, wall_ms: .timings.wall_ms}' stofs/run.json
```

```json
{
  "class": "ok",
  "complete": true,
  "rows": 5252,
  "requests": 32,
  "throttles": 0,
  "peak_rss": 64704512,
  "wall_ms": 952
}
```

A healthy run is `ok` and `complete`, with few or no throttles and a peak memory well under the machine's. The
fields are in [Run report and progress file](reference/run-report.md); its `diagnostics`, which explain a slow
run in depth, are read in [Reading a run report](internals/counters.md#reading-a-run-report).

## Throttling

**What you see.** The service answers some requests with "slow down" (S3's 503 SlowDown, a 429): in
`progress.json` `throttles` rises and `limit` falls below `--workers`, the rate drops, and `--stats` ends with
`throttled N`. This run went through a local test server that answered 30 % of requests with 503 SlowDown;
`progress.json` every 10 seconds:

```json
{"state":"listing","rows_fetched":785000,"throttles":382,"inflight":2,"limit":2,"part_age_ms":4000}
{"state":"listing","rows_fetched":899000,"throttles":436,"inflight":1,"limit":1,"part_age_ms":8000}
{"state":"listing","rows_fetched":1009000,"throttles":486,"inflight":1,"limit":1,"part_age_ms":14000}
```

and its closing lines with `--stats`:

```text
  complete: 2,000,000 objects, 8.18 TiB in 1m18s · 25,542 keys/s → thr (3 parts, 8.36 MiB)
  next: swath show thr · swath du --from-dataset thr
  3,479 API calls · in flight avg 1.55 · peak 63
  throttled 1,062 · retried 1,062
  peak RSS 276 MiB
```

**What it means.** Requests to S3, GCS and Azure Blob pass an adaptive limit that starts at `--workers`, falls
when the service keeps throttling and rises again as answers come back. Occasional throttles do not lower it.
Throttled requests are retried: they cost time, not rows.

**What to do.**

- A few throttles, with `limit` near `--workers`: nothing.
- The store's request quota is shared with other clients (other jobs, several swath runs): set
  `--request-rate N`, requests a second for this run, below your share of it.
- The service throttles at the default 64 workers: lower `--workers`, which is also the limit's ceiling.
- The run crawls with `limit` at 1 or 2 for minutes: turn the adaptive limit off and set a fixed rate,
  `--no-adaptive-limit --request-rate N`. Without the limit each worker waits on one page at a time, and
  swath's own probes of the key space run beside them, so more than `--workers` requests can be in flight.

A request that keeps failing with the same cause (not only throttles) prints a warning after 10 seconds:
[A warning repeats about failing requests](troubleshooting.md#a-warning-repeats-about-failing-requests).

## Short of memory or disk

<a id="three-flags-on-a-small-runner"></a>

### Memory

**What you see.** `peak RSS` in the `--stats` lines and `process.peak_rss_bytes` in the report. swath sets no
memory limit of its own: a run that outgrows the machine or the container is killed by the system.

**What it means.** `--sort-buffer` (by default 1/16 of memory, at most 1 GiB; 128 MiB when no memory limit is
found) bounds the rows a sorted run holds between listing and writing. It is not a cap on the process: pages
in flight, the Parquet writers (admitted against their own 1/16 of memory) and the Go runtime come on top.
Memory is the least of the physical memory, the container's limit and `GOMEMLIMIT`.

**What to do.**

- Peak RSS near the machine's memory: lower `--sort-buffer` together with `--workers` and `--encoders`; a
  smaller sort buffer alone spills more rows to disk without always lowering the peak.
- Many rows spilled (the report's `diagnostics.resources.spill.rows`, the rows written to spill, near the rows
  listed) and memory free: raise `--sort-buffer` so more rows wait in memory.
- On a machine of one vCPU and 1 to 2 GB, start from the defaults: on one core the sorted output is the
  limit, so keep the default `--workers` ([workers](size-a-run.md#workers)). Sorted runs through a test server
  at a 128 MiB sort buffer peaked at a median 0.46 GB and at most 0.9 GB over 251 lists, and at 0.6 to 0.7 GB
  on six lists of 267 to 523 million keys, on machines of 16 to 32 vCPU
  ([measurements](internals/measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)).
  A machine of 1 to 2 GB has not been measured: read the peak on a representative run first.
- A warning that spill is in memory:

  ```text
  swath: warning: rows are spilling to inv/.swath/spill, which is on tmpfs: they are held in memory (outside the peak RSS); write the output to a disk
  ```

  The output directory (for rows on stdout, `TMPDIR`) is on a memory filesystem, a common `/tmp`: put it on a
  disk.

How much memory to plan for: [Memory](size-a-run.md#memory).

<a id="three-bad-patterns"></a>

### Disk

**What you see.** `swath list` keeps the rows it listed ahead of its output in spill files on the output's
filesystem (`DIR/.swath/spill/` for a dataset; beside a text file; in `TMPDIR` for rows on stdout); a large
bucket spills almost every row once. By default spill may use half the filesystem's free space (all of it for
a dataset in a bucket), after a reserve of at least 1 GiB. When it is full, listing slows to the pace of the output, `progress.json` shows
`order.spill_pressure_since`, and after 2 minutes swath says so, again every 30 minutes while it lasts. This
run went through a local test server with a deliberately small `--sort-buffer 8MiB` and a fixed
`--spill-budget 2MiB`, which its remedy names:

```text
swath: spill cap too small: listing has waited on the output for 2m1s: the spill cap cannot hold the rows listed ahead of it (spill 2.00 MiB, cap 2.00 MiB, free-space reserve 1.00 GiB); the output writes 6,403 rows/s; stop the run and resume it with more disk or a larger --spill-budget (swath resume sp2 --spill-budget SIZE), or let it run: it finishes, slowly
```

With the default cap the remedy reads `the cap follows the spill disk: grow the disk, or free space on it, and
listing continues`.

**What it means.** The run does not fail: left alone it finishes at the output's pace, which on a very large
bucket can be hours or days.

**What to do.**

- With the default cap, grow the filesystem under the running process (a cloud disk resized and its
  filesystem grown online) or free space on it: within seconds the cap rises and listing continues.
- With `--spill-budget`, which is fixed, stop the run (Ctrl-C) and resume it with a larger one on a disk that
  has the room: `swath resume DIR --spill-budget 200GiB`. A plain `swath resume DIR` keeps the recorded cap.
- On an unattended run, `--spill-wait-timeout 30m` stops the run once listing has waited that long, exit 74,
  resumable. The same test with `--spill-wait-timeout 10s`:

  ```text
    stopped: --spill-wait-timeout reached; 0 saved, 1,022,591 listed in 12.0s · 85,195 keys/s → sp
    next: swath resume sp
  swath: spill cap too small: listing has waited on the output for 12s: the spill cap cannot hold the rows listed ahead of it (spill 1.99 MiB, cap 2.00 MiB, free-space reserve 1.00 GiB); the output writes 43,825 rows/s; stopped by --spill-wait-timeout 10s: resume it with more disk or a larger --spill-budget (swath resume sp --spill-budget SIZE)
  ```

- A repeat listing of the same bucket spills far less when it starts from the previous dataset
  ([warm start](resume-and-refresh.md#warm-start-for-repeat-listings)).
- A part or the manifest that cannot be written stops the run with `stopped: the disk is full`, exit 74:
  [The disk filled](troubleshooting.md#the-disk-filled-exit-74).

How much disk to plan for: [Disk sizing](size-a-run.md#disk-sizing).

## Is the run stuck

**What you see.** The objects stop rising and the rate reads 0. While every request fails, the rate gives way
to `retrying:` and the cause. At half of `--stall-timeout` swath warns. This run went through a local test
server that stopped answering after a second and a half, with `--stall-timeout 1m` and
`--progress --progress-interval 15s`:

```text
  listing · 743,000 objects · 3.04 TiB · 2.0s elapsed · 371,500 keys/s (avg 371,500) · 0 parts · 64 in flight · 887 API calls
  listing · 743,000 objects · 3.04 TiB · 18.0s elapsed · 0 keys/s (avg 41,278) · 0 parts · 64 in flight · 887 API calls
  listing · 743,000 objects · 3.04 TiB · 34.0s elapsed · retrying: context deadline exceeded · 1 part · 64 in flight · 951 API calls
  listing · 743,000 objects · 3.04 TiB · 50.0s elapsed · retrying: context deadline exceeded · 1 part · 64 in flight · 951 API calls
swath: warning: S3 127.0.0.1:19424: context deadline exceeded (retrying; 65 attempts; the run stops as stuck after 1m0s without progress)
swath: warning: no progress for 30s (the run is stopped as stuck at 1m0s)
  listing · 743,000 objects · 3.04 TiB · 1m06s elapsed · retrying: context deadline exceeded · 1 part · 64 in flight · 1,015 API calls
  listing · 743,000 objects · 3.04 TiB · 1m22s elapsed · retrying: context deadline exceeded · 1 part · 64 in flight · 1,015 API calls
swath: warning: no progress for 1m2s (stuck_timeouts): stopping the run as stuck
swath: warning: goroutine dump written to stall/.swath/stacks-stuck.txt
  stopped: no progress within --stall-timeout; 503,000 saved, 743,000 listed in 1m36s · 7,738 keys/s → stall (1 part, 2.10 MiB)
  next: swath resume stall
swath: stuck: no progress within the stall timeout (stuck_timeouts); every request was failing: S3 127.0.0.1:19424: context deadline exceeded (192 attempts)
```

**What swath does.** Rows listed or written, parts published and pages of entries answered count as
progress; failed, throttled and hung requests do not, nor a service that answers only empty pages. After
`--stall-timeout` (10 minutes by default) without progress, swath stops the run with exit 75 and writes every
goroutine's stack to `DIR/.swath/stacks-stuck.txt`. The cause in the last line, in `DIR/error.json` and in the
report's `outcome.cause` says what kept rising: `stuck_throttle`,
`stuck_timeouts`, `stuck_empty_chain` (the service answered only empty pages) or `stuck_no_rows`
([exit 75](reference/exit-codes.md)). A part published while requests hang counts too: above, the part at
30 seconds restarted the clock.

**What to do.**

- Resume it once: `swath resume DIR`.
- Stuck again on the same output: keep `stacks-stuck.txt` and the report, and
  [report the problem](troubleshooting.md#reporting-a-problem).
- A service that is slow to answer for minutes at a time: raise `--stall-timeout`.
- An endpoint that has never answered stops after 30 seconds instead:
  [The endpoint cannot be reached](troubleshooting.md#the-endpoint-cannot-be-reached).

The objects can also keep rising while the parts stay flat and `part_age_ms` grows: one slow range at the start
of the key space holds the output back. The run goes on listing ahead, spilling to disk, and writes its parts
once that range answers; watch the disk ([short of disk](#disk)).
