# Diagnostics

`swath diag` runs a listing with options that exist to run an experiment or a comparison, not to operate the
tool. `swath list` and `swath du` refuse each of them with exit 2 and a message naming `swath diag`.

```text
swath diag list SOURCE [OPTIONS]    swath list, with the diagnostic options
swath diag count SOURCE [OPTIONS]   a count that writes nothing (a throughput measurement)
```

`swath diag list` takes every `swath list` option and those below. `swath diag count` takes the count options
(`swath du`'s, without its totals) and the diagnostic ones that apply to a count; it lists and counts rows as
they arrive, never ordering, buffering or spilling them ([ordering.md](ordering.md#counting-without-order)),
and prints one line on stdout, its source counters first:

```text
$ swath diag count s3://bkt --endpoint-url http://127.0.0.1:19777 --anonymous
objects=60004 bytes=374790026 pages=166 requests=229 retries=0 splits=37 child_empty=36 max_active=63 truncated=false rows=60004 object_bytes=374790026 wall=0.180s user=0.33s sys=0.04s peak_rss=61.4MiB
$ swath diag count /data/photos
files=4 dirs=3 bytes=470003 errors=0 rows=7 object_bytes=470003 wall=0.000s user=0.00s sys=0.00s peak_rss=24.9MiB
```

## Options

Generated from the flag table, as `--help` prints them; `TestCLIDocFlags` fails when a block is stale
(`UPDATE=1 go test ./cmd/swath -run TestCLIDocFlags` rewrites it) and `TestHelpGroupsCoverFlags` when a
flag is in no group. `swath diag list`:

<!-- diag-list:begin -->
```text
Diagnostics:
      --no-spill-compress   write spill frames uncompressed (not zstd level 1)
      --spill-plain         write spill frames without delta coding (a comparison arm)
      --no-journal          publish parts only at the end, without the journal resume needs
      --no-spill            wait on the sort buffer instead of spilling rows to disk
      --spill-reserve SIZE  free space spill leaves on its filesystem (0: derived)
                              Derived: one part per encoder, at least 1 GiB.
      --adaptive-limit-slow-start
                            start the limit at 4 in flight, doubling each second to --workers
      --no-seed             start from one range, without the delimiter-listing seed
      --structure[=auto|on|off]
                            probes whose folder boundaries splits pivot on (default auto)
                              The bare flag is on, so a mode needs '='. auto probes on the open
                              frontier while its cursor is in a folder, and in bounded ranges whose
                              chain stays inside one folder; on probes every range in a folder.
      --slow-split          split a range whose pages are slow without waiting for an idle worker
      --sequential          list with one continuation-token chain (no ranges; the reference)
      --max-keys N          with --sequential, stop after N objects (marked incomplete)
      --page-size N         ask for pages of N entries (1 to 1000; default the provider's)
```
<!-- diag-list:end -->

`swath diag count`:

<!-- diag-count:begin -->
```text
Diagnostics:
      --adaptive-limit-slow-start
                     start the limit at 4 in flight, doubling each second to --workers
      --no-seed      start from one range, without the delimiter-listing seed
      --structure[=auto|on|off]
                     probes whose folder boundaries splits pivot on (default auto)
                       The bare flag is on, so a mode needs '='. auto probes on the open frontier
                       while its cursor is in a folder, and in bounded ranges whose chain stays
                       inside one folder; on probes every range in a folder.
      --slow-split   split a range whose pages are slow without waiting for an idle worker
      --sequential   list with one continuation-token chain (no ranges; the reference)
      --max-keys N   with --sequential, stop after N objects (marked incomplete)
      --page-size N  ask for pages of N entries (1 to 1000; default the provider's)
```
<!-- diag-count:end -->

`swath resume` takes these again on a run started with them (the last value wins; the other spelling undoes a
recorded switch):

<!-- diag-resume:begin -->
```text
Diagnostics:
      --no-spill-compress   write spill frames uncompressed (not zstd level 1)
      --spill-plain         write spill frames without delta coding (a comparison arm)
      --spill-reserve SIZE  free space spill leaves on its filesystem (0: derived)
                              Derived: one part per encoder, at least 1 GiB.
      --adaptive-limit-slow-start
                            start the limit at 4 in flight, doubling each second to --workers
      --spill-compress      undo a recorded --no-spill-compress
      --no-spill-plain      undo a recorded --spill-plain
      --no-adaptive-limit-slow-start
                            undo a recorded --adaptive-limit-slow-start
```
<!-- diag-resume:end -->

## What they change

- **The seed** (S3, GCS, Azure Blob). By default, before any range lists, delimiter listings of the folder
  hierarchy (the top level up to 4 pages, levels narrower than `--workers` descended up to 4 deep, at most 64
  requests in at most 6 serial waves) tile the scope, or a resumed scope after its last published key. The
  first wave also carries the scope's first data page: a complete page (the whole scope) lists a bucket of one
  page in one round trip; a truncated one is discarded, and the first tile starts at the scope's start (or the
  resumed key) as without it; a delimiter chain whose page held no folder is not followed
  ([range-engine.md](range-engine.md)). `--no-seed` starts from one range instead
  ([measurements](measurements.md) has the seed's effect on split depth).
- **`--sequential`** is the one-chain oracle, and the only mode where `--max-keys` stops early (the dataset is
  then marked incomplete; such a run keeps no journal and cannot be resumed).
- **`--structure`**, **`--slow-split`** and **`--page-size`** change how ranges are probed and split, and the
  page size asked for ([range-engine.md](range-engine.md)).
- **`--adaptive-limit-slow-start`** starts the request limit at 4 in flight and doubles it each second up to
  `--workers` ([clients.md](clients.md#request-admission)).
- **Spill.** `--no-spill` turns spill off (listing waits on the sort buffer instead); `--spill-reserve` sets the
  free space spill leaves on its filesystem (derived by default: one part per encoder at full size, at least
  1 GiB); `--no-spill-compress` writes the frames uncompressed (by default zstd level 1 runs over the
  delta-coded frames); `--spill-plain` writes each key and payload whole instead of delta-coding them, the
  comparison arm ([ordering.md](ordering.md)).
- **`--no-journal`** publishes parts only at the end, without the journal a resume needs.

## Environment

| Variable | Effect |
|---|---|
| `SWATH_CPUPROFILE=FILE`, `SWATH_MEMPROFILE=FILE` | write pprof CPU and allocation profiles, split at the end of listing ([Phase profiles](#phase-profiles)); each file is checked as `--report FILE` is ([run-lifecycle.md](run-lifecycle.md#side-paths)) and may not be a symbolic link |
| `SWATH_MEMPROFILERATE=N` | with `SWATH_MEMPROFILE`: `runtime.MemProfileRate`, bytes allocated per sample (default 65536; 1 records every allocation, 0 none) |
| `SWATH_LOCALFS_ENGINE=portable` | select the portable filesystem engine in place of `getdents` on Linux ([providers/local.md](providers/local.md)) |
| `SWATH_SPLIT_TRACE` | trace range-engine splits on stderr, one line per split: range id, bounds, cursor, pivot, keys listed, the pivot's source, the children's depth, the range's gallop generation and the anchor its upper child carries |

## Phase profiles

A sorted run has two phases with different work: listing, until the provider has finished (the report's
`timings.listing_done_at`), and the drain's tail after it, in which the order stage replays spill and the
encoders write the remaining parts. The profiles are split at that boundary, so each phase is read on its own:

| File | Holds |
|---|---|
| `FILE.listing` and `FILE` (`SWATH_CPUPROFILE`) | CPU from the run's start to the boundary |
| `FILE.tail` (`SWATH_CPUPROFILE`) | CPU from the boundary to the end of the run |
| `FILE.listing` (`SWATH_MEMPROFILE`) | allocations from the process's start to the boundary |
| `FILE.exit` and `FILE` (`SWATH_MEMPROFILE`) | allocations from the process's start to the end of the run |

Go runs one CPU profile at a time, so there is no whole-run CPU profile beside the phases': `FILE` is the
listing phase (it was the whole run before the split), and `go tool pprof FILE.listing FILE.tail` merges the two.
Between them lies a gap, from the boundary until the tail's profile starts, while the listing phase's profile
is written out (the report's `profiles.cpu_gap_ns`). The phase switch runs on a goroutine of its own, so the
order stage does not wait for it. The allocation profile is cumulative: `go tool pprof -base FILE.listing
FILE.exit` is the tail's allocations, and each file is as of the latest garbage collection before it was written
(the process forces none). A run that never reaches the boundary (a failed or stopped listing, `swath diag count`
and `swath du`, which order nothing) writes the CPU listing files for the whole run and neither `FILE.tail` nor
the allocation `FILE.listing`.

The run report's `profiles` names the files by kind (`cpu_listing`, `cpu_tail`, `mem_listing`, `mem_exit`), the
boundary (`phase_boundary_at`), the gap and the sample rate ([run report](../reference/run-report.md#run-report));
the tail's and the exit's files are written as the process ends, after `run.json`. Unset, nothing changes: no
profile runs and the report has no `profiles`.
