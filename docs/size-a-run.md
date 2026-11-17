# Size a run

Before you list a large bucket, pick the machine: disk for the dataset and its spill, memory, cores, and the
`--workers` to give it. Start from the measured runs below, size the disk with the rule after them, then list
a representative prefix and read its report. What a running listing shows, and the flags to change when it
says so, are in [Watch and tune a run](watch-a-run.md).

## What a bucket needs

Four measured runs, each run once, on the stated machine and day: observations, not rates.

| Objects | Output | Dataset | Spill at its peak | Peak memory | Machine | `--workers` | Time |
|---|---|---:|---:|---:|---|---:|---:|
| 37 million | sorted dataset | 825 MiB, 11 parts | 618 MB | 1.51 GiB | 4 vCPU, 7.7 GiB | 64 (the default) | 61 s |
| 37 million | count (`swath du`) | none | none | 109 MiB | 4 vCPU, 7.7 GiB | 64 | 53 s |
| 1.09 billion | sorted dataset | 32.6 GB, 494 parts | 32.3 GB | 5.8 GB | 32 vCPU, 62 GiB | 1,024 | 289 s |
| 1.09 billion | count (`swath du`) | none | none | 1.3 GB | 32 vCPU, 62 GiB | 1,024 | 188 s |

- **37 million objects:** the public bucket `s3://noaa-gestofs-pds` (AWS us-east-1), listed anonymously on
  2026-11-15 with a build of that day, before the release, from a cloud VM shared with other work; on average
  52 of the 64 requests were in flight. The count is `swath du -d 1 --sort size`
  ([measurements](internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15)).
- **1.09 billion objects:** the public bucket `s3://sentinel-cogs` (AWS us-west-2), listed anonymously on
  2026-11-17 with the 0.4.0 release from a VM in another cloud with a 400 GB disk. At its peak the sorted run
  held 64.6 GB on that disk, and it kept 22.8 of the 32 cores busy on average
  ([measurements](internals/measurements.md#a-billion-live-objects-on-the-040-release-against-032-sentinel-cogs-2026-11-17)).
- **Other sizes:** not measured. Size the disk with the rule below, and take memory and time from a
  representative prefix of your own bucket.

A count (`swath du`) writes nothing to disk and holds no rows, only the requests in flight and the parts of
the key space waiting for a worker. A sorted run's memory depends on the machine (the default `--sort-buffer`
is a share of its memory), on `--workers` and on the encoders: read its peak resident memory (RSS) on a
representative run, and size the host from that.

On cores, the measurements give no rule beyond these runs: a sorted run on one core completes, bound by the
core and the disk, and how cores and workers go together is under [Workers](#workers).

### Measure a prefix of your own

List a prefix that is typical of the bucket into a dataset, with a report, and read three figures from it:
the peak resident memory, the dataset's bytes per row and the peak spill. On the public prefix used throughout
these pages (give your own bucket's prefix and credentials instead):

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ -o sample --anonymous --s3-region us-east-1 \
  --report sample.json
jq '{peak_rss_bytes: .process.peak_rss_bytes,
     bytes_per_row: (.output.output_bytes / .output.rows_committed),
     spill_peak_bytes: .diagnostics.resources.spill.peak_bytes}' sample.json
```

```text
  complete: 5,252 objects, 383 GiB in 1.0s → sample (1 part, 121 KiB)
  next: swath show sample · swath du --from-dataset sample
{
  "peak_rss_bytes": 64380928,
  "bytes_per_row": 23.62966488956588,
  "spill_peak_bytes": 0
}
```

Without `jq`, the same fields are in `sample.json`: `process.peak_rss_bytes`, `output.output_bytes` over
`output.rows_committed`, and `diagnostics.resources.spill.peak_bytes`. A small prefix may never spill, as
this one did not: a spill peak of 0 says nothing about the whole bucket, so size its spill with the rule below.

## Disk sizing

A sorted run needs room for the dataset and for its spill: the rows it has listed ahead of the output, kept on
disk until the output reaches them. A first listing of a large bucket spills most of its rows once. On the
measured S3 buckets:

- **The dataset:** 23 to 30 bytes an object at the default part size: 865,491,977 bytes for 37,064,981
  objects, and 32.6 GB for 1.09 billion. Keys and columns of another shape give another figure: measure your
  own from a representative prefix, the report's `output.output_bytes` over `output.rows_committed`.
- **Spill per row:** 18 to 30 bytes: 18 to 22 in 35 production listings of 50.6 billion objects in all,
  with 0.4.0-rc.2 writing the dataset to a bucket
  ([measurements](internals/measurements.md#spill-at-billion-scale-and-a-spill-cap-too-small-2026-11-13)), 30 on
  the 1.09-billion-object bucket above. A benchmark of production keys that end in random identifiers, with
  random (synthetic) MD5 ETags, reached 53.2 bytes ([measurements](internals/measurements.md#delta-coded-spill-frames)).
- **Rows ahead of the output at the peak** (listed and not yet written, in memory or spilled; past the sort
  buffer they are spilled): 96 to 99 % in the same 35 listings. On lists of 20 to 150 million keys through a
  test service (builds of 2026-11-07), a median 71 % at 64 workers
  ([measurements](internals/measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms))
  and 94 % at 256 ([measurements](internals/measurements.md#workers-one-core-and-the-study-harness-through-the-fake)).

Peak spill is at most the rows, times that share, times the spill bytes per row. By default spill may use half
the free space left after a reserve, as the dataset needs the rest; the reserve is 1 GiB, or one part (64 MiB
by default) per encoder when more than 16 encoders start (the report's
`diagnostics.resources.spill.reserve_bytes`). So a dataset on the local disk needs free space of about the
dataset, plus twice the peak spill, plus the reserve.

### Example: 200 million objects

- The dataset: 200,000,000 × 30 B = 6.0 GB.
- Peak spill, low: 71 % of the rows ahead at 30 B, 0.71 × 200,000,000 × 30 B = 4.26 GB. High: every row at
  53.2 B, 200,000,000 × 53.2 B = 10.64 GB.
- The reserve: 1 GiB = 1.07 GB, with at most 16 encoders at the start.
- Free space: 6.0 + 2 × 4.26 + 1.07 = 15.59 GB, up to 6.0 + 2 × 10.64 + 1.07 = 28.35 GB: about 16 to 29 GB.

The low end assumes the median 71 %: at 94 % and 30 B the same formula needs 6.0 + 2 × 5.64 + 1.07 =
18.35 GB. A machine that starts 32 encoders has a 2 GiB (2.15 GB) reserve, which makes the top 29.43 GB.
Round up and leave headroom: the formula is a planning rule, and both inputs are assumptions until a
representative prefix measures them. With less disk, listing slows to the
output's pace: a run a little short of disk still completes, and a run far short of it may not finish in any
useful time. It says so after 2 minutes, and `--spill-wait-timeout` stops it
([disk](watch-a-run.md#disk)). Set `--spill-budget` when you know the room ([Disk](#disk)).

### Other cases

- **A repeat listing seeded from the previous dataset** (`--seed-from` with the dataset, or a cut file with
  counts) aims to keep about 8 million rows ahead of the output instead of the whole bucket, so it usually
  spills a small fraction of a first listing ([warm start](resume-and-refresh.md#warm-start-for-repeat-listings)).
  That is a target, not a bound on rows or disk: a part of the key space already being listed keeps listing
  whatever it holds, so coarse parts of the previous dataset, a part of the bucket that grew since it, keys
  added after its last key, and the spill files a run keeps for a resume until their rows are published can
  all go past it. Size the disk
  for the dataset and that growth, and take the spill a seeded run needs from its report's
  `diagnostics.resources.spill.peak_bytes`. A cut file without counts spills as a first listing does.
- **A bucket of unknown size:** nothing reports the object count before the listing, so the first listing is
  the one that can outgrow its disk. Give it room to spare or a disk that can be grown while it runs (the
  default spill cap follows it), bound it with `--max-duration`, and size later runs from its manifest's row
  count.
- **A dataset in a bucket** (`-o` a bucket prefix, with `--work-dir`): the work directory holds no part. It
  needs room for the spill, a 1 GiB reserve, and the run's records, which grow with the number of parts and
  the length of the keys (each part's records hold its first and last key): read their size in a
  representative run's work directory. Spill may use all its free space after the reserve: on a disk that can
  be grown while the run goes on, start modest and grow it if spill fills it. On the example above, that is
  4.26 to 10.64 GB of spill plus the reserve and the records.

## Workers

**Default:** 64 for an object store, whatever the CPU count; 4 per CPU for a local directory, at most 64. An
object-store listing waits on the service, not the CPU. A local walk waits on the disk: more workers past a
few per CPU only add memory and contention (raise it for a network filesystem).

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

## Memory

**Default `--sort-buffer`:** 1/16 of the memory found, at most 1 GiB. The memory found is the least of the
physical memory, the container's (cgroup's) limit and `GOMEMLIMIT`; the report's `diagnostics.sort_buffer`
says which it was. When none is found, 128 MiB. Less than 128 KiB is refused (exit 2).

| Memory found | 2 GiB | 4 GiB | 8 GiB | 16 GiB or more | none |
|---|---:|---:|---:|---:|---:|
| Default `--sort-buffer` | 128 MiB | 256 MiB | 512 MiB | 1 GiB | 128 MiB |

The sort buffer holds the rows a sorted run has listed ahead of its output; past it they spill to disk. It is
not a limit on the process, and swath sets none: the requests in flight, the Parquet writers (admitted against
an allowance of their own, also 1/16 of memory: [Encoders](#encoders)), compression and the Go runtime come on
top, and after listing ends the run can hold about one more sort buffer while it writes the last parts.
`GOMEMLIMIT` is a soft target for Go-managed memory, not a limit on the resident set; an operating system or
container limit can stop the process when memory cannot be reclaimed.

**When to change it.** Raise it when many rows spill and memory is free. When the peak comes near the
machine's memory, lower it together with `--workers` and `--encoders`: a smaller sort buffer alone spills more
rows without always lowering the peak. **If it is wrong**, the system stops a run that outgrows the machine or
the container: [memory](watch-a-run.md#memory).

Outside the sort buffer, besides the above:

- **A local listing's largest directory:** a listing reads each directory whole to sort it, so a directory of
  millions of files holds all its names in memory at once ([how much](internals/providers/local.md)). A count
  reads large directories a part at a time on Linux; on macOS and Windows it reads each one whole too.
- **Azure Blob with a counted `--seed-from`** (a previous dataset, or a cut file with counts): pages of 5,000
  entries instead of 1,000, about five times the memory per request in flight.
- **A `.zst` text file:** as many compression jobs as `--encoders`, a few MiB each
  ([how much](internals/run-lifecycle.md#encoder-sizing)); lower `--encoders` to lower it. Gzip is one stream.
- **`swath du` by prefix:** one entry per prefix, capped by `--max-prefixes` ([totals](totals.md)).
- **Spill on tmpfs** (a common `/tmp`) is memory, outside the process's peak; swath warns when it spills there
  ([memory](watch-a-run.md#memory)). Put the output on a disk.

## Disk

**Default spill cap:** half the free space on the spill's filesystem after the reserve, as the output shares
it; all of it for a dataset in a bucket. The default follows the disk: a filesystem grown under the running
process is used without a restart. `--spill-budget SIZE` sets a fixed cap instead.

Spill goes:

- for a dataset, under `DIR/.swath/spill/` (for a dataset in a bucket, `DIR` is the `--work-dir`);
- for a text file, in a scratch directory beside it, `.NAME.swath-ATTEMPT-RANDOM/`, which the run removes when
  it ends;
- for rows on stdout, under `$TMPDIR`;
- for `swath du`, nowhere: a count never spills.

A dataset run of an object store also keeps some spill files for a resume, counted against the same cap. A killed text run leaves
its scratch directory and its `.NAME.*.tmp` behind: delete both.

**When to change it.** Set `--spill-budget` when the filesystem is shared with other work. A smaller cap trades
disk for a listing that waits on the output: measure a representative run's
`diagnostics.resources.spill.peak_bytes` before shrinking the disk.

**If it is wrong.** Past the cap, or at the reserve, listing waits instead of failing and slows to the output's
pace; after 2 minutes swath says so, and `--spill-wait-timeout` stops it (exit 74; a dataset run of an object
store resumes). That holds for rows spill cannot take; another spill write that fails, such as a full disk
that cannot be undone or the end of a range's spill file, stops the run (a dataset run of an object store
resumes; other runs start again). A dataset that outgrows the disk stops the run with exit 74, resumable once there is space.
What you see and what to do: [disk](watch-a-run.md#disk).

## Encoders

**Default:** one Parquet writer per CPU (`GOMAXPROCS`), fewer when they do not fit an allowance of 1/16 of the memory found
(at most 1 GiB, whatever `--sort-buffer` says) or the open-file limit; more may start once listing ends.
Writing to a bucket, each also holds one upload buffer (8 MiB by default). For a `.zst` text file,
`--encoders` is the number of compression jobs.

**When to change it.** Lower it to leave cores and memory to other work. An explicit `--encoders N` is kept as
given, never raised, with a warning when the machine cannot carry it. How many start on a given machine:
[encoder admission](internals/ordering.md#the-budget).

## Part size

**Default:** `--part-size 64MiB`, no row cap. Smaller parts suit readers that load whole parts; larger ones mean
fewer files. `--part-max-rows N` also closes a part at N rows. A dataset holds at most 100,000 parts. At 64 MiB
parts and about 30 bytes a row that is about 200 billion objects, but parts closed early by the 30-second time
cut (`--part-interval`), larger rows or a small part size reach it sooner (exit 1, `cause` `part_limit`; start
over with a larger `--part-size`).

## Cost

Object stores charge per listing request. An S3 or GCS request returns up to 1,000 keys, an Azure Blob one up
to 5,000. A run sends about one request per page of keys, plus the seed's delimiter listings, the probes that
pick split points, and retries; a run seeded from a previous dataset (`--seed-from`) comes close to one per
page, and on Azure Blob asks for pages of 5,000 with a counted prior and the default page policy. The run
report counts every HTTP attempt in `requests.http_attempts`, and on AWS S3 and GCS's own endpoints estimates
their cost in `cost.cost_usd` at the provider's reference list rate (0.005 USD per 1,000 requests); a custom endpoint and
Azure Blob have no estimate. The progress line shows the same estimate as the run goes. Pricing varies by
provider, region and time: check the provider's current price for a budget. A resume lists again what was
not yet published, except the rows it replays from the spill files it kept, so an interrupted run costs more
than an uninterrupted one.
