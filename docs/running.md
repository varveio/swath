# Run large listings

This page lists a bucket of your own into a saved inventory, start to finish: the command, what it prints while
it runs and when it ends, how to stop it, and where the result is. It assumes you have followed
[Your first inventory](first-inventory.md) and can reach your store: its URL, credentials and permissions are on
its [source page](sources/README.md).

## The command

```bash
swath list s3://bucket/prefix/ -o inventory --max-duration 6h
```

- `s3://bucket/prefix/` is what to list: a bucket, or a prefix in it. `gs://bucket/prefix/` and
  `az://account/container/prefix/` work the same way.
- `-o inventory` writes a swath dataset, sorted Parquet parts and a `manifest.json` that commits them, to the
  directory `inventory`. It must be new or empty: a non-empty one is refused (exit 2) unless you give
  `--overwrite`, which replaces only a previous swath dataset. Put it on a disk with room for the dataset and for
  the spill a sorted run writes there on its way ([disk sizing](size-a-run.md#disk-sizing)). When you list a
  mounted filesystem, keep the directory outside the tree you list.
- `--max-duration 6h` stops the run if it is still going after six hours (exit 124); `swath resume` can
  continue it. There is no limit by default. Give one to every run you will not watch: it is the one stop for a
  service that keeps answering without the listing ever ending.

The workers (64 by default), memory and disk need no flag for a first run. For a bucket of hundreds of
millions of objects, read [Size a run](size-a-run.md) first.

## While it runs

On a terminal, swath draws one progress line on stderr, redrawn every second:

```text
  listing · 1,441,002 objects · 51.6 TiB · 2.0s elapsed · 969,817 keys/s (avg 720,501) · 0 parts · 63 in flight · 1,510 API calls
```

It shows the objects listed and their bytes, the time and the rate, the Parquet parts written, then the
requests in flight and the requests sent, and last an estimate of their cost when swath knows the store's price
(left out here). There is no percentage: a listing does not know its total in advance. On a large bucket
`parts` can stay at 0 for a long time: swath lists the whole key space at once, holds the rows that are not yet
next in key order (on disk once its memory budget is full), and writes them out in order, much of it near the end of
the run.

When stderr is not a terminal (a log, a scheduler), add `--progress` to get the same line every 30 seconds.
What each number means, the file `inventory/.swath/progress.json` and the signs of trouble are in
[Watch and tune a run](watch-a-run.md).

## When it ends

The run ends with two closing lines on stderr. On a public prefix of 5,252 objects:

```text
  complete: 5,252 objects, 383 GiB in 0.9s → inventory (1 part, 121 KiB)
  next: swath show inventory · swath du --from-dataset inventory
```

`complete` and exit code 0 mean the dataset is committed and covers the whole prefix. The directory now holds
`data/` (the Parquet parts), `manifest.json`, `run.json` (the run report), `cuts.txt` (for the next listing,
[below](#list-it-again)) and `.swath/` (the run's workspace, which readers ignore). `swath show inventory`
prints the verdict, what was listed and when, and the coverage; [Query an inventory](query.md) reads the rows.

A run that stops early says why on its first closing line, and on the second what to run next:

| Closing line | Exit | What to do |
|---|---|---|
| `interrupted` | 129, 130, 143 | `swath resume inventory` ([below](#stop-it-and-continue)) |
| `stopped: --max-duration reached` | 124 | `swath resume inventory`, which keeps the recorded deadline; `swath resume inventory --max-duration 0` lifts it |
| `partial: N unreadable entries` (a mounted filesystem) | 3 | the dataset is committed without the entries that could not be read; fix their permissions and list again ([coverage](results.md#coverage-and-unreadable-entries)) |

A run that fails prints one `swath:` line with the cause instead, such as a bucket that does not exist or access
that is denied (exit 1). [Troubleshooting](troubleshooting.md) has each message and its fix, and
[Exit codes](reference/exit-codes.md) every code, the stuck run (75) and the full disk (74) among them.

<a id="limits-and-the-outcome-of-record"></a>For a script, the exit code and a file in the directory are the
outcome: `inventory/run.json` beside a committed dataset, else `inventory/error.json`, each with `class`,
`exit_code` and `resumable`. `--report FILE` keeps a copy of each attempt's full report: give each attempt a
new path outside the output directory ([Run report](reference/run-report.md)).

## Stop it and continue

Ctrl-C (SIGINT), SIGTERM, and closing the terminal (SIGHUP, unless the run is under `nohup`) stop the run
cleanly: the parts already written stay, and `error.json` records the stop.

```text
  interrupted: 0 saved, 2,233,277 listed in 3.0s · 746,666 keys/s → inventory
  next: swath resume inventory
```

`saved` counts the rows in written parts; rows listed but not yet saved may be listed again. To continue:

```bash
swath resume inventory
```

The resume replays the recorded command, keeps the saved parts and continues after them. `swath show inventory`
says where a stopped run is. To start over instead, run the command again with `--overwrite`, or into a new
directory. A run of a mounted filesystem keeps no journal and cannot be resumed: run it again with
`--overwrite`. What a resume keeps and when it is refused: [Stop, resume and refresh](resume-and-refresh.md).

## List it again

To refresh the inventory later, list into a new directory and pass the previous dataset as `--seed-from`:

```bash
swath list s3://bucket/prefix/ -o inventory-2 --seed-from inventory --max-duration 6h
```

The previous dataset must be of the same bucket and prefix (S3, GCS and Azure Blob; a mounted filesystem
takes none). It tells swath how the keys are spread, so the run divides the work up front instead
of finding it as it goes; the rows are the same
([warm start](resume-and-refresh.md#warm-start-for-repeat-listings)). Keep the old directory until readers
have moved to the new one. `--overwrite` replaces a dataset in place instead
([overwrite and refresh](resume-and-refresh.md#overwrite-and-refresh)), and a nightly run under a supervisor is
in [Run on a schedule](production.md).

## More

- <a id="sizing-a-run"></a><a id="disk"></a><a id="memory"></a><a id="cost"></a>[Size a run](size-a-run.md):
  workers, disk, memory and the cost of requests.
- <a id="watching-a-run"></a><a id="throttling"></a>[Watch and tune a run](watch-a-run.md): the progress line
  and file, the signals to read, throttling and the flags to change.
- <a id="stopping-and-resuming"></a><a id="overwrite-and-refresh"></a><a id="seeding-a-refresh-from-the-previous-dataset"></a>[Stop, resume and refresh](resume-and-refresh.md):
  signals, `swath resume`, replacing an inventory and seeding a refresh.
- [Run on a schedule](production.md): a nightly run under a supervisor, a dataset written to a bucket,
  preemptible machines.
- <a id="listing-every-version"></a>[List every version](all-versions.md): S3 versions and delete markers, GCS
  generations, Azure Blob versions, snapshots and soft-deleted blobs.
- <a id="reading-the-output"></a><a id="verify-a-dataset"></a>[Query an inventory](query.md): reading and
  verifying the dataset a run writes.
- <a id="known-limits"></a>[Known limits](troubleshooting.md#known-limits): platforms, power loss, live scans
  and the scale measured so far.
