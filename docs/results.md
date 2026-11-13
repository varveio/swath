# Understand your results

What swath's totals and inventories cover, and what they leave out, shown on the example tree from
[Your first inventory](first-inventory.md).

## Scope

A run covers its source and nothing else: the tree below a local directory, or the keys of a bucket or
container that start with the prefix you named. An object-store prefix is a string prefix, not a directory:
`s3://bucket/logs` covers `logs/` and `logs-old/`, while `s3://bucket/logs/` covers only `logs/`.

On a local tree swath does not follow symbolic links; it lists each link as an entry of kind `SYMLINK` with its
target. A mounted filesystem below the source is walked like any directory unless you pass
`--one-file-system`. Filters such as `--include` or `--min-size` narrow the rows further. A dataset records
both the scope and the filters, so you can tell later what it was meant to hold ([sources](sources.md),
[filters](filters.md)).

## Apparent and allocated bytes

`size` is a file's apparent size: its length, as `ls -l` shows it. It is not the disk space the file takes.
In the example, `swath du` reports 100 MiB, but `swath du --allocated` reports 36.4 MiB, close to what
`du -sh` prints for the same tree:

```text
allocated_size  files  dirs  last_modified (local)  path
       784 KiB      2     0  2026-11-02 09:30:00    archive/
      6.11 MiB      3     2  2026-01-20 12:00:00    photos/
      29.6 MiB      1     0  2026-11-02 09:30:00    videos/
      36.4 MiB      6     5  2026-11-02 09:30:00    (total)
```

The difference is a sparse 64 MiB disk image that occupies no blocks; compression and block rounding also
make the two differ. Each row of `swath list` carries both, as `size` and `allocated_bytes`.
A directory's row in `swath list` has its own size, but `swath du` counts directories separately and leaves
their sizes out of its byte totals.

For an object store, `size` is the object's length as the listing reports it. It is not the stored or billed
size: storage classes, minimum storage durations, versions and replication are outside it.

## Hard links

A file with several hard links has several names, and `swath list` writes each name as a row. `swath du`
counts the file once, as `du` does, under the first of its names in byte order. In the example, `notes.tar`
and `notes-latest.tar` are one file of 800,000 bytes, counted once; `-l` (`--count-links`) counts it under
both names, and the total becomes 101 MiB. In a listing, the `inode`, `dev` and `nlink` columns identify
names of the same file.

> [!NOTE]
> 0.4.0-rc.2 counts a hard-linked file under each of its names unless you pass `--count-links-once`, and has
> no `-l`: on the example it reports 101 MiB, and 37.2 MiB with `--allocated`.

## Coverage and unreadable entries

An entry swath cannot read, such as a directory without read permission, is missing from the output with
everything below it. The rest is still listed and written; swath names the entry on stderr, ends with a line
that counts the missing entries (it is printed under `-q` too), and exits with code 3:

```text
swath: warning: unreadable entries:
  /home/you/media/private: permission denied
swath: 1 entry could not be read and is missing from the output
```

A symbolic link whose target cannot be read keeps its row and is not counted, and an entry removed while
swath walks the tree is no longer part of it. A source directory that cannot be opened at all stops the run
with exit code 1 ([troubleshooting](troubleshooting.md#entries-could-not-be-read)).

> [!WARNING]
> On 0.4.0-rc.2 the same run exits with code 0: it warns
> `1 entry could not be read: the output leaves them out`, says nothing with `-q`, and an unreadable source
> directory gives empty totals instead of an error. Its `swath show` prints `skipped=1`, and its manifest names
> the count `coverage.skipped_entries`. On rc.2, exit code 0 does not prove a complete scan: check that count
> ([differences in 0.4.0-rc.2](release-candidate.md)).

A dataset keeps the count. `swath show` prints it on the coverage line:

```text
coverage complete=true stop=completed unreadable=1 filters=0 exclusions=0
```

`complete=true` says only that the listing reached the end of its scope. The dataset holds the whole scope
when `stop=completed`, `unreadable=0`, `filters=0` and `exclusions=0` too, or when you accept its exclusions
explicitly. `exclusions` names what was left out by design, such as an output directory inside the tree. A
program reads the same fields from the manifest ([dataset](dataset.md#the-swath-extension)); the run report's
`outcome.unreadable` holds the same count ([run report](reference/run-report.md)).

## Observation time

swath publishes the complete result of the live listing it performed. It is not a point-in-time snapshot of a
source that changes during the run. Each row records an entry as swath saw it when it read that entry; a file
created, changed or removed while swath lists may or may not appear. The manifest records when the listing
started and finished (`coverage.listing_started_at`, `coverage.listing_finished_at`), and `swath show` prints
when the dataset was published. `last_modified` is the entry's own modification time, not the time swath saw
it.

## Live and saved results

`swath du` and `swath list` to a terminal, a pipe or a text file are live: each run lists the source again,
and an interrupted run starts over. A dataset is saved: it describes the source as it was during that
listing, and reading it later does not check the source again. To see changes, list again into a new
directory. A dataset run of an object store can be resumed after an interruption
([stopping and resuming](running.md#stopping-and-resuming)); a local listing is run again.
