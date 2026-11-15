# Understand your results

What swath's totals and inventories cover, and what they leave out, shown on the example tree from
[Your first inventory](first-inventory.md).

## Scope

A run covers its source and nothing else: the tree below a local directory, or the keys of a bucket or
container that start with the prefix you named. An object-store prefix is a string prefix, not a directory:
`s3://bucket/logs` covers `logs/` and `logs-old/`, while `s3://bucket/logs/` covers only `logs/`.

In a directory, swath does not follow symbolic links; it lists each link as an entry of kind `symlink` with its
target. A mounted filesystem below the source is walked like any directory unless you pass
`--one-file-system`. Filters such as `--include` or `--min-size` narrow the rows further. A dataset records
both the scope and the filters, so you can tell later what it was meant to hold ([sources](sources/README.md),
[filters](filters.md)).

## Apparent and allocated bytes

`size` is a file's apparent size: its length, as `ls -l` shows it. It is not the disk space the file takes.
`allocated_bytes`, `on disk` in the table, is the space the filesystem reports for it. `swath du`'s table
shows the size, and the line under it the totals of both: the example tree holds 100 MiB of file lengths in
36.4 MiB on disk, close to what `du -sh` prints for it:

```text
$ swath du -d 1 media
    size  files  dirs  last_modified (CET)  path
64.8 MiB      2     0  2026-11-02 10:30:00  archive/
6.10 MiB      3     2  2026-11-02 10:30:00  photos/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
 100 MiB      6     5  2026-11-02 10:30:00  media (total)
  100 MiB in 6 files · 36.4 MiB on disk
```

Most of the difference is a sparse 64 MiB disk image that occupies no blocks. `--allocated` shows the
allocated bytes instead, and `--sort size` then orders by them:

```text
$ swath du -d 1 media --allocated
 on disk  files  dirs  last_modified (CET)  path
 784 KiB      2     0  2026-11-02 10:30:00  archive/
6.11 MiB      3     2  2026-11-02 10:30:00  photos/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
36.4 MiB      6     5  2026-11-02 10:30:00  media (total)
  36.4 MiB on disk in 6 files · 100 MiB of data
```

TSV and JSONL rows carry both figures either way, as each row of `swath list` does, under the same names.

`size` totals the lengths of the listed entries that are not directories; a symbolic link's is the link's
own. `allocated_bytes` totals their filesystem-reported allocation, `st_blocks` × 512 bytes. It is what the
filesystem says, not unique physical usage or the space a deletion would free:

- Directory blocks, the source directory's included, are left out, so a tree's figure is below `du -sB1`'s by
  them. A directory's row in `swath list` has its own size and blocks.
- A file with several hard links counts once where its links can be identified, under its first name in byte
  order; `-l` counts every name ([Hard links](#hard-links)).
- Sparse files, compression, data stored inline in the metadata and cloud placeholder files can report less
  allocation than their length, down to zero; block rounding and preallocation can report more.
- Shared extents (deduplicated files, APFS clones, Btrfs and XFS reflinks) are counted for each file that
  shares them, so a total can exceed the filesystem's capacity.
- Network filesystems may report a synthetic allocation.
- An unknown allocation is not zero. Windows and `--metadata names` read none: the figure is null (`-` in the
  table), and `allocated_bytes_missing` counts the files without it.

For an object store, `size` is the object's length as the listing reports it. It is not the stored or billed
size: storage classes, minimum storage durations, versions and replication are outside it. An object store has
no `allocated_bytes`: the figure is null and the table leaves its column out.

## Hard links

A file with several hard links has several names, and `swath list` writes each name as a row. `swath du`
counts the file once, as `du` does, under the first of its names in byte order. In the example, `notes.tar`
and `notes-latest.tar` are one file of 800,000 bytes, counted once; `-l` (`--count-links`) counts it under
both names, and the total becomes 101 MiB (37.2 MiB on disk). Each row's `links_once` says which way it
counted. In a listing, the `inode`, `dev` and `nlink` columns identify
names of the same file.

> [!NOTE]
> 0.4.0-rc.2 counts a hard-linked file under each of its names unless you pass `--count-links-once`, and has
> no `-l`: on the example it reports 101 MiB, and 37.2 MiB with `--allocated`.

## Coverage and unreadable entries

An entry swath cannot read, such as a directory without read permission, is missing from the output with
everything below it. The rest is still listed and written; swath names the entry on stderr, ends with a line
that counts the missing entries (it is printed under `-q` too), and exits with code 3:

```text
$ swath du -d 1 media
    size  files  dirs  last_modified (CET)  path
64.8 MiB      2     0  2026-11-02 10:30:00  archive/
6.10 MiB      3     2  2026-11-02 10:30:00  photos/
       -      -     -  -                    private/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
 100 MiB      6     6  2026-11-02 10:30:00  media (total)
  100 MiB in 6 files · 36.4 MiB on disk
swath: warning: unreadable entries:
  /home/you/media/private: permission denied
  partial: 1 unreadable entry; 6 files, 6 directories, 100 MiB in 0.0s
swath: 1 entry could not be read and is missing from the output
```

`swath du`'s table marks the directory it could not read with `-` for its size and counts, not `0`.

A symbolic link whose target cannot be read keeps its row and is not counted, and an entry removed while
swath walks the tree is no longer part of it. A source directory that cannot be opened at all stops the run
with exit code 1 ([troubleshooting](troubleshooting.md#entries-could-not-be-read)).

> [!WARNING]
> On 0.4.0-rc.2 the same run exits with code 0: it warns
> `1 entry could not be read: the output leaves them out`, says nothing with `-q`, and an unreadable source
> directory gives empty totals instead of an error. Its `swath show` prints `skipped=1`, and its manifest names
> the count `coverage.skipped_entries`. On rc.2, exit code 0 does not prove a complete scan: check that count
> ([differences in 0.4.0-rc.2](release-candidate.md)).

A dataset keeps the count. `swath show` prints its verdict first, and the count on its coverage line:

```text
inv: partial: 1 unreadable entry
  ...
  coverage  1 entry unreadable; no filters; no exclusions
```

The verdict is `complete` when nothing is missing: the listing reached the end of its scope and read every
entry. Otherwise it is `partial:` and what is missing (`1 unreadable entry`, `--max-keys reached`). The
coverage line names the filters and exclusions (`filtered: include=*.txt`, `excluded: ...`): a filtered
dataset is complete for its filters, and an exclusion is what was left out by design, such as an output
directory inside the tree.
The run that wrote the dataset printed the same verdict, and `swath du --from-dataset` prints it too. A
program reads `coverage.complete`, `stop_reason`, `unreadable` and `filters` from the manifest
([dataset](dataset.md#the-swath-extension)); the run report's `outcome.complete` and `outcome.unreadable` hold
the same ([run report](reference/run-report.md)).

## Observation time

swath publishes the complete result of the live listing it performed. It is not a point-in-time snapshot of a
source that changes during the run. Each row records an entry as swath saw it when it read that entry; a file
created, changed or removed while swath lists may or may not appear. The manifest records when the listing
started and finished (`coverage.listing_started_at`, `coverage.listing_finished_at`), and `swath show` prints
when the dataset was published. `last_modified` is the entry's own modification time, not the time swath saw
it; `swath du`'s `max_last_modified` is the newest of them under a prefix.

## Live and saved results

`swath du` and `swath list` to a terminal, a pipe or a text file are live: each run lists the source again,
and an interrupted run starts over. A dataset is saved: it describes the source as it was during that
listing, and reading it later does not check the source again. To see changes, list again into a new
directory. A dataset run of an object store can be resumed after an interruption
([stopping and resuming](resume-and-refresh.md#stopping-and-resuming)); a local listing is run again.
