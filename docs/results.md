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

## Sizes and hard links

`size` is a file's apparent size, its length as `ls -l` shows it, or an object's length; `allocated_bytes` is
the space the filesystem reports for a file (`on disk`), and an object store has none. A file with several hard
links is one row per name in a listing, and `swath du` counts it once. [How sizes are
counted](totals.md#how-sizes-are-counted) explains both figures, their limits and hard links. In a listing, the
`inode`, `dev` and `nlink` columns identify the names of the same file.

## Coverage and unreadable entries

An entry swath cannot read, such as a directory without read permission, is missing from the output with
everything below it. The rest is still listed and written; swath names the entry on stderr (relative to
SOURCE), its closing line's verdict counts the missing entries, and it exits with code 3. Under `-q`, which prints
no closing line, a last line `swath: N entries could not be read and are missing from the output` says it:

```text
$ swath list media -o inv
swath: warning: unreadable entries:
  private: permission denied
  partial: 1 unreadable entry; 13 entries, 101 MiB in 0.0s → inv (1 part, 4.04 KiB)
  next: swath show inv · swath du --from-dataset inv
```

`swath du` leaves the entry out of its totals in the same way and marks it ([Entries that could not be
read](totals.md#entries-that-could-not-be-read)).

A symbolic link whose target cannot be read keeps its row and is not counted, and an entry removed while
swath walks the tree is no longer part of it. A source directory that cannot be opened at all stops the run
with exit code 1 ([troubleshooting](troubleshooting.md#entries-could-not-be-read)).

A dataset keeps the count. `swath show` prints its verdict first, and the count on its coverage line:

```text
inv: partial: 1 unreadable entry
  ...
  coverage  1 entry unreadable; no filters; no exclusions
```

The verdict is `complete` when nothing is missing: the listing reached the end of its scope and read every
entry. Otherwise it is `partial:` and what is missing (`1 unreadable entry`, or `--max-keys reached` for a
diagnostic `swath diag list` sample). The
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
