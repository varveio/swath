# Total a source

`swath du` totals a source's objects (a local tree's files), their bytes and their newest time, per prefix
down to a depth, or reads the same totals back from a saved dataset. The rows' fields and the care their totals
need are in [Output](output.md#du-rows); every option is in [swath du](commands/swath-du.md).

## Totals per prefix

With `-d N`, `swath du` prints the total of each prefix down to N levels below SOURCE, a parent including
its children:

```text
$ swath du -d 1 /data/photos --format table
    size  files  dirs  last_modified (CET)  path
 410 KiB      2     1  2026-03-01 11:00:00  2025/
48.8 KiB      1     0  2026-11-02 10:30:00  2026/
 459 KiB      5     3  2026-11-02 10:30:00  /data/photos (total)
  459 KiB in 5 files · 472 KiB on disk
```

A prefix ends at a `/`. Depth counts from SOURCE's prefix up to its last `/`: `s3://b/logs/2026/` counts from
`logs/2026/`, `s3://b/logs` from the top of the bucket. A key with fewer than N slashes counts only in the
prefixes it has. Each row includes everything below it, so add up rows of one depth only, never across depths.
The table's total row names SOURCE as typed (`s3://b/logs/2026/0 (total)`); in TSV and JSONL the total's
`prefix` is the key prefix SOURCE names (`logs/2026/0`), which every key it counts starts with.

With `-d N` (or `--format table`) on a terminal the rows are a table with the total last; in a pipe they are TSV
(`--no-header` leaves out its header line), and `--format jsonl` gives JSONL; both put the total first, its
`row_type` `total`. The fields,
and the care their totals need, are in [Output](output.md#du-rows).

TSV and JSONL rows have two byte figures: `size`, the objects' lengths as listed, and `allocated_bytes`, a local
file's allocated blocks (`st_blocks` × 512; a sparse file counts its blocks, not its length), `on disk`. The
table shows one of them, and on a local tree a line on stderr under it gives the totals of both.
Neither is the storage consumed, replicas or a bill ([Apparent and allocated
bytes](results.md#apparent-and-allocated-bytes)). A bucket has no allocated bytes: its rows carry them as
null and its table has no on-disk line. A local tree's directories are counted in `dirs`, never as objects,
and their own size and blocks are in no total, so a tree's allocated bytes are below `du -sB1`'s by them. With
`--all-versions` every version is an object; an S3 delete marker adds its prefixes but no object, bytes or
time. `max_last_modified` is the newest time among the objects counted; the table shows it in local time to
the second, under a heading that names the zone, as `swath list`'s table does, and `-` when no object had one.

The table shows the size by default (`--apparent-size`). On a local tree, `--allocated` shows the allocated
bytes instead and `--sort size` orders by them; the rows' `primary` says which. `--allocated` needs each entry's stat, so
`--metadata names` refuses it, and a file whose filesystem reports no blocks fails the count.

A local file with several hard links counts once, as `du` counts it: its object and bytes go to the prefixes of
its first name in byte order, so the totals do not depend on which name the walk reached first. swath remembers
each such inode (only files with more than one link; about 100 MB a million), up to `--max-inodes` (1,000,000 by
default). Past it the run fails rather than count some names twice: exit 1, cause `inode_limit`, no totals, and
its last line says what to do:

```text
swath: the tree has more than 1,000,000 files with several hard links, which swath du counts once each: run it again with -l to count every name, or with --max-inodes N to remember more of them (about 100 MB a million)
```

`-l`, `--count-links` counts every name, as `du -l`. `--metadata names`, and Windows, read no link
counts, so every name counts there.

The [filters](filters.md) apply before counting, to every row: on a local tree directories too, so
`--min-size 1M` keeps the large files and drops the directories, whose own size is small.

Memory grows with the number of distinct prefixes, not objects. A run that would hold more than
`--max-prefixes` (1,000,000 by default, about 150 bytes each) fails rather than print partial totals.

Entries of a local tree that could not be read (permissions) are not in the totals: the run prints the totals of
the rest, names the entries on stderr and exits 3 ([exit codes](reference/exit-codes.md)). In the table, a
directory that could not be read shows `-` for its size and counts.

## Totals from a saved dataset

Each `swath du SOURCE` lists SOURCE again. To look at a large source at several depths, or with other filters,
list it once into a dataset (`swath list SOURCE -o DIR`) and total that instead (a live `swath du` that listed
a million entries or more says so on a terminal: its closing lines end with `hint:` lines giving both commands
for its SOURCE):

```text
$ swath du --from-dataset photos-inventory -d 1 --sort size
swath: totals of the dataset photos-inventory, not a live listing: /data/photos as listed 2026-11-12 11:00:03 CET (3 hours ago); complete
    size  files  dirs  last_modified (CET)  path
 410 KiB      2     1  2026-03-01 11:00:00  2025/
48.8 KiB      1     0  2026-11-02 10:30:00  2026/
 459 KiB      5     3  2026-11-02 10:30:00  photos-inventory (total)
  459 KiB in 5 files · 472 KiB on disk
```

The rows are the ones `swath du` of the dataset's source printed when it was listed, with the same depth, order,
formats and filters. They are read from the dataset's Parquet parts: the key and only the columns the totals
and filters need, in one parallel pass, once every part's columns are checked against the dataset's schema (a
dataset that breaks it is refused, exit 1). Depth counts from the dataset's scope, as it would from the source.
Without `--from-dataset`, `swath du DIR` totals the files in DIR, never the rows of the dataset they hold; when
DIR holds a dataset, a `hint:` line on stderr gives the `--from-dataset` command.

An optional SOURCE after the directory takes the form a live `swath du` takes: the dataset's source as it was
listed (the `source` stderr names), which totals the whole dataset as no SOURCE does, or a path or URL below it,
which restricts the totals to the keys below it and prints what a live `swath du` of it prints. A local path
is made absolute, as a live `swath du` resolves it, and taken as a directory below the listed tree:
`/data/photos/2025` totals the keys under `2025/`, labelled and filtered relative to it, with depth counted
from it. A path that names a file in the tree totals that file's row alone, as a live `swath du` of the file
does; a symbolic link is refused (exit 2), since a live `swath du` follows one to a directory whose keys the
dataset holds under the target's path. A URL names the same bucket or container and a prefix that starts with the listed one
(`s3://bucket/logs/2026/` of a dataset listed from `s3://bucket/logs/`); its rows keep whole keys, as a live
run's do, and depth counts from its prefix up to the last `/`. The read seeks to the prefix rather than
scanning the dataset. A SOURCE below the listed one that holds no key prints zeros
and warns, exit 0, as a live listing of an empty prefix. Any other SOURCE is refused (exit 2), naming the
dataset's: `"photos": photos-inventory was listed from /data/photos; give a path below it, or no argument`.

The output says the totals are saved rows. A line on stderr names the dataset, its source, when it was listed
and its coverage verdict (`complete`, or `partial:` and what is missing: `2 unreadable entries`, `--max-keys
reached`), then the listing's filters and exclusions. `-q` silences that line. In JSONL each row also carries a `dataset` member with
the same facts, its `filters` as the manifest's `coverage.filters` records them ([Output](output.md#du-rows)). A dataset whose listing did not complete, such as a
`--max-keys` sample, or that left out entries it could not read (`coverage.unreadable`), prints its totals and
exits 3 (`incomplete`), as the run that wrote it did, its last line saying why. A file with several hard links
counts once where the dataset records links (a local tree's, listed with stat), as a live `swath du` counts it;
`-l` counts every name. A dataset listed with `--metadata names` holds no lengths, blocks or links, so its rows
are those of a live `--metadata names` run: both figures null under a prefix with files counted, and
`links_once` false. With nothing counted (an empty tree, or every file filtered out) both figures are 0, and
`links_once` is still false, as live, since the manifest records the tier.

`--from-dataset` reads a committed local dataset directory. A dataset in a bucket is refused as not supported
yet. A directory holding an interrupted run is refused with the `swath show` and `swath resume` commands that
handle it. The options that shape a listing are refused by name: provider, credential, performance, limit and
report options, and `--all-versions`, since the dataset's schema decides that. So are `--allocated`,
`--count-links` and `--storage-class` when the dataset's schema has no column for them. The filters, the
totals options, `--format`, `--no-header` and the terminal options `-v`, `-q`, `--color`, `--log-format`, `--no-progress`
and `--no-stats` apply. A duration in `--modified-since` counts back from now, not from the listing.
