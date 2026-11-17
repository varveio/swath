# Total a source

`swath du SOURCE` shows where the bytes are: on a terminal it draws the view, the largest prefixes and objects
(a local tree's directories and files) at any depth; in a pipe it prints the total row only. With `-d N` it
totals the objects, their bytes and their newest time per prefix down to N levels, a table or rows for
scripts. It can also read the same totals back from a saved dataset. The rows' fields and the care their totals
need are in [Output](output.md#du-rows); every option is in [swath du](commands/swath-du.md).

## The view

Bare `swath du SOURCE` on a terminal answers "where are the bytes": a tree of the largest prefixes and objects,
largest first, each under its parent. Each line gives the size, the count of objects below it (`files` on a
local tree, three significant digits: `12.0k`), its share of the total, a bar of that share, the date of the
newest modification below it (`max_last_modified`, local, the zone named on the total's line) and the path,
indented two blanks a level under its parent's, under one dim heading:

```text
$ swath du s3://demo-lake
    size  objects  share                        newest          path
 384 GiB     130k   100%  ████████████████████  2025-11-15 UTC  s3://demo-lake
 164 GiB        6  42.7%  ████████▌             2025-11-15        backups/
39.0 GiB        1  10.2%  ██                    2025-11-15          db-2025-05-01.dump
37.0 GiB        1   9.6%  █▉                    2025-11-15          db-2025-02-01.dump
37.0 GiB        1   9.6%  █▉                    2025-11-15          db-2025-04-01.dump
27.0 GiB        1   7.0%  █▍                    2025-11-15          db-2025-03-01.dump
24.0 GiB        1   6.3%  █▎                    2025-11-15          db-2025-01-01.dump
2.00 KiB        1   0.0%                        2025-11-15          (1 more)
 164 GiB    2.18k  42.6%  ████████▌             2025-11-15        warehouse/
 159 GiB    1.85k  41.5%  ████████▎             2025-11-15          events/year=2025/
14.4 GiB      159   3.8%  ▊                     2025-11-15            month=12/
14.1 GiB      167   3.7%  ▋                     2025-11-15            month=03/
13.9 GiB      162   3.6%  ▋                     2025-11-15            month=09/
13.7 GiB      156   3.6%  ▋                     2025-11-15            month=02/
 103 GiB    1.20k  26.9%  █████▍                2025-11-15            (8 more)
4.16 GiB      336   1.1%  ▏                     2025-11-15          (1 more)
27.7 GiB    8.06k   7.2%  █▍                    2025-11-15        logs/api/2025/
24.0 GiB     120k   6.3%  █▎                    2025-11-15        images/
4.54 GiB      201   1.2%  ▏                     2025-11-15        (2 more)
  384 GiB in 130,453 objects
```

Each parent that lists children lists its largest, then one `(N more)` line for the rest: how many there are,
what they hold together (size, count, share) and their newest date, so the lines under a parent always add up
to it. Nothing is hidden without being counted; only a `(N more)` line of 0 bytes is left out. A prefix with a
single child shares its line with it (`logs/api/2025/`). The view takes the terminal's height, less a few
lines, from the largest down: line by line, a fold or a prefix shown whole gives its largest child (a prefix a
fold for the rest too), the one whose child is the largest, so the largest prefixes open first. No line goes to
an entry under a floor, 0.5 % of the total (a quarter of one line's share on a short terminal): a long tail of
small prefixes stays in its fold, and when nothing above the floor is left the view ends, shorter than the
terminal. What is not shown is in the `(N more)` lines.

The columns fit the terminal's width, and a line never wraps: from 100 columns the bar has 20 cells (the rest
goes to the paths), around 80 up to 10 while the paths keep room, around 60 none; narrower, the share goes,
then the date. The size, the count and the path always stay; a path is written as the other terminal lines
write it (a control character as an escape) and cut in the middle, its end kept, where the line would pass
the width.

The view leaves room for every line printed after it (the line of totals, the unreadable line, the closing
line, the prompt), wrapped lines counted as they wrap, so its heading and total stay on screen.

One figure drives the whole view, its order, shares and bars: the size, or the allocated bytes with
`--allocated`. A line on stderr under it gives the total once, exactly, with the other figure where the source
has it (`127 MiB in 309 files · 64.2 MiB on disk`); `-q` leaves it out. Color follows `--color` and
`NO_COLOR`.

Every figure in the view is exact, and the memory does not grow with the listing. A prefix is complete when
one range of the listing held it whole, or, on a local tree, when its directory's listing has ended with its
subdirectories'. Of the complete prefixes and objects, the 100 largest of each depth are kept, far more than
a terminal shows; one dropped for 100 larger ones of its depth is counted in its parent's bytes, and which are
kept does not depend on the order the listing completes them in. A prefix cut by the edge of a range is summed
across ranges and completes once the ranges on both sides of that edge have been listed, so only the ranges
still listing hold such prefixes. The local directories
being listed, and the prefixes kept to the end because they hold a hard-linked file, are bounded by
`--max-prefixes`; past it the view keeps fewer levels rather than fail, and says so on stderr. The run
report's `diagnostics.totals_view` counts what was kept, dropped and merged.

The view is for reading. In a pipe `swath du` prints TSV rows, and an explicit `-d N`
(`-d 0` included), `--sort` (the view is always by size) or `--format` prints the table or the rows.
`--from-dataset` draws the same view from a saved dataset, line for line.

The tree with shares is the idea of [dust](https://github.com/bootandy/dust) and
[pdu](https://github.com/KSXGitHub/parallel-disk-usage) (both Apache-2.0), implemented from their documented
behavior. It differs in what it leaves out: dust shows the largest entries of the whole tree whatever their
parents, and pdu hides entries below 1 % of the total; swath folds each parent's smaller children into a
counted line.

## Totals per prefix

With `-d N`, `swath du` prints the total of each prefix down to N levels below SOURCE, a parent including
its children:

```text
$ swath du -d 1 /data/photos --format table
    size  files  dirs  newest (CET)         path
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
time. `max_last_modified` is the newest time among the objects counted; the table shows it under `newest`, as
the view does, in local time to the second with the zone named in the heading, as `swath list`'s table does, and
`-` when no object had one.

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
the rest, names the entries on stderr (relative to SOURCE) and exits 3 ([exit codes](reference/exit-codes.md)).
In the table, a directory that could not be read shows `-` for its size and counts, and in TSV and JSONL null
for its counts and both figures, where an empty directory shows 0. The view marks with `!` after
the size every line whose sums leave out such a directory, at or below it (a `(N more)` line too when one of the
children it holds does), and says so in one line under it instead of naming them:
`! 5 directories could not be read (marked ! above); -v lists them`.

## Totals from a saved dataset

Each `swath du SOURCE` lists SOURCE again. To look at a large source at several depths, or with other filters,
list it once into a dataset (`swath list SOURCE -o DIR`) and total that instead (a live `swath du` that listed
a million entries or more says so on a terminal: its closing lines end with `hint:` lines giving both commands
for its SOURCE). `--from-dataset` reads a local dataset directory only: a dataset in a bucket is refused
(exit 2), so copy its `manifest.json` and the parts the manifest names, at the same paths, to a local
directory and total that.

```text
$ swath du --from-dataset photos-inventory -d 1 --sort size
swath: totals of the dataset photos-inventory, not a live listing: /data/photos as listed 2026-11-12 11:00:03 CET (3 hours ago); complete
    size  files  dirs  newest (CET)         path
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
and its coverage verdict (`complete`, or `partial:` and what is missing: `2 unreadable entries`, or `--max-keys
reached` for a diagnostic `swath diag list` sample), then the listing's filters and exclusions. `-q` silences that line. In JSONL each row also carries a `dataset` member with
the same facts, its `filters` as the manifest's `coverage.filters` records them ([Output](output.md#du-rows)). A dataset that left out entries it could not read (`coverage.unreadable`), or is
otherwise partial, prints its totals and
exits 3 (`incomplete`), as the run that wrote it did, its last line saying why. The dataset counts the
unreadable entries but does not record which directories they were, so it cannot mark their rows: such a
directory's row shows 0 (in every format) where the live `swath du` shows `-` or null. A file with several hard links
counts once where the dataset records links (a local tree's, listed with stat), as a live `swath du` counts it;
`-l` counts every name. A dataset listed with `--metadata names` holds no lengths, blocks or links, so its rows
are those of a live `--metadata names` run: both figures null under a prefix with files counted, and
`links_once` false. With nothing counted (an empty tree, or every file filtered out) both figures are 0, and
`links_once` is still false, as live, since the manifest records the tier.

`--from-dataset` reads a committed local dataset directory. A directory holding an interrupted run is refused with the `swath show` and `swath resume` commands that
handle it. The options that shape a listing are refused by name: provider, credential, performance, limit and
report options, and `--all-versions`, since the dataset's schema decides that. So are `--allocated`,
`--count-links` and `--storage-class` when the dataset's schema has no column for them. The filters, the
totals options, `--format`, `--no-header` and the terminal options `-v`, `-q`, `--color`, `--log-format`, `--no-progress`
and `--no-stats` apply. A duration in `--modified-since` counts back from now, not from the listing.
