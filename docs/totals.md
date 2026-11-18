# Total a source

`swath du` answers where the bytes are in a directory or a bucket: how much each part of it holds, in how many
files or objects, and how recently it changed. Give it a source:

```bash
swath du media
```

On a terminal it draws [the view](#the-view), the largest prefixes and objects at any depth. With `-d N` it
prints the totals of every prefix down to N levels, as a table or as rows for scripts
([Totals per prefix](#totals-per-prefix)). To total the same source several ways without listing it each
time, save it once and total the saved dataset ([Totals from a saved dataset](#totals-from-a-saved-dataset)).
Every option is in [swath du](commands/swath-du.md).

## The view

Bare `swath du SOURCE` on a terminal shows a tree of the largest prefixes and objects, largest first, each under
its parent. This one is a public NOAA prefix, on a terminal 100 columns wide and 24 lines high:

```text
$ TZ=UTC swath du s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1
    size  objects  share                        newest          path
 383 GiB    5.25k   100%  ████████████████████  2023-01-14 UTC  s3://noaa-gestofs…s_2d_glo.20230113/
33.5 GiB       11   8.8%  █▊                    2023-01-14        18/rerun/
12.4 GiB        1   3.2%  ▋                     2023-01-14          stofs_2d_glo_fcst.64.nc
21.1 GiB       10   5.5%  █                     2023-01-14          (10 more)
33.5 GiB       11   8.8%  █▊                    2023-01-13        12/rerun/
12.4 GiB        1   3.2%  ▋                     2023-01-13          stofs_2d_glo_fcst.64.nc
21.1 GiB       10   5.5%  █                     2023-01-13          (10 more)
33.5 GiB       11   8.7%  █▋                    2023-01-13        06/rerun/
12.4 GiB        1   3.2%  ▋                     2023-01-13          stofs_2d_glo_fcst.64.nc
21.0 GiB       10   5.5%  █                     2023-01-13          (10 more)
33.4 GiB       11   8.7%  █▋                    2023-01-13        00/rerun/
12.4 GiB        1   3.2%  ▋                     2023-01-13          stofs_2d_glo_fcst.64.nc
21.0 GiB       10   5.5%  █                     2023-01-13          (10 more)
23.8 GiB        1   6.2%  █▏                    2023-01-13        stofs_2d_glo.t12…fields.cwl.vel.nc
23.8 GiB        1   6.2%  █▏                    2023-01-14        stofs_2d_glo.t18…fields.cwl.vel.nc
23.8 GiB        1   6.2%  █▏                    2023-01-13        stofs_2d_glo.t06…fields.cwl.vel.nc
23.8 GiB        1   6.2%  █▏                    2023-01-13        stofs_2d_glo.t00…fields.cwl.vel.nc
11.2 GiB        1   2.9%  ▌                     2023-01-13        stofs_2d_glo.t12z.fields.cwl.nc
 142 GiB    5.20k  37.2%  ███████▍              2023-01-14        (5,203 more)
  383 GiB in 5,252 objects
```

Each line is a prefix or an object, with:

- **size**: the bytes under it ([How sizes are counted](#how-sizes-are-counted));
- **objects** (`files` for a directory): how many are under it, to three significant digits (`5.25k`);
- **share** and the bar: its part of the total;
- **newest**: the date of the newest modification under it, in local time, the zone named on the first line
  (`TZ` sets it);
- **path**: SOURCE on the first line, and below it each prefix or object indented two blanks under its parent.

A prefix with a single child shares its line with it (`18/rerun/`; in a directory, `videos/trip.mp4`). Under
each parent the view lists its largest children, then one `(N more)` line for the rest: how many they are, what
they hold together and their newest date, so the bytes of the lines under a parent add up to its bytes: on disk,
less the directory's own blocks when every child has a line (a `(N more)` line holds them otherwise). A shared
line gives the first prefix's bytes, its own blocks included.
Only a `(N more)` line of 0 bytes is left out (unless it holds a directory that could not be read), so the
counts shown under a parent need not add up: empty files and directories may not appear. A `!` after the size marks a line whose sums leave out a directory that could not be
read ([Entries that could not be read](#entries-that-could-not-be-read)).

The view fills the terminal's height from the largest down, opening the largest prefixes first, and keeps its
heading and the line of totals on screen. An entry smaller than 0.5 % of the total (more on a short terminal)
gets no line of its own: a long tail of small prefixes stays in its `(N more)` line, and the view can end before
the terminal is full. On a narrow terminal the bar shrinks, then the share and the date go; a long path is cut in
the middle, keeping its end, and a line never wraps. Every figure is exact, whatever the size of the source.

The line under the view gives the total once, exactly, with the other figure where the source has one
(`36.5 MiB on disk in 6 files · 100 MiB of data`); `-q` leaves it out. Color follows `--color` and `NO_COLOR`.

The view is for reading. In a pipe, bare `swath du` prints the total row only, as TSV, and `-d N` (`-d 0`
included), `--sort` or `--format` prints the table or the rows of [Totals per prefix](#totals-per-prefix)
instead. `--from-dataset` draws the same view from a saved dataset.

## Totals per prefix

With `-d N`, `swath du` prints the total of each prefix down to N levels below SOURCE, a parent including its
children. On the `media` directory of [Your first inventory](first-inventory.md):

```text
$ swath du -d 1 media
 on disk  files  dirs  newest (CET)         path
 788 KiB      2     0  2026-11-02 10:30:00  archive/
6.12 MiB      3     2  2026-11-02 10:30:00  photos/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
36.5 MiB      6     5  2026-11-02 10:30:00  media (total)
  36.5 MiB on disk in 6 files · 100 MiB of data
```

`-d` limits the rows, not the listing: every row and the total include everything below them. Add up rows of
one depth only, never across depths; even then they leave out the objects directly under SOURCE. `--sort size`
puts the largest first under each parent, and the total stays last. A directory's rows count its
subdirectories in `dirs`, and the table indents each level two blanks under the one above. The table rounds
sizes to three significant digits in binary units and groups the counts' digits (`5,252`); a bucket's table
heads its columns `objects` and `prefix`, a directory's `files` and `path`.

A prefix ends at a `/`. Depth counts from SOURCE's prefix up to its last `/`: `s3://b/logs/2026/` counts from
`logs/2026/`, `s3://b/logs` from the top of the bucket. A key with fewer than N slashes counts only in the
prefixes it has. The table's total row names SOURCE as typed (`media (total)`). `newest` is the newest
modification among the objects counted, to the second, `-` when none had a time. With `--all-versions` every
version is an object, and an S3 delete marker adds its prefixes but no object, bytes or time.

On a terminal the rows are a table with the total last. In a pipe they are TSV with the total first
(`--no-header` leaves out the header line), and `--format jsonl` gives JSONL. A script selects rows by their
`row_type`:

```text
$ swath du -d 1 s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 --format jsonl \
    | jq -r 'select(.row_type == "prefix") | [.size, .prefix] | @tsv'
35893517301	stofs_2d_glo.20230113/00/
35926372104	stofs_2d_glo.20230113/06/
35963721793	stofs_2d_glo.20230113/12/
36012993488	stofs_2d_glo.20230113/18/
```

The rows' fields, their types and when they are null are in [Output](output.md#du-rows).

The [filters](filters.md) apply before counting, to every row: in a directory to its subdirectories too, so
`--min-size 1M` keeps the large files and drops the directories, whose own size is small.

The totals take memory for each distinct prefix, never for each object: about 150 bytes a prefix. A run that
would hold more than `--max-prefixes` (1,000,000 by default) fails rather than print partial totals; raise it
for a deep `-d` over a wide tree. The view does not fail there: it keeps fewer levels and says so on stderr
(`levels below depth N are left out: more prefixes than --max-prefixes 1,000,000`).

## How sizes are counted

### Apparent and allocated bytes

Every row has two byte figures. `allocated_bytes` is the space the filesystem reports for each entry, its blocks
× 512 (`st_blocks`), shown as `on disk`. `size` is the apparent size: the length of each file as `ls -l` shows
it, or an object's length as the listing reports it. The table and the view show one of them, and `--sort size`
orders by it; for a directory the line under them gives the total of both, so a sparse file or a tree of small
files shows at a glance.

A local tree shows the space on disk, as `du`, ncdu and dust do, where its listing reads blocks; `-A`
(`--apparent-size`) shows the lengths instead, as `du -A` does:

```text
$ swath du -d 1 media -A
    size  files  dirs  newest (CET)         path
64.8 MiB      2     0  2026-11-02 10:30:00  archive/
6.10 MiB      3     2  2026-11-02 10:30:00  photos/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
 100 MiB      6     5  2026-11-02 10:30:00  media (total)
  100 MiB in 6 files · 36.5 MiB on disk
```

Here `archive/` holds a sparse 64 MiB disk image that takes no blocks: 64.8 MiB long, 788 KiB on disk. Where the
listing reads no blocks, the lengths lead: on Windows, at `--metadata names` (which reads no lengths either),
and for an object store, which has no allocation. `--metadata statx` reads blocks, but a filesystem may answer
it without them for some entries (some network filesystems): those count as missing, and the figure is a lower
bound (`>= `). `--allocated` requires the space on disk: an entry without it fails the count, and
`--metadata names` refuses it. The run report's `totals.primary` says which figure led,
and `primary_reason` why ([Run report](reference/run-report.md)).

The space on disk is what the filesystem says, what `du` prints, but not the space a deletion would free:

- A directory's own blocks count in its row and its parents', as `du` counts them, and its size in none.
  The source directory's own blocks are in no total, so a tree's figure is below `du -sB1`'s by that one
  directory. Directories are counted in `dirs`, never as files.
- A symbolic link counts as a file of its own length; swath does not follow it.
- Sparse files, compression, data stored inline in the metadata and cloud placeholder files can report less
  allocation than their length, down to zero; block rounding and preallocation can report more.
- Shared extents (deduplicated files, APFS clones, Btrfs and XFS reflinks) are counted for each file that
  shares them, so a total can exceed the filesystem's capacity.
- Network filesystems may report a synthetic allocation.
- An unknown figure is not zero. Windows and `--metadata names` read no blocks: the figure is null (`-` in the
  table, `on disk unknown` under it), and `--metadata names` reads no lengths either. A figure that only some
  files had is a lower bound, shown as `>= 12.0 KiB`.

An object store has no allocated bytes: its rows carry them as null, and its table has no line under it. Its
`size` is not the stored or billed size either: storage classes, minimum storage durations, versions and
replication are outside it.

### Hard links

A file with several hard links has several names; `swath list` writes each name as a row. `swath du` counts the
file once, as `du` does, under the first of its names in byte order, so the totals do not depend on which name
the walk reached first. In `media`, `notes.tar` and `notes-latest.tar` are one file of 800,000 bytes.
`-l` (`--count-links`) counts every name, as `du -l`:

```text
$ swath du -d 1 media -l
 on disk  files  dirs  newest (CET)         path
1.54 MiB      3     0  2026-11-02 10:30:00  archive/
6.12 MiB      3     2  2026-11-02 10:30:00  photos/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
37.2 MiB      7     5  2026-11-02 10:30:00  media (total)
  37.2 MiB on disk in 7 files · 101 MiB of data
```

`--metadata names`, and Windows, read no link counts, so every name counts there. Each row's `links_once` says
which way it counted.

To count a file once, swath remembers each file with more than one link, about 100 bytes each, up to
`--max-inodes` (1,000,000 by default). Past it the run fails rather than count some names twice: exit 1, no
totals, and its last line says what to do:

```text
swath: the tree has more than 1,000,000 files with several hard links, which swath du counts once each: run it again with -l to count every name, or with --max-inodes N to remember more of them (about 100 MB a million)
```

### Entries that could not be read

What swath cannot read, such as the contents of a directory without read permission, adds nothing to the
totals: no files, no bytes, no subdirectories. The directory itself was seen in its parent, so it still counts
in its parent's and the total's `dirs`. swath prints the totals of the rest, names the directory on stderr
(relative to SOURCE) and exits 3 ([exit codes](reference/exit-codes.md)). In the table, a directory it could not
read shows `-` for its size and counts, where an empty directory shows 0; in TSV and JSONL its counts and both
figures are null.

Here `media` has gained a directory `private/` that the user running swath cannot read (`chmod 000`); the total
counts it among 6 directories, and its files are missing:

```text
$ swath du -d 1 media
 on disk  files  dirs  newest (CET)         path
 788 KiB      2     0  2026-11-02 10:30:00  archive/
6.12 MiB      3     2  2026-11-02 10:30:00  photos/
       -      -     -  -                    private/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
36.5 MiB      6     6  2026-11-02 10:30:00  media (total)
  36.5 MiB on disk in 6 files · 100 MiB of data
swath: warning: unreadable entries:
  private: permission denied
  partial: 1 unreadable entry; 6 files, 6 directories, 36.5 MiB on disk in 0.0s
```

The view marks with `!` after the size every line whose sums leave out such a directory, at or below it, and
says so in one line under it instead of naming them; `-v` names them:

```text
36.5 MiB !      6   100%  ████████████████████  2026-11-02 CET  media
...
4.00 KiB !      0   0.0%                                          (1 more)
  36.5 MiB on disk in 6 files · 100 MiB of data
! 1 directory could not be read (marked ! above); -v lists them
  partial: 1 unreadable entry; 6 files, 6 directories, 36.5 MiB on disk in 0.0s
```

Under `-q` the last line says it instead: `swath: 1 entry could not be read and is missing from the output`.
A symbolic link whose target cannot be read, and a file removed during the walk, do not make the totals
incomplete ([Troubleshooting](troubleshooting.md#entries-could-not-be-read)).

## Totals from a saved dataset

Each `swath du SOURCE` lists SOURCE again. To total a large source at several depths, or with other filters,
list it once into a dataset and total that instead. [Your first inventory](first-inventory.md) saved `media`
as `media-inventory`; without one, save it first with `swath list media -o media-inventory` (an existing
dataset directory is refused unless you add `--overwrite`). Then:

```bash
swath du --from-dataset media-inventory -d 1 --sort size
```

`--from-dataset` reads a dataset in a local directory only. A dataset in a bucket is refused (exit 2): copy its
`manifest.json` and the parts the manifest names, at the same paths, to a local directory and total that, or
query it in place with DuckDB ([Query an inventory](query.md)).

```text
$ swath du --from-dataset media-inventory -d 1 --sort size
swath: totals of the dataset media-inventory, not a live listing: /home/me/media as listed 2026-11-17 12:59:25 CET (3 hours ago); complete
 on disk  files  dirs  newest (CET)         path
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
6.12 MiB      3     2  2026-11-02 10:30:00  photos/
 788 KiB      2     0  2026-11-02 10:30:00  archive/
36.5 MiB      6     5  2026-11-02 10:30:00  media-inventory (total)
  36.5 MiB on disk in 6 files · 100 MiB of data
```

The rows, the view and every format are the ones `swath du` of the dataset's source printed when it was
listed, with the same depth, order and filters; hard links count once where the dataset records them, and `-l`
counts every name. The space on disk leads where the dataset holds it; a dataset listed where blocks are not
read (Windows, `--metadata names`) leads with the lengths, as its live run did. Totals from a dataset listed on
Windows with no row left after the filters say `allocated_bytes` where the live run says `size`: nothing is
left to tell them apart, and every figure is 0. A live `swath du` that listed a million entries or more ends on a terminal with `hint:` lines
giving both commands for its SOURCE. `swath du DIR` without `--from-dataset` totals the files in DIR, and when
DIR holds a dataset a `hint:` line gives the `--from-dataset` command.

The line on stderr says the totals are saved rows: the dataset, its source, when it was listed and whether the
listing was complete (`complete`, or `partial:` and what is missing), then its filters and exclusions. `-q`
silences it, and in JSONL each row carries the same facts in a `dataset` member ([Output](output.md#du-rows)).
A partial dataset prints its totals and exits 3, its last line saying why. A dataset that missed entries it
could not read counts them but does not record which directories they were, so it cannot mark them: such a
directory's row shows 0 where the live `swath du` shows `-` or null. A dataset listed with `--metadata names`
holds no lengths, blocks or links, so its rows are those of a live `--metadata names` run.

A SOURCE after the directory restricts the totals to part of the dataset: the dataset's source as it was
listed, or a path or URL below it (`swath du --from-dataset media-inventory media/photos -d 1`). It prints what a
live `swath du` of that SOURCE prints, labels and depth included, without reading the rest of the dataset. A
local path is resolved as a live `swath du` resolves it; one that names a file totals that file alone, and a
symbolic link is refused (exit 2), since a live `swath du` would follow it. A URL names the same bucket or
container and a prefix below the listed one. A SOURCE below the listed one that holds nothing prints zeros and
warns (exit 0); any other SOURCE is refused (exit 2), naming the dataset's source:
`"photos": media-inventory was listed from /home/me/media; give a path below it, or no argument`.

A directory holding an interrupted run is refused with the `swath show` and `swath resume` commands that handle
it, and a dataset whose parts do not match its schema is refused with exit 1.

The filters, the totals options, `--format`, `--no-header` and the terminal options (`-v`, `-q`, `--color`,
`--log-format`, `--no-progress`, `--no-stats`) apply. The options that shape a listing are refused by name,
since the dataset is already listed: provider, credential, performance, limit and report options and
`--all-versions`, and `--allocated`, `--count-links` or `--storage-class` when the dataset has no column for
them. A duration in `--modified-since` counts back from now, not from the listing.
