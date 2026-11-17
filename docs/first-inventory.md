# Your first inventory

This walk-through uses a local directory, so it needs no cloud account and no credentials, and nothing leaves
your machine. Replace `media` with a directory of yours. The output below comes from a small example tree of
five directories and six files, one of them a hard-linked file with two names. [Install](install.md) swath
first.

## 1. Find where the space went

```bash
swath du media
```

```text
    size  files  share                        newest          path
 100 MiB      6   100%  ████████████████████  2026-11-02 CET  media
64.8 MiB      2  64.5%  ████████████▉         2026-11-02        archive/
64.0 MiB      1  63.7%  ████████████▋         2026-11-02          disk.img
 781 KiB      1   0.8%  ▏                     2026-11-02          (2 more)
29.6 MiB      1  29.4%  █████▉                2026-11-02        videos/trip.mp4
6.10 MiB      3   6.1%  █▏                    2026-11-02        photos/
4.77 MiB      2   4.7%  ▉                     2026-11-02          2025/
2.86 MiB      1   2.8%  ▌                     2026-11-02            beach.jpg
1.91 MiB      1   1.9%  ▍                     2026-11-02            dunes.jpg
1.34 MiB      1   1.3%  ▎                     2026-11-02          2026/snow.jpg
  100 MiB in 6 files · 36.4 MiB on disk
```

On a terminal, bare `swath du` shows where the bytes are: the largest directories and files at any depth,
largest first, each under its parent with its share of the total and the date of the newest file below it. A
`(N more)` line counts the rest of a directory's entries, so the lines under a directory add up to it
([the view](totals.md#the-view)). In a pipe it prints the total row only. This output comes from a terminal 100
columns wide and 30 lines high.

For the totals of every directory down to a fixed depth, a table on a terminal and TSV in a pipe for
scripts, give `-d N`:

```bash
swath du -d 1 media
```

```text
    size  files  dirs  newest (CET)         path
64.8 MiB      2     0  2026-11-02 10:30:00  archive/
6.10 MiB      3     2  2026-11-02 10:30:00  photos/
29.6 MiB      1     0  2026-11-02 10:30:00  videos/
 100 MiB      6     5  2026-11-02 10:30:00  media (total)
  100 MiB in 6 files · 36.4 MiB on disk
```

Each row totals everything below it, as `du` does: its files, its directories and their bytes. `-d 1` limits
the breakdown to one level. It does not limit the walk: swath still reads the whole tree, and every row and
the total include everything below them. `--sort size` puts the largest first.

The sizes are apparent bytes, the length of each file as `ls -l` shows it; the last line adds the space the
filesystem reports for the whole tree, on disk, close to what `du` prints. Here `archive/` holds a sparse
64 MiB disk image that takes almost no disk space, and two names for one hard-linked file, counted once, as
`du` counts it. `--allocated` shows the on-disk figure instead and sorts by it, and `-l` (`--count-links`)
counts every name.
[Understand your results](results.md) explains both. In a pipe the same totals are TSV, with each row's
newest modification time ([Output](output.md#du-rows)).

## 2. List every entry

```bash
swath list media --columns key,size,kind,last_modified | head -5
```

```text
key	size	kind	last_modified
archive/	4096	dir	2026-11-02T09:30:00.000000Z
archive/disk.img	67108864	file	2026-11-02T09:30:00.000000Z
archive/notes-latest.tar	800000	file	2026-11-02T09:30:00.000000Z
archive/notes.tar	800000	file	2026-11-02T09:30:00.000000Z
```

There is one row per entry, files and directories both, in ascending byte order of the key. A key is the path
below the directory you named, and a directory's key ends in `/`. In a pipe the rows are TSV with a header
line. On a terminal they are a table, `--format jsonl` gives JSONL, and `-o rows.tsv.zst` writes a
compressed file. Without `--columns` every metadata column is there ([output](output.md)). Closing the
pipe early, as `head` does, ends the run with exit code 0.

## 3. Save an inventory

```bash
swath list media -o media-inventory
```

```text
  complete: 12 entries, 101 MiB in 0.0s → media-inventory (1 part, 4.05 KiB)
  next: swath show media-inventory · swath du --from-dataset media-inventory
```

The closing lines give the verdict (`complete`), what was listed and its bytes (the hard-linked file counts
under both its names here), the time, and where the dataset went, then what to run next.

`media-inventory` is now a swath dataset. A swath dataset is a directory of sorted Parquet parts and a
`manifest.json` that commits them, in the SeekParquet format, with one of swath's registered schema ids and
valid swath metadata ([the swath dataset profile](dataset.md)). Here it holds `data/part-00000.parquet`, the
manifest, `run.json` (the run report) and
`.swath/` (the run's private workspace, which readers ignore)
([layout](dataset.md#what-a-swath-dataset-is)). Its 12 rows are 7 file names and 5 directories;
two of the names are one hard-linked file.

A complete run exits 0, as this one did. If swath cannot read some entries (a directory without permission),
it still writes the dataset, names them on stderr, ends with the closing line `partial: N unreadable entries; …`
and exits 3 ([exit codes](reference/exit-codes.md)); step 4 shows how the dataset records it.

The output directory must be new or empty. Keep it outside the tree you list; if it is inside, swath skips it
and records it as an exclusion.

## 4. Look at what you saved

```bash
swath show media-inventory
```

```text
media-inventory: complete
  source    /home/you/media (whole tree)
  listed    2026-11-15 21:03:25 to 21:03:25 CET (0.0s)
  contents  12 entries, 101 MiB; keys "archive/" .. "videos/trip.mp4"
  coverage  nothing unreadable; no filters; no exclusions
  storage   1 part, 4.05 KiB of Parquet (schema swath/local-entries/v1)
```

`swath show` reads the manifest, not the rows. The first line is the verdict; then what was listed, when, what
the dataset holds, how completely, and last how it is stored. Here `complete`, with nothing unreadable and no
filters or exclusions, means the dataset covers the whole tree. Read the verdict and the coverage before you
rely on an inventory: [Understand your results](results.md#coverage-and-unreadable-entries) explains them.

To read the rows themselves, open the parts with DuckDB, Python or another Parquet
reader ([Query an inventory](query.md)).

## 5. Total the saved inventory

The saved inventory answers step 1 again without walking the tree:

```bash
swath du --from-dataset media-inventory
```

It prints the totals of step 1 as they were when the tree was listed, and says on stderr when that was and
whether the listing was complete ([swath du](totals.md#totals-from-a-saved-dataset)).

## Next

- [Understand your results](results.md): scope, sizes, hard links, coverage and time.
- [Sources](sources/README.md): list an S3, GCS or Azure Blob bucket the same way.
- [Query an inventory](query.md): read the saved inventory with DuckDB, pandas or Spark.
- [Run large listings](running.md): list a bucket of your own start to finish.
