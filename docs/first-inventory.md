# Your first inventory

This walk-through uses a local directory, so it needs no cloud account and no credentials, and nothing leaves
your machine. Replace `media` with a directory of yours. The output below comes from a small example tree of
seven files. [Install](install.md) swath first.

## 1. Find where the space went

```bash
swath du -d 1 media
```

```text
    size  files  dirs  last_modified (local)  path
65.5 MiB      3     0  2026-11-02 09:30:00    archive/
6.10 MiB      3     2  2026-01-20 12:00:00    photos/
29.6 MiB      1     0  2026-11-02 09:30:00    videos/
 101 MiB      7     5  2026-11-02 09:30:00    (total)
```

Each row totals everything below it, as `du` does: its files, its directories, their bytes and the newest
modification time. `-d 1` limits the breakdown to one level. It does not limit the walk: swath still reads the
whole tree, and every row and the total include everything below them. Without `-d` you get the total alone.
`--sort size` puts the largest first.

The sizes are apparent bytes, the length of each file as `ls -l` shows it, not the disk space it takes. Here
`archive/` holds a sparse 64 MiB disk image that takes almost no disk space, and two names for one hard-linked
file, each counted. `--allocated` totals allocated bytes and `--count-links-once` counts a hard-linked file
once. [Understand your results](results.md) explains both.

## 2. List every entry

```bash
swath list media --columns key,size,kind,last_modified | head -5
```

```text
key	size	kind	last_modified
archive/	4096	DIR	2026-11-02T09:30:00.000000Z
archive/disk.img	67108864	FILE	2026-11-02T09:30:00.000000Z
archive/notes-latest.tar	800000	FILE	2026-11-02T09:30:00.000000Z
archive/notes.tar	800000	FILE	2026-11-02T09:30:00.000000Z
```

There is one row per entry, files and directories both, in ascending byte order of the key. A key is the path
below the directory you named, and a directory's key ends in `/`. In a pipe the rows are TSV with a header
line. On a terminal they are a table, `--format jsonl` gives JSON lines, and `-o rows.tsv.zst` writes a
compressed file. Without `--columns` every metadata column is there ([text formats](output.md)). Closing the
pipe early, as `head` does, ends the run with exit code 0.

## 3. Save an inventory

```bash
swath list media -o media-inventory
```

```text
  12 entries in 0.0s · 2,400 entries/s
  1 part · 4.03 KiB written · peak RSS 45.4 MiB
```

`media-inventory` is now a swath dataset. A swath dataset is a directory of sorted Parquet parts and a
`manifest.json` that commits them, in the SeekParquet format, with one of swath's registered schema ids and
valid swath metadata ([the swath dataset profile](dataset.md)). Here it holds `data/part-00000.parquet`, the
manifest and `run.json`, the run report. Its 12 rows are the 7 files and 5 directories.

The output directory must be new or empty. Keep it outside the tree you list; if it is inside, swath skips it
and records it as an exclusion.

## 4. Look at what you saved

```bash
swath show media-inventory
```

```text
dataset media-inventory: SeekParquet format 0 draft 1, swath schema swath/local-entries/v1, swath metadata version 1
key key unique=true, published 2026-11-13T20:16:47Z
source /home/you/media, scope whole ""
coverage complete=true stop=completed skipped=0 filters=0 exclusions=0
1 part, 12 rows, 4125 bytes, keys "archive/" .. "videos/trip.mp4"
```

`swath show` reads the manifest, not the rows. The lines say which schema the rows follow, when the dataset
was published, what was listed, how completely, and what the parts hold. Here `complete=true` with
`skipped=0` and no filters means the dataset covers the whole tree. Read the coverage line before you rely
on an inventory: [Understand your results](results.md#coverage-and-unreadable-entries) explains each field.

To read the rows themselves, open the parts the manifest lists with DuckDB, Python or another Parquet
reader ([reading a dataset](running.md#reading-the-output)).

## Next

- [Understand your results](results.md): scope, sizes, hard links, coverage and time.
- [Sources](sources.md): list an S3, GCS or Azure Blob bucket the same way.
- [Run large listings](running.md): size, watch, stop and resume a long run.
