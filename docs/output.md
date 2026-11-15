# Output

swath writes what it lists as text rows (a table, TSV or JSONL) on stdout or in a file, as a sorted Parquet
dataset on disk or in a bucket, or as `swath du` totals. This page shows each and when to choose it.

```bash
swath list s3://bucket/logs/ | head          # TSV in a pipe, a table on a terminal
swath list s3://bucket/logs/ --format jsonl  # JSONL on stdout
swath list s3://bucket/logs/ -o rows.tsv.zst # a compressed TSV file
swath list s3://bucket/logs/ -o logs-inventory # a sorted Parquet dataset
swath list s3://bucket/logs/ -o gs://out/logs/2026-11-11 --work-dir work  # the dataset in a GCS bucket
swath list s3://bucket/logs/ -o s3://out/logs/2026-11-11 --work-dir work  # the dataset in an S3 bucket
swath list s3://bucket/logs/ -o az://acct/out/logs/2026-11-11 --work-dir work  # in an Azure Blob container
swath du s3://bucket -d 1                    # totals per prefix
```

## Choosing an output

| Output | Choose it when |
|---|---|
| text rows on stdout | you read the rows, or pipe them to `head`, `grep`, `awk`, `jq` |
| a `.tsv` or `.jsonl` file | another program reads the rows once, or you keep a small listing |
| a dataset (`-o DIR`) | you query the listing with SQL or DataFrames, keep it, compare it with a later one, or list a large source: only a dataset run can be resumed, and it says whether it is complete |
| a dataset in a bucket (`-o gs://BUCKET/PREFIX`, `-o s3://BUCKET/PREFIX` or `-o az://ACCOUNT/CONTAINER/PREFIX`, with `--work-dir DIR`) | the same, written straight to GCS, S3 or Azure Blob Storage: the runner keeps only spill and the run's metadata ([A dataset in a bucket](dataset.md#a-dataset-in-a-bucket), [Running swath in production](production.md#writing-to-a-bucket)) |
| `swath du` | you want sizes and counts, not rows |

A [swath dataset](dataset.md) is a directory of sorted Parquet parts and a `manifest.json` that commits them.
A bare local destination or bucket prefix whose final path element ends in `.parquet` (case-insensitive) is
refused. Use a name without that suffix, or add a trailing separator to explicitly select a directory,
such as `-o inventory.parquet/` or `-o s3://out/inventory.parquet/` (a Spark-style directory).
The same name rule applies to `swath resume DIR`.
Querying one is shown in [Running swath](running.md#reading-the-output).

A listing feeds analysis, migration planning or a comparison with an earlier one; swath itself does not copy,
compare or schedule. A dataset is not a direct `rclone --files-from` input: a handoff must generate and test a
compatible path list. Names, sizes and times alone do not validate a backup.

## Text rows

The `-o` name chooses the format and the compression: `NAME.tsv` or `NAME.jsonl`, optionally followed by
`.gz` or `.zst`. Without `-o`, or with `-o -`, the rows go to stdout as `--format` says: `auto` (the default)
is the table on a terminal and TSV otherwise, or `table`, `tsv`, `jsonl`. Compress stdout with a pipe
(`| zstd`). The file's directory must exist; the file is written beside its name and renamed into place when
complete, replacing a previous file. A file written inside the local tree it lists is left out of the listing,
with everything the run writes beside it: the file itself (a previous run's, on a repeat), its temporary
`.NAME.*.tmp` and the attempt's scratch directory ([sources](sources/local.md)).

TSV and JSONL rows have the dataset's columns under the dataset's names, by default `key`, then `size`,
`last_modified` and the rest of the source's schema, in the schema's order
([The swath dataset](dataset.md#schema-registry)). Columns after `last_modified` differ between sources: read them by
name, not by position. `--columns size,key` prints only those columns, in that order. The table shows a few
columns for people, with `key` last.

Values are as the dataset stores them: S3's `etag` without its quotes, integers exact, booleans `true` and
`false`, every time in UTC with six fraction digits (`2026-01-01T00:00:00.123000Z`). A null is neither an
empty string nor zero.

### TSV

One header line of the column names (`--no-header` leaves it out), then one line per row, the fields separated
by tabs:

```text
key	size	last_modified	etag	storage_class	owner_id	owner_display_name	checksum_algorithm	checksum_type
top.txt	8	2026-01-01T00:00:00.000000Z	dddddddddddddddddddddddddddddddd	STANDARD	\N	\N	\N	\N
weird/new\nline.txt	6	2026-01-01T00:00:00.000000Z	bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb	STANDARD	\N	\N	\N	\N
weird/sp ace é.txt	7	2026-01-01T00:00:00.123000Z	cccccccccccccccccccccccccccccccc	STANDARD	\N	\N	\N	\N
weird/tab\there.txt	5	2026-01-01T00:00:00.000000Z	aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa	STANDARD	\N	\N	\N	\N
```

A field that is exactly `\N` is null; an empty field is the empty string. Within a field:

| Written | Stands for |
|---|---|
| `\\` | a backslash |
| `\t` | a tab |
| `\n` | a line feed |
| `\r` | a carriage return |
| `\xHH` (two lowercase hex digits) | the byte HH: every other byte below 0x20, DEL (0x7f), and every byte that is not part of valid UTF-8 |

Every other byte stands for itself. So any key round-trips, a row is always one line, and a terminal is never
sent a control byte. Split at tabs (`awk -F'\t'`, `cut`), never at runs of blanks.

### JSONL

One JSON object per line, its members in column order, a null `null`, strings escaped as `jq` writes them:

```text
{"key":"weird/new\nline.txt","size":6,"last_modified":"2026-01-01T00:00:00.000000Z","etag":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","storage_class":"STANDARD","owner_id":null,"owner_display_name":null,"checksum_algorithm":null,"checksum_type":null}
```

A string that is not valid UTF-8 (a local file name's raw bytes) cannot be a JSON string: its member is then
`NAME_base64`, the exact bytes in standard base64, in place of `NAME` (`key_base64`, `link_target_base64`).
No column name ends in `_base64`.

### The table

For reading on a terminal: the time to the second in local time (`TZ` sets the zone), its heading naming the
zone by its abbreviation (`last_modified (CET)`), the size in binary units, the local tree's `kind`, the version
columns with `--all-versions`, and the key last, escaped as in TSV. A missing value is `-`. TSV, JSONL and the
dataset keep times in UTC.

```text
$ TZ=UTC swath list /data/photos
last_modified (UTC)      size  kind          key
2026-11-02 09:30:00  4.00 KiB  DIR           2025/
2026-03-01 10:00:00   293 KiB  FILE          2025/a.jpg
2026-11-02 09:30:00  4.00 KiB  DIR           2025/raw/
2026-03-01 10:00:00   117 KiB  FILE          2025/raw/b.cr2
2026-11-02 09:30:00  4.00 KiB  DIR           2026/
2026-11-02 09:30:00  48.8 KiB  FILE          2026/c.jpg
2026-11-02 09:30:00       3 B  FILE          notes.txt
```

`--columns` and `--no-header` do not apply to the table; with either on a terminal, `auto` prints TSV.

### Reading text rows in DuckDB

JSONL needs no options; a column that is null in every row reads as `JSON`, so name its type if you filter on
it:

```sql
SELECT key, size, last_modified FROM read_json('rows.jsonl');
```

`read_csv` with its defaults reads `\N` as the text `\N` and leaves the escapes in place. Give it the
separator, no quoting and the null string, and undo the escapes with a macro:

```sql
CREATE MACRO swath_unescape(s) AS
  array_to_string(list_transform(string_split(s, '\\'),
    lambda p: replace(replace(replace(p, '\t', chr(9)), '\n', chr(10)), '\r', chr(13))), '\');

SELECT swath_unescape(key) AS key, size, last_modified
FROM read_csv('rows.tsv', delim = '\t', quote = '', escape = '', nullstr = '\N', header = true);
```

The macro undoes `\\`, `\t`, `\n` and `\r`. A `\xHH` byte stays written as it is: DuckDB's text cannot hold a
byte that is not valid UTF-8. `contains(replace(key, '\\', ''), '\x')` finds those keys; read them from JSONL
(`key_base64`) or a dataset instead, which carry the exact bytes.

## du rows

`swath du` prints, for the total and each prefix down to `-d N`, the objects under it, two byte figures and the
newest modification time among them (a local tree adds its directories):

```text
$ swath du -d 1 /data/photos --format tsv
row_type	depth	objects	dirs	size	size_missing	allocated_bytes	allocated_bytes_missing	max_last_modified	primary	sort	links_once	prefix
total	0	4	3	470003	0	483328	0	2026-11-02T09:30:00.000000Z	size	name	true	
prefix	1	2	1	420000	0	425984	0	2026-03-01T10:00:00.000000Z	size	name	true	2025/
prefix	1	1	0	50000	0	53248	0	2026-11-02T09:30:00.000000Z	size	name	true	2026/
```

| Field | Meaning |
|---|---|
| `row_type` | `total` for the total, `prefix` for every other row |
| `depth` | the row's level below SOURCE; 0 is the total |
| `objects` | the objects under the prefix (a local tree's files and other entries, never its directories) |
| `dirs` | the directories below the prefix (local trees only) |
| `size` | the sum of the objects' lengths, as the dataset's `size` column holds them |
| `size_missing` | the objects counted that had no length (a local walk with `--metadata names`) |
| `allocated_bytes` | the sum of the files' allocated bytes (`st_blocks` × 512), as the dataset's `allocated_bytes` column; null for a source that has none, such as a bucket |
| `allocated_bytes_missing` | the objects counted that had no allocated bytes |
| `max_last_modified` | the newest `last_modified` among the objects counted under the prefix; directories, S3 delete markers and the names of a hard-linked file that is counted under another do not count; null when no object had one |
| `primary` | the figure `--sort size` orders by and the table puts first: `size`, or `allocated_bytes` with `--allocated` |
| `sort` | the order of the rows, `name` or `size` (`--sort`) |
| `links_once` | `true` when a file with several hard links is counted once, under the first of its names: a local tree listed with stat, without `-l`; `false` for a bucket, with `--metadata names` and on Windows, which read no links. It says how the listing counts, whatever the rows hold, and a saved dataset gives the same as the live run: its manifest records the metadata tier |
| `prefix` | the prefix, up to and including its `/`; the total's is SOURCE's prefix up to its last `/`, empty for a whole bucket or tree |

Every row has both figures, whichever `--allocated` puts first. A figure that no object under the prefix had is
null, `\N` in TSV: it is unknown, not zero. A figure that only some objects had is the sum of the ones there
were, and its `*_missing` count says how many it leaves out. An empty prefix has 0 of each figure its source
has: an empty bucket's `size` is 0 and its `allocated_bytes` null. The byte figures are JSON numbers and can
exceed 2^53; parse them as 64-bit integers, not as floating point (JavaScript's `JSON.parse` rounds them).

TSV puts the numbers first and the prefix last, as `du` prints a size and then its path; `--no-header` leaves
out its header line. JSONL has the same fields, the prefix after `depth` (`prefix_base64` for a prefix that is
not valid UTF-8). Totals read from a saved dataset (`swath du --from-dataset`) add a last JSONL member,
`dataset`: the directory (`path`), the listed `source`, the `schema_id`, `listing_started_at` and
`listing_finished_at`, and the listing's coverage from its manifest (`complete`, `stop_reason`, `unreadable`,
`filters`, `exclusions`; null where the manifest does not say), so a stored row still says it was not a live
listing and how complete it was. Both sort the rows by prefix in byte order, the total first and each parent
before its children; the table puts the total last. Select the total by `row_type`, not by its place:
`jq 'select(.row_type == "total") | .size'`. Three members depend on the source or the data, and every other
is in every row: `dirs` (a local tree's rows only), `dataset` (JSONL read from a saved dataset) and
`prefix_base64` (in place of `prefix`, for a prefix that is not valid UTF-8).

Read the totals with care:

- **Each row includes everything below it.** Add up rows of one depth only: the depth-1 rows together leave out
  the objects directly under SOURCE, and all rows together count an object once per level it is under.
- **A null figure was not read**, which is not an empty file: with `--metadata names` a local walk reads names
  only, so both figures are null beside their counts:

  ```text
  $ swath du /data/photos --metadata names --format jsonl
  {"row_type":"total","depth":0,"prefix":"","objects":4,"dirs":3,"size":null,"size_missing":4,"allocated_bytes":null,"allocated_bytes_missing":4,"max_last_modified":null,"primary":"size","sort":"name","links_once":false}
  ```

- **`size` is not disk use.** It is the objects' lengths as listed: not storage consumed, compression, replicas
  or a bill. `allocated_bytes` is closer to `du`'s figure, with limits of its own
  ([Apparent and allocated bytes](results.md#apparent-and-allocated-bytes)). A local directory's own size and
  blocks are in no total.
- **`max_last_modified` is not a directory's time.** It is the newest of the objects' own `last_modified`, the
  column `swath list` prints for each row.
- **Filters apply first**, to every row, directories included ([Filters](filters.md)).

The table shows one figure: `size`, or with `--allocated` the allocated bytes, headed `on disk`. It rounds it
to three significant digits in binary units and groups the counts' digits (`60,004`). A local tree's table
calls its objects `files` and its prefixes `path`, and indents each path two blanks a level below the first.
It shows `max_last_modified` as the [list table](#the-table) shows times: local, to the second, under a heading
that names the zone, `-` when no object had one. An unknown figure is `-`, and a partial one is a lower bound,
`>= 12.0 KiB`. For a source with allocated bytes, a line on stderr under the table gives the total of both
figures, the shown one first, so a sparse file or a tree of small files shows at a glance; `-q` leaves it out:

```text
$ swath du -d 2 /data/photos
    size  files  dirs  last_modified (CET)  path
 410 KiB      2     1  2026-03-01 11:00:00  2025/
 117 KiB      1     0  2026-03-01 11:00:00    2025/raw/
48.8 KiB      1     0  2026-11-02 10:30:00  2026/
 459 KiB      4     3  2026-11-02 10:30:00  (total)
  459 KiB in 4 files · 472 KiB on disk
```

With `--allocated` the line reads `472 KiB on disk in 4 files · 459 KiB of data`; when no file's allocated bytes
were read, `on disk unknown`. A bucket's table has no such line:

```text
$ swath du -d 1 s3://bucket
   size  objects  last_modified (CET)  prefix
357 MiB   60,000  2026-11-15 06:59:58  logs/
   18 B        3  2026-01-01 01:00:00  weird/
357 MiB   60,004  2026-11-15 06:59:58  (total)
```

The table fits 80 columns at `-d 2` while a path stays within about 34 characters; a longer path is printed
whole and wraps.

The totals are printed when the count completes. A run that fails or stops before prints none; one that
fails while they are written exits non-zero. `--report FILE` adds the total and the counts to the report's
`output.totals`.
