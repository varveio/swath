# Output

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
| a dataset in a bucket (`-o gs://BUCKET/PREFIX`, `-o s3://BUCKET/PREFIX` or `-o az://ACCOUNT/CONTAINER/PREFIX`, with `--work-dir DIR`) | the same, written straight to GCS, S3 or Azure Blob Storage: the runner keeps only spill and the run's metadata ([A dataset in a bucket](dataset.md#a-dataset-in-a-bucket), [production.md](production.md#writing-to-a-bucket)) |
| `swath du` | you want sizes and counts, not rows |

A [swath dataset](dataset.md) is a directory of sorted Parquet parts and a `manifest.json` that commits them.
A bare local destination or bucket prefix whose final path element ends in `.parquet` (case-insensitive) is
refused. Use a name without that suffix, or add a trailing separator to explicitly select a directory,
such as `-o inventory.parquet/` or `-o s3://out/inventory.parquet/` (a Spark-style directory).
The same name rule applies to `swath resume DIR`.
Querying one is shown in [getting-started.md](getting-started.md#6-query-it-with-duckdb).

A listing feeds analysis, migration planning or a comparison with an earlier one; swath itself does not copy,
compare or schedule. A dataset is not a direct `rclone --files-from` input: a handoff must generate and test a
compatible path list. Names, sizes and times alone do not validate a backup.

## Text rows

The `-o` name chooses the format and the compression: `NAME.tsv` or `NAME.jsonl`, optionally followed by
`.gz` or `.zst`. Without `-o`, or with `-o -`, the rows go to stdout as `--format` says: `auto` (the default)
is the table on a terminal and TSV otherwise, or `table`, `tsv`, `jsonl`. Compress stdout with a pipe
(`| zstd`). The file's directory must exist; the file is written beside its name and renamed into place when
complete, replacing a previous file.

TSV and JSONL rows have the dataset's columns under the dataset's names, by default `key`, then `size`,
`last_modified` and the rest of the source's schema, in the schema's order
([dataset.md](dataset.md#schema-registry)). Columns after `last_modified` differ between sources: read them by
name, not by position. `--columns size,key` prints only those columns, in that order. The table shows a few
columns for people, with `key` last.

Values are as the dataset stores them: S3's `etag` without its quotes, integers exact, booleans `true` and
`false`, every time in UTC with six fraction digits (`2026-01-01T00:00:00.123000Z`). A null is neither an
empty string nor zero.

### TSV

One header line of the column names, then one line per row, the fields separated by tabs:

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

For reading on a terminal: the time to the second in local time (`TZ` sets the zone), the size in binary
units, the local tree's `kind`, the version columns with `--all-versions`, and the key last, escaped as in TSV.
A missing value is blank.

```text
$ swath list /data/photos
last_modified (local)      size  kind          key
2026-11-02 09:30:00    4.00 KiB  DIR           2025/
2026-03-01 10:00:00     293 KiB  FILE          2025/a.jpg
2026-11-02 09:30:00    4.00 KiB  DIR           2025/raw/
2026-03-01 10:00:00     117 KiB  FILE          2025/raw/b.cr2
2026-11-02 09:30:00    4.00 KiB  DIR           2026/
2026-11-02 09:30:00    48.8 KiB  FILE          2026/c.jpg
2026-11-02 09:30:00         3 B  FILE          notes.txt
```

`--columns` does not apply to the table; with `--columns` on a terminal, `auto` prints TSV.

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

`swath du` prints, for the total and each prefix down to `-d N`, the objects under it, their bytes and the
newest modification time among them (a local tree adds its directories and the files without a size):

```text
$ swath du -d 1 /data/photos --format tsv
depth	objects	dirs	size	unsized	last_modified	prefix
0	4	3	470003	0	2026-11-02T09:30:00.000000Z	
1	2	1	420000	0	2026-03-01T10:00:00.000000Z	2025/
1	1	0	50000	0	2026-11-02T09:30:00.000000Z	2026/
```

| Field | Meaning |
|---|---|
| `depth` | the row's level below SOURCE; 0 is the total |
| `objects` | the objects under the prefix (a local tree's files and other entries, never its directories) |
| `dirs` | the directories below the prefix (local trees only) |
| `size` | the bytes of the objects that had a size (`allocated_size` with `--allocated`) |
| `unsized` | the objects that had no size (local trees only) |
| `last_modified` | the newest modification time under the prefix; null when none had one |
| `prefix` | the prefix, up to and including its `/`; the total's is SOURCE's prefix up to its last `/`, empty for a whole bucket or tree |

TSV puts the numbers first and the prefix last, as `du` prints a size and then its path; JSONL has the same
fields (`prefix_base64` for a prefix that is not valid UTF-8). Both sort the rows by prefix in byte order, the
total first and each parent before its children; the table puts the total last. Select the total by
`depth`, not by its place: `jq 'select(.depth == 0) | .size'`. An object-store source has no `dirs` and
`unsized` fields.

Read the totals with care:

- **Each row includes everything below it.** Add up rows of one depth only: the depth-1 rows together leave out
  the objects directly under SOURCE, and all rows together count an object once per level it is under.
- **`size` 0 with `unsized` above 0 means the sizes were not read**, not that the files are empty: with
  `--metadata names` a local walk reads names only. Any `unsized` above 0 means `size` leaves those files out;
  the table shows such a size as `-`:

  ```text
  $ swath du /data/photos --metadata names --format jsonl
  {"depth":0,"prefix":"","objects":4,"dirs":3,"size":0,"unsized":4,"last_modified":null}
  ```

- **`--allocated` renames the field.** The bytes are then `allocated_size` (blocks on disk, as `du` reports by
  default), not `size`; a script that reads `size` finds no such field:

  ```text
  $ swath du /data/photos --allocated --format jsonl
  {"depth":0,"prefix":"","objects":4,"dirs":3,"allocated_size":483328,"unsized":0,"last_modified":"2026-11-02T09:30:00.000000Z"}
  ```

- **The bytes are object sizes as listed**: not storage consumed, compression, replicas or a bill. A local
  directory's own size is in no total.
- **Filters apply first**, to every row, directories included ([filters.md](filters.md)).

The table rounds sizes to three significant digits in binary units, calls a local tree's objects `files` and
its prefixes `path`, and shows times in local time:

```text
$ swath du -d 1 s3://bucket
   size  objects  last_modified (local)  prefix
357 MiB    60000  2026-02-09 00:00:00    logs/
   18 B        3  2026-01-01 00:00:00    weird/
357 MiB    60004  2026-02-09 00:00:00    (total)
```

The totals are printed when the count completes. A run that fails or stops before prints none; one that
fails while they are written exits non-zero. `--report FILE` adds the total and the counts to the report's
`output.totals`.
