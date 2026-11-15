# swath du

swath du totals SOURCE's objects (a local tree's files), their lengths and allocated bytes, and their newest time, per prefix down to a depth.

## Synopsis

<!-- synopsis:begin -->
```text
swath du SOURCE [OPTIONS]
swath du --from-dataset DIR [SOURCE] [OPTIONS]
```
<!-- synopsis:end -->

## Description

`swath du` lists SOURCE and prints one total, or with `-d N` the total of each prefix down to N levels below
SOURCE, a parent including its children, as `du` does for directories. It writes no rows and keeps nothing
on disk.

```text
$ swath du -d 1 /data/photos --format table
    size  files  dirs  last_modified (CET)  path
 410 KiB      2     1  2026-03-01 11:00:00  2025/
48.8 KiB      1     0  2026-11-02 10:30:00  2026/
 459 KiB      4     3  2026-11-02 10:30:00  (total)
  459 KiB in 4 files · 472 KiB on disk
```

A prefix ends at a `/`. Depth counts from SOURCE's prefix up to its last `/`: `s3://b/logs/2026/` counts from
`logs/2026/`, `s3://b/logs` from the top of the bucket. A key with fewer than N slashes counts only in the
prefixes it has. Each row includes everything below it, so add up rows of one depth only, never across depths.

On a terminal the rows are a table with the total last; in a pipe they are TSV (`--no-header` leaves out its
header line), and `--format jsonl` gives JSONL; both put the total first, its `row_type` `total`. The fields,
and the care their totals need, are in [Output](../output.md#du-rows).

TSV and JSONL rows have two byte figures: `size`, the objects' lengths as listed, and `allocated_bytes`, a local
file's allocated blocks (`st_blocks` × 512; a sparse file counts its blocks, not its length), `on disk`. The
table shows one of them, and on a local tree a line on stderr under it gives the totals of both.
Neither is the storage consumed, replicas or a bill ([Apparent and allocated
bytes](../results.md#apparent-and-allocated-bytes)). A bucket has no allocated bytes: its rows carry them as
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

The [filters](../filters.md) apply before counting, to every row: on a local tree directories too, so
`--min-size 1M` keeps the large files and drops the directories, whose own size is small.

Memory grows with the number of distinct prefixes, not objects. A run that would hold more than
`--max-prefixes` (1,000,000 by default, about 150 bytes each) fails rather than print partial totals.

Entries of a local tree that could not be read (permissions) are not in the totals: the run prints the totals of
the rest, names the entries on stderr and exits 3 ([exit codes](../reference/exit-codes.md)).

### Totals from a saved dataset

Each `swath du SOURCE` lists SOURCE again. To look at a large source at several depths, or with other filters,
list it once into a dataset (`swath list SOURCE -o DIR`) and total that instead (a live `swath du` that listed
a million entries or more says so on a terminal: its statistics end with `hint:` lines giving both commands
for its SOURCE):

```text
$ swath du --from-dataset photos-inventory -d 1 --sort size
swath: totals of the dataset photos-inventory, not a live listing: /data/photos as listed 2026-11-12T10:00:03Z (3 hours ago); complete
    size  files  dirs  last_modified (CET)  path
 410 KiB      2     1  2026-03-01 11:00:00  2025/
48.8 KiB      1     0  2026-11-02 10:30:00  2026/
 459 KiB      4     3  2026-11-02 10:30:00  (total)
  459 KiB in 4 files · 472 KiB on disk
```

The rows are the ones `swath du` of the dataset's source printed when it was listed, with the same depth, order,
formats and filters. They are read from the dataset's Parquet parts: the key and only the columns the totals
and filters need, in one parallel pass, once every part's columns are checked against the dataset's schema (a
dataset that breaks it is refused, exit 1). Depth counts from the dataset's scope, as it would from the source.
Without `--from-dataset`, `swath du DIR` totals the files in DIR, never the rows of the dataset they hold.

An optional SOURCE after the directory takes the form a live `swath du` takes: the dataset's source as it was
listed (the `source` stderr names), which totals the whole dataset as no SOURCE does, or a path or URL below it,
which restricts the totals to the keys below it. A local path is made absolute, as a live `swath du` resolves
it, and taken as a directory below the listed tree (`/data/photos/2025` totals the keys under `2025/`); a URL
names the same bucket or container and a prefix that starts with the listed one (`s3://bucket/logs/2026/` of a
dataset listed from `s3://bucket/logs/`). Depth counts from its prefix up to the last `/`, and the read seeks
to the prefix rather than scanning the dataset. A SOURCE below the listed one that holds no key prints zeros
and warns, exit 0, as a live listing of an empty prefix. Any other SOURCE is refused (exit 2), naming the
dataset's: `"photos": photos-inventory was listed from /data/photos; give a path below it, or no argument`.

The output says the totals are saved rows. A line on stderr names the dataset, its source, when it was listed
and how far the listing reached (`complete`, `INCOMPLETE` with the reason, the entries it could not read, the
listing's filters and exclusions). `-q` silences that line. In JSONL each row also carries a `dataset` member with
the same facts, its `filters` as the manifest's `coverage.filters` records them ([Output](../output.md#du-rows)). A dataset whose listing did not complete, such as a
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

## Options

`swath du` refuses `swath list`'s output options (`-o`, `--overwrite`, `--columns` and the part and budget
sizes) with exit 2.

<!-- flags:begin -->
### Totals

- <a id="option-depth"></a>[`-d, --depth N`](#option-depth)\
  also total each prefix down to N levels below SOURCE (0: the total)

  A prefix ends at a '/'; each includes what is below it, as du. --max-depth N, GNU du's spelling, is the same.

- <a id="option-allocated"></a>[`--allocated`](#option-allocated)\
  local: show and --sort size by the files' allocated bytes (on disk)

  The files' st_blocks x 512: the table's one figure, the line under it giving the lengths; TSV and JSONL rows carry both either way. A directory's own blocks are not counted, so a tree's total is below du's by them. A file without them (the names metadata tier reads none) fails the count.

- <a id="option-apparent-size"></a>[`--apparent-size`](#option-apparent-size)\
  show and --sort size by the lengths (the default)

  Not with --allocated.

- <a id="option-count-links"></a>[`-l, --count-links`](#option-count-links)\
  local: count a file with several hard links under each of its names

  By default such a file counts once, as du counts it, under the first of its names in byte order. The names metadata tier, and Windows, read no links: every name counts there.

- <a id="option-sort"></a>[`--sort name|size`](#option-sort)\
  order prefixes by name, or by size, largest first (default `name`)

  By size the prefixes below each parent are ordered, each still above its own; the total keeps its place (last in the table, first in TSV and JSONL), and a prefix of unknown size comes last.

- <a id="option-max-prefixes"></a>[`--max-prefixes N`](#option-max-prefixes)\
  fail rather than hold more than N distinct prefixes (default `1000000`)

  Raise it for a deep --depth over a wide tree when memory allows (about 150 bytes a prefix); it bounds memory, never the objects counted.

- <a id="option-max-inodes"></a>[`--max-inodes N`](#option-max-inodes)\
  local: the most hard-linked files to remember (default `1000000`)

  Unless --count-links. Raise it for a tree of more hard-linked files when memory allows (about 100 bytes each).

- <a id="option-from-dataset"></a>[`--from-dataset DIR`](#option-from-dataset)\
  total the dataset saved in DIR (swath list -o DIR) instead of listing

  An optional SOURCE, the dataset's source as listed or a path or URL below it, restricts the totals to it, sought rather than scanned. DIR is a local dataset directory; without this option swath du DIR totals DIR's own files. Options that shape a listing are refused. Stderr names the dataset's source, when it was listed and what its listing covered.

### Output

- <a id="option-format"></a>[`--format auto|table|tsv|jsonl`](#option-format)\
  on stdout: auto is the table on a terminal, TSV otherwise

- <a id="option-no-header"></a>[`--no-header`](#option-no-header)\
  TSV: leave out the header line of column names

  Not with the table, JSONL or a dataset; on a terminal the rows are then TSV.

### Filters

- <a id="option-include"></a>[`--include GLOB`](#option-include)\
  keep keys this glob matches (repeatable: any of them)

  Matched against the key below SOURCE: with a '/' from SOURCE's root (logs/\*.gz), without one the last segment at any depth (\*.gz). \* and ? stay within a segment, \*\* is whole segments, \[a-z\], \[!a-z\], {a,b}, \\c. Includes sharing a literal prefix narrow the listed prefix. Filters select rows; recursive excludes can also prune local traversal.

- <a id="option-exclude"></a>[`--exclude GLOB`](#option-exclude)\
  drop keys this glob matches (repeatable; beats any include)

  Matched as --include is. dir/ drops a directory's own row, dir/\*\* the rows below it. Local trees skip traversal below directories covered by a terminal /\*\*; other excludes only filter rows.

- <a id="option-include-regex"></a>[`--include-regex REGEX`](#option-include-regex)\
  keep keys this RE2 regex matches anywhere in the key (repeatable)

  Matched against the key below SOURCE, as a glob is, on every source: ^b/ is SOURCE's b/. Any include, glob or regex, keeps a key. ^ with a literal prefix narrows the listed prefix as a glob's does.

- <a id="option-exclude-regex"></a>[`--exclude-regex REGEX`](#option-exclude-regex)\
  drop keys this RE2 regex matches anywhere in the key (repeatable)

- <a id="option-min-size"></a>[`--min-size SIZE`](#option-min-size)\
  keep objects (local: entries) of at least this size (1K, 1.5GiB)

  Every SIZE: bytes, or K, M, G, T and KiB, MiB, GiB, TiB (binary) or KB, MB, GB, TB (decimal), any case.

- <a id="option-max-size"></a>[`--max-size SIZE`](#option-max-size)\
  keep objects or entries of at most this size

- <a id="option-modified-since"></a>[`--modified-since TIME`](#option-modified-since)\
  keep objects or entries modified at or after TIME (2026-01-01, 7d)

  2026-01-01, 2026-01-01T12:00 or 2026-01-01 12:00:00 is local time (TZ), as the tables print it; with Z or an offset, that zone. A duration (7d, 12h) is that long before the run started. Either is recorded as the UTC time it names.

- <a id="option-modified-until"></a>[`--modified-until TIME`](#option-modified-until)\
  keep objects or entries modified at or before this time

- <a id="option-storage-class"></a>[`--storage-class NAME[,NAME...]`](#option-storage-class)\
  keep objects in these storage classes (STANDARD,GLACIER; repeatable)

  In any case; an object without one is in none. Azure Blob: the access tier (Hot, Cool, Cold, Archive).

### Object stores

- <a id="option-anonymous"></a>[`--anonymous`](#option-anonymous)\
  send no credentials (a public bucket or container)

  Ignores any configured. Without it a source with no usable credentials fails (auth) before listing.

- <a id="option-endpoint-url"></a>[`--endpoint-url URL`](#option-endpoint-url)\
  the service URL in place of the provider's own (emulators, MinIO)

  S3: path-style addressing unless --no-s3-path-style; default AWS_ENDPOINT_URL_S3, else AWS_ENDPOINT_URL. GCS: the JSON API base URL (fake-gcs-server). Azure Blob: the blob service URL, the account then its first path segment (Azurite). User information, a query and a fragment are refused. Part of the resume identity.

- <a id="option-ca-bundle"></a>[`--ca-bundle FILE`](#option-ca-bundle)\
  trust the certificates in this PEM file besides the system's

  For a service behind a private certificate authority. S3: default AWS_CA_BUNDLE.

- <a id="option-no-verify-ssl"></a>[`--no-verify-ssl`](#option-no-verify-ssl)\
  do not verify the service's TLS certificate (warns)

  For a test endpoint only. A resume replays it, and warns again.

- <a id="option-all-versions"></a>[`--all-versions`](#option-all-versions)\
  list every version of every key, not only the current objects

  S3: versions and delete markers (needs s3:ListBucketVersions). GCS: generations, live and noncurrent. Azure Blob: versions, snapshots and soft-deleted blobs. A key's rows are consecutive and never split across parts.

### S3

- <a id="option-s3-region"></a>[`--s3-region NAME`](#option-s3-region)\
  the bucket's region

  Default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, else us-east-1.

- <a id="option-s3-profile"></a>[`--s3-profile NAME`](#option-s3-profile)\
  the shared config profile to take credentials and region from

  Default: the AWS SDK chain (environment, AWS_PROFILE or default, SSO, web identity, IMDS).

- <a id="option-s3-requester-pays"></a>[`--s3-requester-pays`](#option-s3-requester-pays)\
  accept the request charges of a requester-pays bucket

- <a id="option-s3-path-style"></a>[`--s3-path-style`](#option-s3-path-style)\
  address the bucket in the path (default with --endpoint-url)

- <a id="option-no-s3-path-style"></a>[`--no-s3-path-style`](#option-no-s3-path-style)\
  address the bucket in the host name, also with --endpoint-url

- <a id="option-s3-bearer-token-command"></a>[`--s3-bearer-token-command COMMAND`](#option-s3-bearer-token-command)\
  a shell command printing a bearer token, sent instead of SigV4

  For GCS's XML API: 'gcloud auth print-access-token'. Never recorded: swath resume needs it again.

- <a id="option-s3-bearer-token-refresh-interval"></a>[`--s3-bearer-token-refresh-interval DURATION`](#option-s3-bearer-token-refresh-interval)\
  run --s3-bearer-token-command again after this long (default `45m`)

- <a id="option-s3-fips"></a>[`--s3-fips`](#option-s3-fips)\
  use the FIPS endpoint (s3-fips.REGION.amazonaws.com)

  Default: AWS_USE_FIPS_ENDPOINT. Refused with --endpoint-url, where it would select nothing.

- <a id="option-s3-dualstack"></a>[`--s3-dualstack`](#option-s3-dualstack)\
  use the dual-stack (IPv6) endpoint

  Default: AWS_USE_DUALSTACK_ENDPOINT. Refused with --endpoint-url, where it would select nothing.

- <a id="option-s3-fetch-owner"></a>[`--s3-fetch-owner`](#option-s3-fetch-owner)\
  fill owner_id and owner_display_name

### GCS

- <a id="option-gcs-credentials-file"></a>[`--gcs-credentials-file FILE`](#option-gcs-credentials-file)\
  a service-account or authorized-user JSON file (default: ADC)

  Without it, Application Default Credentials.

- <a id="option-gcs-billing-project"></a>[`--gcs-billing-project NAME`](#option-gcs-billing-project)\
  bill a requester-pays bucket's requests to this project

### Azure Blob

- <a id="option-azure-sas-token"></a>[`--azure-sas-token TOKEN`](#option-azure-sas-token)\
  a shared access signature (a query string); never recorded

  Else a SAS in the source's query, else AZURE_STORAGE_SAS_TOKEN; with none, an account key in AZURE_STORAGE_KEY (Shared Key), else Entra ID (DefaultAzureCredential). Give it again to swath resume.

- <a id="option-azure-fetch-metadata"></a>[`--azure-fetch-metadata`](#option-azure-fetch-metadata)\
  fill the metadata column with each blob's user metadata

  A JSON object, keys sorted; {} for a blob without any, null for one encrypted with a customer-provided key. Up to 8 KiB a blob: it grows the responses, the memory a row holds and the dataset.

- <a id="option-azure-fetch-tags"></a>[`--azure-fetch-tags`](#option-azure-fetch-tags)\
  fill the tags column with each blob's index tags

  A JSON object, keys sorted; {} for a blob without any. Needs the right to read tags (a SAS with the t permission, or an Entra ID role such as Storage Blob Data Owner); without it every request fails, anonymous ones included.

- <a id="option-azure-fetch-immutability"></a>[`--azure-fetch-immutability`](#option-azure-fetch-immutability)\
  fill the immutability policy and legal hold columns

- <a id="option-azure-assume-flat"></a>[`--azure-assume-flat`](#option-azure-assume-flat)\
  assume a flat namespace where the account will not tell

  For when Get Account Information and the Data Lake host's Filesystem Get Properties are both refused or silent (a Data Lake host that refuses the account for soft delete or blob events already establishes a flat one); a hierarchical namespace they report still refuses. Frozen for resume and shown in the report.

### Local

- <a id="option-metadata"></a>[`--metadata names|stat|statx`](#option-metadata)\
  what is read per entry (statx adds created_at; Linux) (default `stat`)

- <a id="option-no-dirs"></a>[`--no-dirs`](#option-no-dirs)\
  omit the directories' rows (kind dir)

- <a id="option-no-link-target"></a>[`--no-link-target`](#option-no-link-target)\
  do not read symbolic links' targets

- <a id="option-one-file-system"></a>[`-x, --one-file-system`](#option-one-file-system)\
  do not walk into a mount point below the root, as du -x

### Performance

- <a id="option-workers"></a>[`-j, --workers N`](#option-workers)\
  listing workers (default 64; a local tree: 4 per CPU, at most 64)

  Raise it (128, 256) for a large listing the service does not throttle: listing waits on round trips, not the CPU; lower it when it throttles at the default.

- <a id="option-seed-from"></a>[`--seed-from PATH`](#option-seed-from)\
  object stores: start from a previous listing's key distribution

  A committed dataset of the same source and scope (its cuts.txt, else its parts' key page index), or a cut file (one key per line, ascending bytewise, optionally TAB and the keys expected since the previous cut; a bucket dataset's cuts.txt is one); the engine still splits adaptively.

### Limits

- <a id="option-max-duration"></a>[`--max-duration DURATION`](#option-max-duration)\
  stop the run after this long (exit 124); 0: no limit

  Give every unattended run one. Only a dataset run of an object store (one with a journal) resumes where it stopped; text output and swath du run again.

- <a id="option-stall-timeout"></a>[`--stall-timeout DURATION`](#option-stall-timeout)\
  stop as stuck after this long without progress (exit 75) (default `10m`)

  0: never. Raise it for a service that is slow to answer for minutes at a time. Only a dataset run of an object store (one with a journal) resumes where it stopped.

- <a id="option-request-timeout"></a>[`--request-timeout DURATION`](#option-request-timeout)\
  per-request timeout; a request over it is retried (default `30s`)

  Raise it for a slow endpoint or emulator whose pages take longer to answer.

- <a id="option-request-rate"></a>[`--request-rate N`](#option-request-rate)\
  cap every HTTP attempt at N per second (0: no cap)

  Set it to stay under a request quota shared with other clients. Refused when the spacing exceeds half --stall-timeout.

- <a id="option-no-adaptive-limit"></a>[`--no-adaptive-limit`](#option-no-adaptive-limit)\
  do not lower concurrency when the service throttles requests

  Turn it off (with --request-rate) when a run crawls under sustained throttling with the limit near 1. Without it each of the --workers listing workers waits on one page at a time, and seed listings and structure probes run beside them.

### Reporting

- <a id="option-report"></a>[`--report FILE`](#option-report)\
  write the run report (JSON) to FILE, or - for stdout, at the end

  Written on failure too, with the outcome and the attempt id; not for a command line that does not parse (an unknown flag, a bad value). A directory, or a name whose directory does not exist, is refused before listing; a report that cannot be written at the end fails the command (exit 1). - is refused where stdout carries rows.

- <a id="option-verbose"></a>[`-v, --verbose`](#option-verbose)\
  a start line, progress records; -vv, -vvv: the request log

  -vv adds each failed request and retry with its cause, -vvv every request (credentials and tokens redacted).

- <a id="option-quiet"></a>[`-q, --quiet`](#option-quiet)\
  no warnings, no automatic progress or statistics (-qq: no errors)

  With -qq the exit code and the reports stand alone.

- <a id="option-progress"></a>[`--progress`](#option-progress)\
  show progress on stderr (default: a terminal that rows do not share)

  Not by default with -q or -v (which appends progress records), nor while rows go to the same terminal, where swath list refuses it; swath du's table comes after the line is erased.

- <a id="option-no-progress"></a>[`--no-progress`](#option-no-progress)\
  no live progress on stderr

- <a id="option-progress-interval"></a>[`--progress-interval DURATION`](#option-progress-interval)\
  how often progress is printed (at least 1s); implies --progress

  Default 1s while the line redraws on a terminal, 30s for appended records.

- <a id="option-stats"></a>[`--stats`](#option-stats)\
  print the end-of-run statistics on stderr

  Default: for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe.

- <a id="option-no-stats"></a>[`--no-stats`](#option-no-stats)\
  no end-of-run statistics on stderr

- <a id="option-color"></a>[`--color auto|always|never`](#option-color)\
  color the progress line, the statistics and the help (default `auto`)

  auto: on a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it.

- <a id="option-log-format"></a>[`--log-format text|json`](#option-log-format)\
  stderr as text, or json: one JSON object per line (default `text`)

  json: warnings, the start line, progress records, the request log and the outcome as events, no redrawn line or statistics block.

<details>
<summary>Plain text, in the <code>--help</code> layout</summary>

```text
Totals:
  -d, --depth N               also total each prefix down to N levels below SOURCE (0: the total)
                                A prefix ends at a '/'; each includes what is below it, as du.
                                --max-depth N, GNU du's spelling, is the same.
      --allocated             local: show and --sort size by the files' allocated bytes (on disk)
                                The files' st_blocks x 512: the table's one figure, the line under
                                it giving the lengths; TSV and JSONL rows carry both either way. A
                                directory's own blocks are not counted, so a tree's total is below
                                du's by them. A file without them (the names metadata tier reads
                                none) fails the count.
      --apparent-size         show and --sort size by the lengths (the default)
                                Not with --allocated.
  -l, --count-links           local: count a file with several hard links under each of its names
                                By default such a file counts once, as du counts it, under the first
                                of its names in byte order. The names metadata tier, and Windows,
                                read no links: every name counts there.
      --sort name|size        order prefixes by name, or by size, largest first (default name)
                                By size the prefixes below each parent are ordered, each still above
                                its own; the total keeps its place (last in the table, first in TSV
                                and JSONL), and a prefix of unknown size comes last.
      --max-prefixes N        fail rather than hold more than N distinct prefixes (default 1000000)
                                Raise it for a deep --depth over a wide tree when memory allows
                                (about 150 bytes a prefix); it bounds memory, never the objects
                                counted.
      --max-inodes N          local: the most hard-linked files to remember (default 1000000)
                                Unless --count-links. Raise it for a tree of more hard-linked files
                                when memory allows (about 100 bytes each).
      --from-dataset DIR      total the dataset saved in DIR (swath list -o DIR) instead of listing
                                An optional SOURCE, the dataset's source as listed or a path or URL
                                below it, restricts the totals to it, sought rather than scanned.
                                DIR is a local dataset directory; without this option swath du DIR
                                totals DIR's own files. Options that shape a listing are refused.
                                Stderr names the dataset's source, when it was listed and what its
                                listing covered.

Output:
      --format auto|table|tsv|jsonl
                              on stdout: auto is the table on a terminal, TSV otherwise
      --no-header             TSV: leave out the header line of column names
                                Not with the table, JSONL or a dataset; on a terminal the rows are
                                then TSV.

Filters:
      --include GLOB          keep keys this glob matches (repeatable: any of them)
                                Matched against the key below SOURCE: with a '/' from SOURCE's root
                                (logs/*.gz), without one the last segment at any depth (*.gz). * and
                                ? stay within a segment, ** is whole segments, [a-z], [!a-z], {a,b},
                                \c. Includes sharing a literal prefix narrow the listed prefix.
                                Filters select rows; recursive excludes can also prune local
                                traversal.
      --exclude GLOB          drop keys this glob matches (repeatable; beats any include)
                                Matched as --include is. dir/ drops a directory's own row, dir/**
                                the rows below it. Local trees skip traversal below directories
                                covered by a terminal /**; other excludes only filter rows.
      --include-regex REGEX   keep keys this RE2 regex matches anywhere in the key (repeatable)
                                Matched against the key below SOURCE, as a glob is, on every source:
                                ^b/ is SOURCE's b/. Any include, glob or regex, keeps a key. ^ with
                                a literal prefix narrows the listed prefix as a glob's does.
      --exclude-regex REGEX   drop keys this RE2 regex matches anywhere in the key (repeatable)
      --min-size SIZE         keep objects (local: entries) of at least this size (1K, 1.5GiB)
                                Every SIZE: bytes, or K, M, G, T and KiB, MiB, GiB, TiB (binary) or
                                KB, MB, GB, TB (decimal), any case.
      --max-size SIZE         keep objects or entries of at most this size
      --modified-since TIME   keep objects or entries modified at or after TIME (2026-01-01, 7d)
                                2026-01-01, 2026-01-01T12:00 or 2026-01-01 12:00:00 is local time
                                (TZ), as the tables print it; with Z or an offset, that zone. A
                                duration (7d, 12h) is that long before the run started. Either is
                                recorded as the UTC time it names.
      --modified-until TIME   keep objects or entries modified at or before this time
      --storage-class NAME[,NAME...]
                              keep objects in these storage classes (STANDARD,GLACIER; repeatable)
                                In any case; an object without one is in none. Azure Blob: the
                                access tier (Hot, Cool, Cold, Archive).

Object stores:
      --anonymous             send no credentials (a public bucket or container)
                                Ignores any configured. Without it a source with no usable
                                credentials fails (auth) before listing.
      --endpoint-url URL      the service URL in place of the provider's own (emulators, MinIO)
                                S3: path-style addressing unless --no-s3-path-style; default
                                AWS_ENDPOINT_URL_S3, else AWS_ENDPOINT_URL. GCS: the JSON API base
                                URL (fake-gcs-server). Azure Blob: the blob service URL, the account
                                then its first path segment (Azurite). User information, a query and
                                a fragment are refused. Part of the resume identity.
      --ca-bundle FILE        trust the certificates in this PEM file besides the system's
                                For a service behind a private certificate authority. S3: default
                                AWS_CA_BUNDLE.
      --no-verify-ssl         do not verify the service's TLS certificate (warns)
                                For a test endpoint only. A resume replays it, and warns again.
      --all-versions          list every version of every key, not only the current objects
                                S3: versions and delete markers (needs s3:ListBucketVersions). GCS:
                                generations, live and noncurrent. Azure Blob: versions, snapshots
                                and soft-deleted blobs. A key's rows are consecutive and never split
                                across parts.

S3:
      --s3-region NAME        the bucket's region
                                Default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, else
                                us-east-1.
      --s3-profile NAME       the shared config profile to take credentials and region from
                                Default: the AWS SDK chain (environment, AWS_PROFILE or default,
                                SSO, web identity, IMDS).
      --s3-requester-pays     accept the request charges of a requester-pays bucket
      --s3-path-style         address the bucket in the path (default with --endpoint-url)
      --no-s3-path-style      address the bucket in the host name, also with --endpoint-url
      --s3-bearer-token-command COMMAND
                              a shell command printing a bearer token, sent instead of SigV4
                                For GCS's XML API: 'gcloud auth print-access-token'. Never recorded:
                                swath resume needs it again.
      --s3-bearer-token-refresh-interval DURATION
                              run --s3-bearer-token-command again after this long (default 45m)
      --s3-fips               use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
                                Default: AWS_USE_FIPS_ENDPOINT. Refused with --endpoint-url, where
                                it would select nothing.
      --s3-dualstack          use the dual-stack (IPv6) endpoint
                                Default: AWS_USE_DUALSTACK_ENDPOINT. Refused with --endpoint-url,
                                where it would select nothing.
      --s3-fetch-owner        fill owner_id and owner_display_name

GCS:
      --gcs-credentials-file FILE
                              a service-account or authorized-user JSON file (default: ADC)
                                Without it, Application Default Credentials.
      --gcs-billing-project NAME
                              bill a requester-pays bucket's requests to this project

Azure Blob:
      --azure-sas-token TOKEN
                              a shared access signature (a query string); never recorded
                                Else a SAS in the source's query, else AZURE_STORAGE_SAS_TOKEN; with
                                none, an account key in AZURE_STORAGE_KEY (Shared Key), else Entra
                                ID (DefaultAzureCredential). Give it again to swath resume.
      --azure-fetch-metadata  fill the metadata column with each blob's user metadata
                                A JSON object, keys sorted; {} for a blob without any, null for one
                                encrypted with a customer-provided key. Up to 8 KiB a blob: it grows
                                the responses, the memory a row holds and the dataset.
      --azure-fetch-tags      fill the tags column with each blob's index tags
                                A JSON object, keys sorted; {} for a blob without any. Needs the
                                right to read tags (a SAS with the t permission, or an Entra ID role
                                such as Storage Blob Data Owner); without it every request fails,
                                anonymous ones included.
      --azure-fetch-immutability
                              fill the immutability policy and legal hold columns
      --azure-assume-flat     assume a flat namespace where the account will not tell
                                For when Get Account Information and the Data Lake host's Filesystem
                                Get Properties are both refused or silent (a Data Lake host that
                                refuses the account for soft delete or blob events already
                                establishes a flat one); a hierarchical namespace they report still
                                refuses. Frozen for resume and shown in the report.

Local:
      --metadata names|stat|statx
                              what is read per entry (statx adds created_at; Linux) (default stat)
      --no-dirs               omit the directories' rows (kind dir)
      --no-link-target        do not read symbolic links' targets
  -x, --one-file-system       do not walk into a mount point below the root, as du -x

Performance:
  -j, --workers N             listing workers (default 64; a local tree: 4 per CPU, at most 64)
                                Raise it (128, 256) for a large listing the service does not
                                throttle: listing waits on round trips, not the CPU; lower it when
                                it throttles at the default.
      --seed-from PATH        object stores: start from a previous listing's key distribution
                                A committed dataset of the same source and scope (its cuts.txt, else
                                its parts' key page index), or a cut file (one key per line,
                                ascending bytewise, optionally TAB and the keys expected since the
                                previous cut; a bucket dataset's cuts.txt is one); the engine still
                                splits adaptively.

Limits:
      --max-duration DURATION
                              stop the run after this long (exit 124); 0: no limit
                                Give every unattended run one. Only a dataset run of an object store
                                (one with a journal) resumes where it stopped; text output and swath
                                du run again.
      --stall-timeout DURATION
                              stop as stuck after this long without progress (exit 75) (default 10m)
                                0: never. Raise it for a service that is slow to answer for minutes
                                at a time. Only a dataset run of an object store (one with a
                                journal) resumes where it stopped.
      --request-timeout DURATION
                              per-request timeout; a request over it is retried (default 30s)
                                Raise it for a slow endpoint or emulator whose pages take longer to
                                answer.
      --request-rate N        cap every HTTP attempt at N per second (0: no cap)
                                Set it to stay under a request quota shared with other clients.
                                Refused when the spacing exceeds half --stall-timeout.
      --no-adaptive-limit     do not lower concurrency when the service throttles requests
                                Turn it off (with --request-rate) when a run crawls under sustained
                                throttling with the limit near 1. Without it each of the --workers
                                listing workers waits on one page at a time, and seed listings and
                                structure probes run beside them.

Reporting:
      --report FILE           write the run report (JSON) to FILE, or - for stdout, at the end
                                Written on failure too, with the outcome and the attempt id; not for
                                a command line that does not parse (an unknown flag, a bad value). A
                                directory, or a name whose directory does not exist, is refused
                                before listing; a report that cannot be written at the end fails the
                                command (exit 1). - is refused where stdout carries rows.
  -v, --verbose               a start line, progress records; -vv, -vvv: the request log
                                -vv adds each failed request and retry with its cause, -vvv every
                                request (credentials and tokens redacted).
  -q, --quiet                 no warnings, no automatic progress or statistics (-qq: no errors)
                                With -qq the exit code and the reports stand alone.
      --progress              show progress on stderr (default: a terminal that rows do not share)
                                Not by default with -q or -v (which appends progress records), nor
                                while rows go to the same terminal, where swath list refuses it;
                                swath du's table comes after the line is erased.
      --no-progress           no live progress on stderr
      --progress-interval DURATION
                              how often progress is printed (at least 1s); implies --progress
                                Default 1s while the line redraws on a terminal, 30s for appended
                                records.
      --stats                 print the end-of-run statistics on stderr
                                Default: for runs over 1.5 s, runs that write output, and runs that
                                do not complete; not with -q, nor after a closed downstream pipe.
      --no-stats              no end-of-run statistics on stderr
      --color auto|always|never
                              color the progress line, the statistics and the help (default auto)
                                auto: on a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE
                                forces it.
      --log-format text|json  stderr as text, or json: one JSON object per line (default text)
                                json: warnings, the start line, progress records, the request log
                                and the outcome as events, no redrawn line or statistics block.
```

</details>
<!-- flags:end -->

## Examples

Total a prefix:

```bash
swath du s3://bucket/logs/
```

Total each top-level directory of a tree, largest first:

```bash
swath du -d 1 --sort size /data
```

Read the totals of a bucket's first two levels as JSONL:

```bash
swath du gs://bucket -d 2 --format jsonl
```

Total what a local tree's files take on disk, without crossing into other filesystems:

```bash
swath du /home --allocated --one-file-system
```

Total the objects larger than 1 GiB:

```bash
swath du s3://bucket --min-size 1GiB
```

List a bucket once, then total its saved dataset at two depths, and one prefix of it deeper, without listing
the bucket again:

```bash
swath list s3://bucket -o bucket-inventory
swath du --from-dataset bucket-inventory -d 2 --sort size
swath du --from-dataset bucket-inventory s3://bucket/logs/2026/ -d 3
```

## Exit status

0 when the totals are printed, also when the reader closes stdout early. 1 when the listing failed or the
prefixes passed `--max-prefixes`, and with `--from-dataset` when the dataset could not be read; 2 for a usage
error; 3 when the totals are printed but are incomplete: entries could not be read, or with `--from-dataset` the
dataset's listing did not cover its scope. An interrupted `swath du` is run again: it keeps no journal. See
[Exit codes](../reference/exit-codes.md).

## Environment

See [swath(1)](swath.md#environment) and [Sources](../sources/README.md).

## See also

[swath list](swath-list.md), [swath show](swath-show.md), [Output](../output.md#du-rows),
[Filters](../filters.md), [Sources](../sources/README.md)
