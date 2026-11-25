# swath du

swath du totals SOURCE's objects (a local tree's files), the space they take on disk and their lengths, and their newest time, per prefix down to a depth.

## Synopsis

<!-- synopsis:begin -->
```text
swath du SOURCE [OPTIONS]
swath du --from-dataset DIR [SOURCE] [OPTIONS]
```
<!-- synopsis:end -->

## Description

`swath du` lists SOURCE and prints one total, or with `-d N` the total of each prefix down to N levels below
SOURCE, a parent including its children, as `du` does for directories. On a terminal, without `-d`, it draws
the largest prefixes and objects at whatever depth they are ([the view](#the-view)). It writes no rows and keeps
nothing on disk.

### The view

Bare `swath du SOURCE` on a terminal answers "where are the bytes": a tree of the largest prefixes and objects,
largest first, each under its parent, with its size, count, share of the total, a bar and the newest date. What
it shows, what it folds into `(N more)` lines and how it fits the terminal are in
[Total a source](../totals.md#the-view).

How depth counts, filters and memory are in [Total a source](../totals.md#totals-per-prefix); the two byte
figures, hard links and unreadable entries in [How sizes are counted](../totals.md#how-sizes-are-counted).

### Totals from a saved dataset

`swath du --from-dataset DIR` prints the totals of a dataset saved by `swath list SOURCE -o DIR` instead of
listing SOURCE again ([Total a source](../totals.md#totals-from-a-saved-dataset)).

## Options

`swath du` refuses `swath list`'s output options (`-o`, `--overwrite`, `--columns` and the part and budget
sizes) with exit 2.

<!-- flags:begin -->
### Totals

- <a id="option-depth"></a>[`-d, --depth N`](#option-depth)\
  also total each prefix down to N levels below SOURCE (0: the total)

  A prefix ends at a '/'; each includes what is below it, as du. --max-depth N, GNU du's spelling, is the same. Without it, a terminal shows the view instead: the largest prefixes and objects at any depth.

- <a id="option-allocated"></a>[`--allocated`](#option-allocated)\
  local: show and --sort size by the space on disk, and require it

  The entries' st_blocks x 512 (on Windows the directory listing's allocation size), directories' included, as du counts them: a local tree's default where its listing reads them, so this only makes it required. A file without them fails the count; the names metadata tier, which reads none, refuses it. TSV and JSONL rows carry both figures either way.

- <a id="option-apparent-size"></a>[`-A, --apparent-size`](#option-apparent-size)\
  show and --sort size by the files' lengths, as du -A

  An object store's default, and a local tree's where its listing reads no blocks (the names metadata tier). Not with --allocated.

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
  stop at other filesystems (macOS: volumes; Windows: junctions, mounts)

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
  no warnings, no automatic progress or closing lines (-qq: no errors)

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
  print the closing lines on stderr, with the run's statistics

  The closing lines (the verdict, the count, the time, where the output went, what to run next) print by default for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe. --stats prints them always, followed by the requests, in flight, throttles, retries, the estimate and the peak RSS.

- <a id="option-no-stats"></a>[`--no-stats`](#option-no-stats)\
  no closing lines on stderr

- <a id="option-color"></a>[`--color auto|always|never`](#option-color)\
  color progress, closing lines, the help and du's view (default `auto`)

  auto: on a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it.

- <a id="option-log-format"></a>[`--log-format text|json`](#option-log-format)\
  stderr as text, or json: one JSON object per line (default `text`)

  json: warnings, the start line, progress records, the request log and the outcome as events, no redrawn line or closing lines.

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

Total what a local tree takes on disk, without crossing into other filesystems, and the files' lengths instead:

```bash
swath du /home --one-file-system
swath du /home --one-file-system -A
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
