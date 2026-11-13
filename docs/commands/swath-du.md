# swath du

swath du totals SOURCE's objects (a local tree's files), bytes and newest time, per prefix down to a depth.

## Synopsis

<!-- synopsis:begin -->
```text
swath du SOURCE [OPTIONS]
swath du --from-dataset DIR [PREFIX] [OPTIONS]
```
<!-- synopsis:end -->

## Description

`swath du` lists SOURCE and prints one total, or with `-d N` the total of each prefix down to N levels below
SOURCE, a parent including its children, as `du` does for directories. It writes no rows and keeps nothing
on disk.

```text
$ swath du -d 1 /data/photos --format table
    size  files  dirs  last_modified (local)  path
 410 KiB      2     1  2026-03-01 10:00:00    2025/
48.8 KiB      1     0  2026-11-02 09:30:00    2026/
 459 KiB      4     3  2026-11-02 09:30:00    (total)
```

A prefix ends at a `/`. Depth counts from SOURCE's prefix up to its last `/`: `s3://b/logs/2026/` counts from
`logs/2026/`, `s3://b/logs` from the top of the bucket. A key with fewer than N slashes counts only in the
prefixes it has. Each row includes everything below it, so add up rows of one depth only, never across depths.

On a terminal the rows are a table with the total last; in a pipe they are TSV, and `--format jsonl` gives
JSONL; both put the total first, at depth 0. The fields, and the care their totals need, are in
[output.md](../output.md#du-rows).

The bytes are the objects' sizes as listed: not the storage consumed, replicas or a bill. A local tree's
directories are counted in `dirs`, never as objects, and their own size is in no total. With `--all-versions`
every version is an object; an S3 delete marker adds its prefixes but no object, bytes or time.

On a local tree, `--allocated` totals the files' allocated blocks (`st_blocks` × 512; a sparse file counts its
blocks, not its length) instead of their apparent size, and the field is then `allocated_size`. A directory's own
blocks are in no total, so a tree's total is below `du -sB1`'s by them. `--allocated` needs each entry's stat, so
`--metadata names` refuses it.

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
    size  files  dirs  last_modified (local)  path
 410 KiB      2     1  2026-03-01 10:00:00    2025/
48.8 KiB      1     0  2026-11-02 09:30:00    2026/
 459 KiB      4     3  2026-11-02 09:30:00    (total)
```

The rows are the ones `swath du` of the dataset's source printed when it was listed, with the same depth, order,
formats and filters. They are read from the dataset's Parquet parts: the key and only the columns the totals
and filters need, in one parallel pass, once every part's columns are checked against the dataset's schema (a
dataset that breaks it is refused, exit 1). Depth counts from the dataset's scope, as it would from the source.
Without `--from-dataset`, `swath du DIR` totals the files in DIR, never the rows of the dataset they hold.

An optional PREFIX after the directory restricts the totals to the keys under it, as the dataset holds them (a
local tree's keys are relative to its root, an object store's are whole keys), and depth counts from PREFIX up to
its last `/`. The read seeks to the prefix rather than scanning the dataset. A PREFIX outside the dataset's scope
is refused, since the dataset cannot total it.

The output says the totals are saved rows. A line on stderr names the dataset, its source, when it was listed
and how far the listing reached (`complete`, `INCOMPLETE` with the reason, the entries it could not read, the
listing's filters and exclusions). `-q` silences that line. In JSONL each row also carries a `dataset` member with
the same facts ([output.md](../output.md#du-rows)). A dataset whose listing did not complete, such as a
`--max-keys` sample, or that left out entries it could not read (`coverage.unreadable`), prints its totals and
exits 3 (`incomplete`), as the run that wrote it did, its last line saying why. A file with several hard links
counts once where the dataset records links (a local tree's, listed with stat), as a live `swath du` counts it;
`-l` counts every name.

`--from-dataset` reads a committed local dataset directory. A dataset in a bucket is refused as not supported
yet. A directory holding an interrupted run is refused with the `swath show` and `swath resume` commands that
handle it. The options that shape a listing are refused by name: provider, credential, performance, limit and
report options, and `--all-versions`, since the dataset's schema decides that. So are `--allocated`,
`--count-links` and `--storage-class` when the dataset's schema has no column for them. The filters, the
totals options, `--format` and the terminal options `-v`, `-q`, `--color`, `--log-format`, `--no-progress`
and `--no-stats` apply. A duration in `--modified-since` counts back from now, not from the listing.

## Options

`swath du` refuses `swath list`'s output options (`-o`, `--overwrite`, `--columns` and the part and budget
sizes) with exit 2.

<!-- flags:begin -->
```text
Totals:
  -d, --depth N               also total each prefix down to N levels below SOURCE (0: the total)
                                A prefix ends at a '/'; each includes what is below it, as du.
      --allocated             local: total the files' allocated bytes instead of their apparent size
                                The files' st_blocks x 512. A directory's own blocks are not
                                counted, so a tree's total is below du's by them.
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
                                Then PREFIX, not SOURCE: an optional key prefix as the dataset's
                                keys hold it, sought rather than scanned. DIR is a local dataset
                                directory; without this option swath du DIR totals DIR's own files.
                                Options that shape a listing are refused. Stderr names the dataset's
                                source, when it was listed and what its listing covered.

Output:
      --format auto|table|tsv|jsonl
                              on stdout: auto is the table on a terminal, TSV otherwise

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
      --include-regex REGEX   keep keys this RE2 regex matches anywhere in the full key (repeatable)
                                The full key, SOURCE's object-store prefix included (a glob matches
                                the key below SOURCE). Any include, glob or regex, keeps a key. ^
                                with a literal prefix narrows the listed prefix as a glob's does.
      --exclude-regex REGEX   drop keys this RE2 regex matches anywhere in the full key (repeatable)
      --min-size SIZE         keep objects (local: entries) of at least this size (1K, 1.5GiB)
                                Every SIZE: bytes, or K, M, G, T and KiB, MiB, GiB, TiB (binary) or
                                KB, MB, GB, TB (decimal), any case.
      --max-size SIZE         keep objects or entries of at most this size
      --modified-since TIME   keep objects or entries modified at or after TIME (2026-01-01, 7d)
                                An offset or Z; UTC without one. A duration (7d, 12h) is that long
                                before the run started, recorded as the time it names.
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
                                establishes a flat one); a hierarchical namespace they report is
                                never overridden and uses tree listing for current blobs. Frozen for
                                resume and shown in the report.

Local:
      --metadata names|stat|statx
                              what is read per entry (statx adds birth_time; Linux) (default stat)
      --no-dirs               omit the directories' rows (kind DIR)
      --no-link-target        do not read symbolic links' targets
      --one-file-system       do not walk into a mount point below the root, as du -x

Performance:
  -j, --workers N             listing workers (default 64; a local tree: 4 per CPU, at most 64)
                                Raise it (128, 256) for a large listing the service does not
                                throttle: listing waits on round trips, not the CPU; lower it when
                                it throttles at the default.
      --seed-from PATH        start from a previous listing's key distribution, not the seed
                                A committed dataset of the same source and scope, or a cut file (one
                                key per line, ascending bytewise, optionally TAB and the keys
                                expected since the previous cut); the engine still splits
                                adaptively.

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
                                while rows go to the same terminal.
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
swath du --from-dataset bucket-inventory logs/2026/ -d 3
```

## Exit status

0 when the totals are printed, also when the reader closes stdout early. 1 when the listing failed or the
prefixes passed `--max-prefixes`, and with `--from-dataset` when the dataset could not be read; 2 for a usage
error; 3 when the totals are printed but are incomplete: entries could not be read, or with `--from-dataset` the
dataset's listing did not cover its scope. An interrupted `swath du` is run again: it keeps no journal. See
[reference/exit-codes.md](../reference/exit-codes.md).

## Environment

`TZ` sets the time zone of the table's times; see also [swath(1)](swath.md#environment) and
[sources.md](../sources.md).

## See also

[swath-list](swath-list.md), [swath-show](swath-show.md), [output.md](../output.md#du-rows),
[filters.md](../filters.md), [sources.md](../sources.md)
