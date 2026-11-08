# CLI

```text
swath list SOURCE -o DIR [flags]        list SOURCE into a sorted Parquet dataset in DIR
swath list SOURCE [-o FILE|-] [flags]   list SOURCE as text rows to a .tsv or .jsonl file (.gz, .zst) or stdout
swath count SOURCE [flags]              count SOURCE's objects and bytes, writing nothing
swath resume DIR [flags]                continue the interrupted run in DIR
swath show [--journal] DIR              show a dataset or an interrupted run
swath diag list|count SOURCE [flags]    list or count with the diagnostic flags (experiments, comparisons)
swath --version                         print the build (tool, module version, VCS revision)
```

`swath --help` prints that summary; `--help` on every command (`swath list --help`, `swath count --help`,
`swath resume --help`, `swath show --help`, `swath diag --help`, `swath diag list --help`) prints its flags and
exits 0.

SOURCE is a local directory, `s3://bucket[/prefix]` or `gs://bucket[/prefix]`. `swath list` writes a dataset to `-o DIR`
([internals/parquet-output.md](internals/parquet-output.md)), text rows to a file or stdout (**Text output**
below). `swath count` only lists and counts, writing nothing (rows are counted as they arrive, never ordered,
buffered or spilled: [internals/ordering.md](internals/ordering.md#counting-without-order)); it takes the source,
filter, performance, limit and reporting flags, and refuses the output ones (`-o`, `--format`, `--overwrite`,
`--part-size`, `--row-budget`, `--spill-budget`, `--encoders`) with exit 2. Each prints one
line (`swath count`: `rows=N object_bytes=B wall=...`), or the run report (`run.json`, below) with `--report-format json`; `--report FILE` also writes that report to
FILE when the attempt ends, on failure too. FILE may not land on managed output (exit 2, checked before the
source is opened, and by `swath resume` before it settles anything): the text output file, the dataset directory's own entries
(`manifest.json`, `data/`, `run.json`, `error.json`, `.swath/` with the lock and journal) or anything below them,
or anything inside another dataset or run of this tool; an ordinary name such as `OUT/report.json` is fine. FILE
is resolved once and written where it was checked: its directory's symbolic links are resolved before a `..`
after them applies (as an open climbs from a link's target), its own name is checked as the entry it is even
while a symbolic link there points elsewhere, and so is where that link leads, a dangling one included, and the output is found by file identity (on a case-insensitive
filesystem `out/` is `OUT/`). Another dataset or run is recognized by a heuristic: a `.swath/lock` or
`.swath/journal`, or a `manifest.json` with this tool's `format` and a `coverage` object; an earlier layout's
marks (`_SUCCESS` beside `data/`, a `schemaId` manifest) and other tools' manifests are not. The
`SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` files are checked the same way and may not be symbolic links
(a profile is opened, never through a link; the report replaces its name by a rename, which replaces a link
there rather than writing its target). `swath resume DIR` replays the command the journal recorded; only
the runtime flags (`--workers`, `--encoders`, `--row-budget`, `--spill-budget`, the
`--part-interval`, the diagnostic `--spill-compress`, `--spill-plain` and `--spill-reserve`, `--report-format`, `--report`, `--max-duration`, `--stall-timeout`, `--seed-from`, and the terminal flags
`--progress`, `--no-progress`, `--progress-interval`, `--stats`, `--no-stats`, `-v`, `-q`, `--color`) and the
operational ones, how requests are
authenticated, timed and admitted (`--profile`, `--anonymous`, `--request-timeout`, `--gcs-anonymous`,
`--gcs-credentials`, `--gcs-request-timeout`, `--limiter`, `--limiter-slow-start`, `--request-rate`,
`--bearer-token-command`, `--bearer-token-refresh-interval`), may be given again (the last value
wins: `--anonymous=false` undoes a recorded `--anonymous`); anything else is a usage error. The source and output
identity stays frozen: a resume whose overrides resolve another route (a profile with another region, say) is
refused by the identity check. `swath resume` on a committed dataset with complete coverage has nothing to do
and exits 0 (`DIR is already complete: nothing to resume`; the report's outcome is `completed`, `complete`
true), so a supervisor may resume until exit 0; one committed with partial coverage stays `incomplete`, exit 1.
`swath show DIR` prints a committed dataset's manifest summary (format, schema, source, scope, coverage, parts,
rows, key range) or an interrupted run's recorded command, frozen identity, committed parts and the key
`swath resume` would start after; `--journal` dumps the journal as JSON lines. `swath diag list` and `swath diag
count` take every `list` (or `count`) flag plus the diagnostic ones (the **Diagnostics** group below); they exist
to run an experiment or a comparison, not to operate the tool, and `swath list` and `swath count` refuse each of
them with exit 2 and a message naming `swath diag`. Each flag has one name; an unknown flag is a usage error
(exit 2). From the Java 0.3 releases, and renames since the unreleased builds of `main`: [upgrade.md](upgrade.md#changes-from-03-to-04). Flags take one or two dashes. The
Java distribution keeps its own `swath` launcher; the two are separate tools.
Every Java CLI option, output mode, exit code and report field against this tool: [cli-inventory.md](cli-inventory.md).
Running it in production (sizing, watching, resuming, reading the output): [operating.md](operating.md).

## Flags

Generated from the flag set, as `--help` prints it, grouped (source and output, performance, limits, output and
reporting; the performance and limits flags say when to change them). `TestCLIDocFlags` fails when this block
is stale (`UPDATE=1 go test ./cmd/swath` rewrites it), and `TestHelpGroupsCoverFlags` when a flag is in no
group:

<!-- flags:begin -->
```text
$ swath list --help
usage: swath list SOURCE [-o DIR|FILE|-] [flags]
  -o DIR writes a sorted Parquet dataset; -o NAME.tsv or NAME.jsonl (optionally .gz or .zst) one text
  file; no -o, or -o -, text rows on stdout
  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]

Source and output:
  -o directory
        output: a dataset directory, a .tsv or .jsonl file (optionally .gz or .zst), or - for stdout (the default)
  --overwrite
        replace a previous dataset in -o
  --format string
        stdout rows: auto, table, tsv or jsonl (auto: the table on a terminal, TSV otherwise); a -o file's name says its format
  --region string
        S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
  --endpoint URL
        S3: endpoint URL of an S3-compatible store (path-style addressing unless --path-style=false)
  --anonymous
        S3: send unsigned requests, ignoring any credentials
  --path-style
        S3: path-style addressing (default: on with --endpoint, as Java's --force-path-style)
  --profile profile
        S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  --request-payer
        S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)
  --bearer-token-command command
        S3: shell command whose stdout is a bearer token, sent instead of SigV4 signatures (GCS's XML API: 'gcloud auth print-access-token'); never recorded, so swath resume needs it again
  --bearer-token-refresh-interval duration
        S3: run --bearer-token-command again after this duration (default 45m)
  --fips
        S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  --dualstack
        S3: use the dual-stack (IPv6) endpoint
  --fetch-owner
        S3: fill owner_id and owner_display_name
  --all-versions
        list every version of every key, not only the current objects: S3 versions and delete markers (ListObjectVersions; needs s3:ListBucketVersions; schema swath/s3-versions/v1), GCS generations, live and noncurrent (versions=true; schema swath/gcs-generations/v1); a key's rows are consecutive and never split across parts
  --gcs-endpoint URL
        GCS: JSON API base URL (default https://storage.googleapis.com/storage/v1/)
  --gcs-anonymous
        GCS: send no credentials, ignoring any in the environment
  --gcs-credentials file
        GCS: service-account or authorized-user JSON file (default: Application Default Credentials)
  --gcs-user-project project
        GCS: bill a requester-pays bucket's requests to this project
  --metadata tier
        local: metadata tier per entry: names (no stat), stat, statx (adds birth_time; Linux) (default stat)
  --no-dirs
        local: omit kind=DIR rows
  --no-link-target
        local: do not read symlink targets
  --include regex
        keep keys in which this regex (Go RE2 syntax) matches anywhere; ^ with a literal prefix also narrows the listed prefix
  --exclude regex
        drop keys in which this regex (Go RE2 syntax) matches anywhere
  --min-size size
        keep objects of at least this size (1k, 256mb, 1.5GiB: binary units)
  --max-size size
        keep objects of at most this size
  --modified-since time
        keep objects modified at or after this time (2026-01-01, 2026-01-01T12:00:00, an offset or Z; UTC without one)
  --modified-until time
        keep objects modified at or before this time
  --storage-class classes
        keep objects in these storage classes (STANDARD,GLACIER; repeatable)
  --seed-from path
        start from a previous listing's key distribution instead of the delimiter seed: a committed dataset path of the same source and scope, or a cut file (one key per line, ascending bytewise, optionally TAB and the keys expected up to it); the engine still splits adaptively

Performance:
  --workers int
        listing goroutines, the most requests in flight (default 64, whatever the CPU count)
        change it: raise it (128, 256) for a large listing the service does not throttle: listing waits on round trips, not the CPU; lower it when it throttles at the default
  --row-budget bytes
        bytes of rows held between listing and encoding (reorder storage and the encoders' queues); not a cap on resident memory (default 134217728)
        change it: raise it when resources.budget_wait_ns is large or spill is heavy and memory allows; lower it on a small host (the peak RSS is a few times it)
  --spill-budget bytes
        cap on spill bytes on disk (0: half the free space on the output filesystem at start, after a reserve of one part per encoder and at least 1 GiB); past it, or at the reserve, listing waits instead of spilling
        change it: set it when the output filesystem is shared or small: the default takes up to half its free space
  --encoders int
        Parquet encoder goroutines, or a .zst text file's compression jobs (default GOMAXPROCS, lowered to what --row-budget and the descriptor limit carry, never below one); an explicit count is kept, with a warning when they cannot carry it
        change it: lower it to leave cores to other work; the default already follows the cores and the row budget
  --part-size bytes
        target bytes per Parquet part (64MiB, 1.5g: see Sizes in docs/cli.md) (default 67108864)
        change it: smaller parts for readers that load whole parts, larger for fewer files on a very large listing
  --part-interval duration
        close an open part with rows after this duration although not full (a rotation target: the part is then encoded and published in order, so the recovery point can be older, and a stalled frontier leaves no part to close); 0: parts close by size only (default 30s)
        change it: lower it to close parts sooner on a slow listing (it bounds how long rows wait in an open part, not how much a resume repeats: a closed part still publishes in order); 0 for part boundaries independent of timing
  --part-rotation-max-rows N
        also close a part at N rows (Java's flag; 0: by size and --part-interval only); part of the resume identity
        change it: set it when readers want parts of a bounded row count; it is frozen for resume

Limits:
  --max-duration duration
        stop the run after this duration (exit 124; a journaled run is resumable); 0: no limit
        change it: give every unattended run one; a stopped object-store run resumes where it stopped
  --stall-timeout duration
        stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
        change it: raise it for a service that is slow to answer for minutes at a time
  --request-rate N
        requests: cap every HTTP attempt (pages, probes, retries, requeues) at N per second, after the limiter's permit; refused when the spacing exceeds half --stall-timeout; 0: no cap
        change it: set it to stay under a request quota shared with other clients
  --limiter
        requests: adaptive concurrency: throttle answers lower the requests in flight (x0.7, at most once per round trip, while 15 % or more of the recent answers are throttles), growth returns one per second while under 7.5 % are; false: no limit: each of the --workers listing workers waits on one page at a time, and seed listings and structure probes run beside them (default true)
        change it: turn it off (with --request-rate) when a run crawls under sustained throttling with the limit near 1
  --request-timeout duration
        S3: per-request timeout; a request that exceeds it is retried (default 30s)
        change it: raise it for a slow S3-compatible store whose pages take longer to answer
  --gcs-request-timeout duration
        GCS: per-request timeout; a request that exceeds it is retried (default 30s)
        change it: raise it for a slow GCS endpoint or emulator

Output and reporting:
  --report file
        also write the run report (JSON, with the outcome and the attempt id) to this file when the attempt ends, on failure too
  --report-format text
        text (one line) or json (the run report, as in run.json) (default text)
  --progress
        print live progress on stderr (default: when stderr is a terminal and neither -q nor -v is given; --progress-interval implies it)
  --no-progress
        no live progress on stderr
  --progress-interval interval
        progress interval (default 1s while the line redraws on a terminal, 30s for appended records; at least 1s)
  --stats
        print the end-of-run statistics on stderr (default: for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe)
  --no-stats
        no end-of-run statistics on stderr
  -v, -vv, -vvv
        verbose: a start line, and progress as appended records (-vv, -vvv: the same)
  -q, -qq
        quiet: no warnings, no automatic progress or statistics (-qq: no error line either; the exit code and the reports stand)
  --color string
        colour the progress line and the statistics: auto, always or never (auto: a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it) (default auto)

swath resume DIR continues an interrupted run with its recorded command; only
workers, encoders, row-budget, spill-budget, spill-reserve, spill-compress, spill-plain, part-interval, report-format, report, max-duration, stall-timeout, seed-from, progress, no-progress, progress-interval, stats, no-stats, v, vv, vvv, q, qq, color (runtime) and
profile, anonymous, request-timeout, gcs-anonymous, gcs-credentials, gcs-request-timeout, limiter, limiter-slow-start, request-rate, bearer-token-command, bearer-token-refresh-interval (operational) may change.
Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.

$ swath count --help
usage: swath count SOURCE [flags]
  lists SOURCE and counts its rows and object bytes, writing nothing: rows are counted as they
  arrive, never ordered, buffered or spilled; prints one line, or the run report
  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]

Source and output:
  --region string
        S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
  --endpoint URL
        S3: endpoint URL of an S3-compatible store (path-style addressing unless --path-style=false)
  --anonymous
        S3: send unsigned requests, ignoring any credentials
  --path-style
        S3: path-style addressing (default: on with --endpoint, as Java's --force-path-style)
  --profile profile
        S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  --request-payer
        S3: accept the request charges of a requester-pays bucket (x-amz-request-payer: requester)
  --bearer-token-command command
        S3: shell command whose stdout is a bearer token, sent instead of SigV4 signatures (GCS's XML API: 'gcloud auth print-access-token'); never recorded, so swath resume needs it again
  --bearer-token-refresh-interval duration
        S3: run --bearer-token-command again after this duration (default 45m)
  --fips
        S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  --dualstack
        S3: use the dual-stack (IPv6) endpoint
  --fetch-owner
        S3: fill owner_id and owner_display_name
  --all-versions
        list every version of every key, not only the current objects: S3 versions and delete markers (ListObjectVersions; needs s3:ListBucketVersions; schema swath/s3-versions/v1), GCS generations, live and noncurrent (versions=true; schema swath/gcs-generations/v1); a key's rows are consecutive and never split across parts
  --gcs-endpoint URL
        GCS: JSON API base URL (default https://storage.googleapis.com/storage/v1/)
  --gcs-anonymous
        GCS: send no credentials, ignoring any in the environment
  --gcs-credentials file
        GCS: service-account or authorized-user JSON file (default: Application Default Credentials)
  --gcs-user-project project
        GCS: bill a requester-pays bucket's requests to this project
  --metadata tier
        local: metadata tier per entry: names (no stat), stat, statx (adds birth_time; Linux) (default stat)
  --no-dirs
        local: omit kind=DIR rows
  --no-link-target
        local: do not read symlink targets
  --include regex
        keep keys in which this regex (Go RE2 syntax) matches anywhere; ^ with a literal prefix also narrows the listed prefix
  --exclude regex
        drop keys in which this regex (Go RE2 syntax) matches anywhere
  --min-size size
        keep objects of at least this size (1k, 256mb, 1.5GiB: binary units)
  --max-size size
        keep objects of at most this size
  --modified-since time
        keep objects modified at or after this time (2026-01-01, 2026-01-01T12:00:00, an offset or Z; UTC without one)
  --modified-until time
        keep objects modified at or before this time
  --storage-class classes
        keep objects in these storage classes (STANDARD,GLACIER; repeatable)
  --seed-from path
        start from a previous listing's key distribution instead of the delimiter seed: a committed dataset path of the same source and scope, or a cut file (one key per line, ascending bytewise, optionally TAB and the keys expected up to it); the engine still splits adaptively

Performance:
  --workers int
        listing goroutines, the most requests in flight (default 64, whatever the CPU count)
        change it: raise it (128, 256) for a large listing the service does not throttle: listing waits on round trips, not the CPU; lower it when it throttles at the default

Limits:
  --max-duration duration
        stop the run after this duration (exit 124; a journaled run is resumable); 0: no limit
        change it: give every unattended run one; a stopped object-store run resumes where it stopped
  --stall-timeout duration
        stop the run as stuck after this long without progress (exit 75; resumable); 0: never (default 10m0s)
        change it: raise it for a service that is slow to answer for minutes at a time
  --request-rate N
        requests: cap every HTTP attempt (pages, probes, retries, requeues) at N per second, after the limiter's permit; refused when the spacing exceeds half --stall-timeout; 0: no cap
        change it: set it to stay under a request quota shared with other clients
  --limiter
        requests: adaptive concurrency: throttle answers lower the requests in flight (x0.7, at most once per round trip, while 15 % or more of the recent answers are throttles), growth returns one per second while under 7.5 % are; false: no limit: each of the --workers listing workers waits on one page at a time, and seed listings and structure probes run beside them (default true)
        change it: turn it off (with --request-rate) when a run crawls under sustained throttling with the limit near 1
  --request-timeout duration
        S3: per-request timeout; a request that exceeds it is retried (default 30s)
        change it: raise it for a slow S3-compatible store whose pages take longer to answer
  --gcs-request-timeout duration
        GCS: per-request timeout; a request that exceeds it is retried (default 30s)
        change it: raise it for a slow GCS endpoint or emulator

Output and reporting:
  --report file
        also write the run report (JSON, with the outcome and the attempt id) to this file when the attempt ends, on failure too
  --report-format text
        text (one line) or json (the run report, as in run.json) (default text)
  --progress
        print live progress on stderr (default: when stderr is a terminal and neither -q nor -v is given; --progress-interval implies it)
  --no-progress
        no live progress on stderr
  --progress-interval interval
        progress interval (default 1s while the line redraws on a terminal, 30s for appended records; at least 1s)
  --stats
        print the end-of-run statistics on stderr (default: for runs over 1.5 s, runs that write output, and runs that do not complete; not with -q, nor after a closed downstream pipe)
  --no-stats
        no end-of-run statistics on stderr
  -v, -vv, -vvv
        verbose: a start line, and progress as appended records (-vv, -vvv: the same)
  -q, -qq
        quiet: no warnings, no automatic progress or statistics (-qq: no error line either; the exit code and the reports stand)
  --color string
        colour the progress line and the statistics: auto, always or never (auto: a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE forces it) (default auto)

swath resume DIR continues an interrupted run with its recorded command; only
workers, encoders, row-budget, spill-budget, spill-reserve, spill-compress, spill-plain, part-interval, report-format, report, max-duration, stall-timeout, seed-from, progress, no-progress, progress-interval, stats, no-stats, v, vv, vvv, q, qq, color (runtime) and
profile, anonymous, request-timeout, gcs-anonymous, gcs-credentials, gcs-request-timeout, limiter, limiter-slow-start, request-rate, bearer-token-command, bearer-token-refresh-interval (operational) may change.
Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.

$ swath diag list --help
usage: swath diag list SOURCE [flags]
  every swath list flag (swath list --help), and the diagnostic flags below:
  they exist to run an experiment or a comparison, not to operate the tool
  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]

Diagnostics:
  --seed
        ranges: tile the keyspace from delimiter listings of the folder hierarchy before splitting (the top level up to 4 pages, narrow levels descended up to 4 deep: 1 to 6 serial round trips before the first range lists, the first carrying the first data page, which lists a scope of one page alone; not with -sequential); false: start from one range (default true)
  --structure
        ranges: structure probes (delimiter pages of 32 alongside a range's pages; a split pivots on the folder boundaries they list), written --structure=auto|on|off with '=' (the bare flag is on, so a separate word is the next argument): auto probes on the open frontier while its cursor is in a folder and in bounded ranges whose chain stays inside one folder; on (or true) probes every range in a folder; off (or false) none (default auto)
  --slow-split
        ranges: rank ranges by time left (keys left x their page latency) and split a range without an idle worker when its pages take at least 4 times the run's mean latency and its time left is at least twice the running ranges' mean (one such split waiting at a time)
  --floors
        ranges: the gate-chain floors: never split a range with at most 4 pages of keys left, nor leave the child past the pivot below 2 pages
  --sequential
        ranges: list with one continuation-token chain (no ranges; the reference)
  --max-keys N
        ranges: with -sequential, stop after N objects (experiments; the dataset is marked incomplete)
  --page-size N
        ranges: ask the service for pages of N entries (1 to 1000; default 1000): small pages exercise page seams, splits inside a version history and the probes
  --limiter-slow-start
        requests: with -limiter, start at 4 requests in flight and double each second up to --workers until the first cut or timeout (Java swath's slow start); applies to swath resume too
  --spill-plain
        write spill frames without delta coding (each key and payload whole; a comparison arm)
  --spill-compress
        compress spill frames (zstd level 1); --spill-compress=false writes them uncompressed (default true)
  --spill-reserve bytes
        free bytes spill leaves on its filesystem (0: derived, one part per encoder and at least 1 GiB)
  --no-spill
        wait on the reorder budget instead of spilling rows ahead of the frontier to disk
  --no-journal
        publish parts only at the end, without the journal that swath resume needs
```
<!-- flags:end -->

## Semantics worth stating

- **Sizes** (`--part-size`, `--row-budget`, `--spill-budget`, `--spill-reserve`, `--min-size`, `--max-size`) have
  one syntax, Java's: a decimal number and an optional unit, `b`, `k`/`kb`/`kib`, `m`/`mb`/`mib`, `g`/`gb`/`gib`
  or `t`/`tb`/`tib`, in any case and all binary (`64MB` is 64 MiB), coming to a whole number of bytes (`1.5g`,
  not `1.3b`).
- **Provider flags** (`S3:` and `GCS:` in the list, and the range-engine flags both object stores share) apply
  to their own sources only: one given for another source (`gs://... --anonymous`, where GCS takes
  `--gcs-anonymous`; `--endpoint` against `--gcs-endpoint`; an S3 flag on a local tree) is refused (exit 2)
  rather than ignored.
- **Durations** are Go durations (`90s`, `1h30m`). A negative one is refused (exit 2) on every duration flag,
  by `swath list` and by `swath resume` before it settles anything;
  `0` turns off `--max-duration`, `--stall-timeout` and `--part-interval`'s time cut.
- **`--row-budget`** bounds the bytes of rows and scheduler metadata in flight (reorder storage and encoder
  queues); it is not a cap on resident memory. See [internals/ordering.md](internals/ordering.md).
- **`-o DIR`** is refused when non-empty unless `--overwrite`, which replaces only
  a previous dataset of this tool and never anything inside the listed root.
- **Text output** (S3 and GCS sources; a local tree's keys are bytes, which JSON cannot carry, so it needs the
  dataset: `swath list DIR` without `-o DIR`, or with a text `-o`, is refused with exit 2, naming `-o DIR` and `swath count`). The `-o` name decides: `NAME.tsv` or `NAME.jsonl`, optionally `.gz` or `.zst`, is one file in that
  format and compression; any other name is the Parquet dataset; no `-o`, or `-o -`, is stdout, where `--format
  auto|table|tsv|jsonl` chooses (`auto`, the default: the table on a terminal, TSV otherwise) and a pipe
  compresses (`| zstd`). `--format` with a `-o` file or directory is refused, because the name already says it.
  Rows are the S3 API's object (ListObjectsV2 `Contents`), so they are read with what already reads S3 listings:
  - JSONL, one object per line, the API's names in its order: `Key`, `LastModified`, `ETag`,
    `ChecksumAlgorithm` (a list), `ChecksumType`, `Size`, `StorageClass`, `Owner` (`DisplayName`, `ID`; only with
    `--fetch-owner`). A field without a value, or one the provider lacks (GCS has no checksum or owner fields), is
    left out, not null. A line is byte for byte what `aws s3api list-objects-v2 ... | jq -c '.Contents[]'`
    prints for the object: `ETag` keeps its quotes (the dataset stores it bare), `LastModified` is printed as the
    AWS CLI does (UTC, `+00:00`, a microsecond fraction only when not zero), strings are escaped as jq does.

    ```text
    {"Key":"logs/2023/app.log","LastModified":"2023-11-14T22:13:20+00:00","ETag":"\"fba9dede5f27731c9771645a39863328\"","ChecksumAlgorithm":["CRC64NVME"],"ChecksumType":"FULL_OBJECT","Size":434234,"StorageClass":"STANDARD"}
    ```
  - TSV: the same fields as columns in the same order under one header line, a column for each field the
    provider has (S3: all nine, `Owner.DisplayName` and `Owner.ID` flattened; GCS: `Key LastModified ETag Size
    StorageClass`), an absent value empty, `ChecksumAlgorithm` its one value.

    ```text
    Key	LastModified	ETag	ChecksumAlgorithm	ChecksumType	Size	StorageClass	Owner.DisplayName	Owner.ID
    logs/2023/app.log	2023-11-14T22:13:20+00:00	"fba9dede5f27731c9771645a39863328"	CRC64NVME	FULL_OBJECT	434234	STANDARD		
    ```
  - The table: the `aws s3 ls --recursive` line, the time to the second, the size right-aligned in 10 columns,
    the key, no header; in UTC (aws prints local time), so two runs' rows compare equal wherever they ran.

    ```text
    2023-11-14 22:13:20     434234 logs/2023/app.log
    ```

  One escaping rule for all three, JSON's string escapes: `\\`, `\b`, `\f`, `\n`, `\r`, `\t`, any other byte
  below 0x20 and 0x7f as `\u00xx`, and `\"` inside JSON only. It is reversible, keeps a row on one line and a
  TSV field between its tabs, and never sends a terminal a control byte. Other provider columns (GCS
  `generation`, `md5`, `crc32c`, `content_type`, `time_created`) have no S3 field and are not written: they are
  in the dataset.
  A file is written to a temporary beside it and installed only when complete (synced, then renamed, the
  directory synced), replacing a previous file; a failed run leaves the destination as it was. A reader that closes
  stdout (`| head`) ends the run at once with exit 0, the report's outcome `stopped`, `stop_reason`
  `downstream_closed`, `complete` false. On stdout the one-line summary is not printed (stdout carries the
  rows; `--stats` prints the account on stderr) and `--report-format json` is refused (use `--report FILE`).
  Text runs keep no journal: `swath resume` refuses a text destination, and an interrupted text run is run
  again. `output.text` in the report names the format, destination, compression, rows and bytes.
- **Filters** (Java's seven): `--include REGEX` and `--exclude REGEX` keep or drop keys in which the expression
  matches anywhere (a substring search, as Java's `find`), `--min-size` and `--max-size` keep sizes within the
  inclusive bounds (Java's units: `1k`, `256mb`, `1.5GiB`, binary, case-insensitive), `--modified-since` and
  `--modified-until` keep modification times within the inclusive bounds (`2026-01-01`, `2026-01-01T12:00:00`
  or with `Z` or an offset; UTC without one; a row without a time is kept, as Java's), `--storage-class
  A,B` (repeatable) keeps the listed classes (case-insensitive). They reduce the rows written, not the requests
  made, with one exception: an `--include` anchored at the start with a literal (`^logs/2026/`) narrows the listed
  prefix to it (the same rows, fewer requests; the report's `invocation.source` names the narrowed root). The
  dropped rows are counted in `output.rows_filtered` (`rows_emitted` is what passed; `rows_fetched` both). The filters are part of the
  resume identity (and of `args_hash`): a resume keeps them and refuses others. A filtered dataset says so in its
  manifest: `coverage.filters` holds Java's descriptions (`include=…`, `exclude=…`, `min_size=…`, `max_size=…`,
  `modified_after=…`, `modified_before=…`, `storage_class=…`), and a reader that needs the whole source refuses a
  dataset whose `coverage.filters` is not empty ([upgrade.md](upgrade.md)). **Regular expressions are Go's RE2,
  not Java's:** the common syntax is the same, but backreferences (`\1`), lookahead and lookbehind, possessive
  quantifiers and atomic groups do not exist and are refused (exit 2); `\Z` is not supported (`\z` is), and
  some character-class and Unicode-property details differ. RE2 runs in time linear in the key, so no pattern
  can stall a listing.
- **S3 modes.** The default is the parallel core, seeded: before any range lists, delimiter listings of the
  folder hierarchy (the top level up to 4 pages, levels narrower than `--workers` descended up to 4 deep, at
  most 64 requests in at most 6 serial waves) tile the scope, or a resumed scope after its last published key.
  The first wave also carries the scope's first data page: a complete page (the whole scope) lists a bucket of
  one page in one round trip; a truncated one is discarded, and the first tile starts at the scope's start (or
  the resumed key) as without it; a delimiter chain whose page held no folder is not
  followed ([internals/providers.md](internals/providers.md#range-engine-internalsourcekeyrange));
  `swath diag list --seed=false` starts from one
  range instead (an experiment: [measurements](measurements.md) has the seed's effect on split depth);
  `swath diag list --sequential` is the one-chain oracle and the only mode where `--max-keys` stops early (the
  dataset is then marked incomplete; such a run keeps no journal and cannot be resumed). `--endpoint` selects an S3-compatible store, addressed path-style by default, as Java's
  `--force-path-style` default with a custom endpoint; `--path-style=false` uses virtual-host addressing there. S3 directory buckets (Express One Zone, names ending in
  `--x-s3`) are refused before any request (exit 2, class `refused`).
- **Warm start:** `--seed-from PATH` (S3 and GCS) starts the range engine from a previous listing's key
  distribution instead of the delimiter seed: no seed request, and the scope tiled into up to
  `min(1000, 4 × --workers)` ranges of about equal expected keys. PATH is either a committed dataset directory of
  the same source and scope (the manifest's bucket and prefix must be the run's; its parts' bounds, row counts and
  key page index give the cuts) or a cut file: one key per line, ascending bytewise without repeats, each inside the
  scope, optionally followed by a tab and the keys expected since the previous cut (every line or none), at most
  262,144 lines and 16 MiB (thin a larger one: every nth line, its count the sum of those it replaces). Anything
  else is refused before any request (exit 2, class `refused`): another bucket or prefix, an unsorted or repeated
  cut, a cut outside the scope, a cut file past those bounds, the output directory itself, `--sequential`, a filesystem source. Production should
  seed from the previous dataset, which carries identity and counts; a cut file is for priors from elsewhere (an
  S3 Inventory, a prefix list, another tool's segments). A prior changes only how the work is first divided: the
  splits stay adaptive, the rows are the same. It is a runtime flag: `swath resume` reads it again and may change
  or drop it (`--seed-from=`); a resume whose prior is unreadable warns and lists without it. Counters
  `seed.from.*`; the report's `invocation.prior` ([internals/providers.md](internals/providers.md)).
- **Workers:** `--workers` (default 64 whatever the CPU count, Java's default; listing waits on the service,
  not the CPU) is the listing goroutines and the request limiter's start; `--encoders` stays CPU-derived.
- **Request admission.** By default (S3 and GCS) every request takes a permit from an adaptive limit that
  starts at `--workers`, falls by 0.7 on a throttle answer, at most once per round trip and only while 15 % or
  more of the last 4 × `--workers` answers (at least 256) are throttles, and grows back by one per second while under 7.5 % are
  or after 10 s without one (Java swath's rules, read as a fraction so that a random background of throttles
  is not taken for congestion; [internals/providers.md](internals/providers.md)); `--limiter=false` admits
  every request at once: no limit applies, the `--workers` listing workers each wait on one page at a time, and
  the seed's delimiter listings and the structure probes run beside them, so more than `--workers` requests can
  be in flight. `swath diag list --limiter-slow-start` (off by default) starts the limit at 4 instead
  and doubles it each second up to `--workers` until the first cut or timeout (Java's slow start);
  a resume replays it from the recorded command and may add or undo it, since a resumed client starts a new
  limiter (counters `slow_start.*`, feature `slow_start`, on only with the limiter). `--request-rate N` (S3 and GCS, default 0: none) also caps the run's
  HTTP attempts at N per second (at least 0.001), every attempt counted (pages, probes, client retries, engine
  requeues): a token bucket of one token, taken after the limiter's permit, so the k-th token of a burst is due
  (k-1)/N seconds after the first, as with Java's `--request-rate`. Tokens are scheduled, not the sends: a caller
  delayed after its token can send together with the next, so the cap holds over a window but sends are not
  strictly spaced. A quota shared across a fleet is the use. Counters `rate.waits`, `rate.wait_ns`.
- **Requester pays:** `--request-payer` sends `x-amz-request-payer: requester` (signed) on every request, so
  a requester-pays bucket can be listed at the caller's charge.
- **S3 credentials:** `--anonymous` sends unsigned requests whatever the environment holds. Otherwise the AWS
  SDK's default credential chain applies: environment, shared files and profiles (`--profile`), SSO, web
  identity, credential processes, AssumeRole, container and instance roles. Requests go unsigned only when
  the chain is empty (the environment and the shared files name no source, and the machine shows no sign of
  EC2) and the lookup finds nothing; a configured source that fails stops the run with cause `auth`, and one
  that is slow or failing transiently is waited for and retried, never replaced by anonymity. Secrets are never taken from flags (they would show in `ps`). Details:
  [internals/providers.md](internals/providers.md).
- **Bearer tokens** (S3 client, Java's flags): `--bearer-token-command CMD` runs `CMD` with `/bin/sh -c`, or on
  Windows with `cmd.exe /d /s /c "CMD"` (`%ComSpec%`, so write it in cmd syntax there); its
  stdout (trimmed, one line) is a bearer token sent as `Authorization: Bearer TOKEN` on every request instead of a
  SigV4 signature, and no AWS credential is looked up (it excludes `--anonymous` and `--profile`). The use is an
  S3-compatible endpoint that takes OAuth tokens, such as GCS's XML API: `swath list s3://bucket --endpoint
  https://storage.googleapis.com --bearer-token-command 'gcloud auth print-access-token' -o DIR` (the native
  `gs://` provider is the alternative). The token is fetched again after `--bearer-token-refresh-interval`
  (default 45 m, under a one-hour token lifetime), one run of the command at a time however many requests need
  it; a command that exits non-zero, runs past 30 s, prints nothing or more than one line fails the request
  with cause `auth` (fail closed: a stale token is never reused); its message carries the exit code and up to
  4,096 characters of its stderr, control characters escaped. The command is never recorded: the journal,
  `run.json`, the report, `error.json` and the stderr outcome line show `--bearer-token-command=[redacted]`, so
  `swath resume DIR` must be given it again (`swath resume DIR --bearer-token-command CMD`) and refuses with a
  message saying so otherwise (exit 2).
- **Spill:** when the reorder budget is full, rows of ranges ahead of the frontier go to disposable files
  under `DIR/.swath/spill/` (a text file's in a scratch directory of its attempt beside it,
  `.NAME.swath-ATTEMPT-RANDOM/spill/`, removed when the attempt ends; stdout's under `$TMPDIR`) instead of
  stalling the workers. `--spill-budget` is the one disk budget: spill stops
  (and listing waits instead) at that many bytes on disk (default 0: half the space free on the spill
  filesystem when the run starts, after the reserve) or when the filesystem would drop below the reserve, one
  part per encoder at full size and at least 1 GiB (room for the parts being written on the same filesystem).
  `swath diag list --no-spill` turns spill off and `--spill-reserve` sets the reserve; `--spill-compress=false`
  writes the frames uncompressed (by default zstd level 1 runs over the delta-coded frames); `--spill-plain` writes each key and payload whole
  instead of delta-coding them, the comparison arm
  ([internals/ordering.md](internals/ordering.md)).
- **Encoders:** the default count is GOMAXPROCS lowered to what the resources carry: one writer's working set
  (24 MiB) each from an allowance equal to `--row-budget`, apart from the rows, and the descriptor limit (after the spill readers,
  the workers' connections and a reserve), never fewer than one. An explicit `--encoders N` is kept as given;
  when the resources cannot carry it, a warning names the shortfall. For a `.zst` text file the same flag
  is the zstd jobs compressed at once, 16 MiB each from that allowance (no descriptor each); one job is a
  synchronous encoder ([operating.md](operating.md#sizing-a-run), Memory).
- **Row bound per part:** `--part-rotation-max-rows N` (default 0: off; Java's default was 2,000,000 with
  256 MB parts) also closes a part at N rows. The cut depends on the rows only, so it is part of the resume
  identity, a resume reproduces the boundaries, and a row-cut part does not calibrate the size ratio (only a part
  closed by size does); `resources.part_row_cuts` counts them.
- **Part limit:** a dataset has at most 100,000 parts (`part-00000` to `part-99999`: ordinals have five digits, `dataset.MaxParts`); at the defaults (64 MiB parts, about 30 bytes per row) it takes about 200 billion objects or 35 days of uninterrupted 30-second time cuts; in practice a small `--part-size` or `--part-rotation-max-rows` reaches it. A run that would open part 100,000 first publishes every earlier part, then
  stops with exit 1, class `fatal`, `cause` `part_limit`, not resumable: the part size and row cap are frozen in
  the resume identity, so a resume would stop at the same place. Start over (`--overwrite`) with a larger
  `--part-size` or without `--part-rotation-max-rows`.
- **Durable-progress age:** `--part-interval` (default 30 s, Java's rotation interval; 0: off) closes an open part that holds rows once
  it is that old, although not full (checked every eighth of the interval; not a part whose boundary a resume
  fixed). It is a rotation target, not a durability deadline: the closed part still encodes, syncs and is
  published in ordinal order behind the parts before it, so the journal's recovery point (what a resume keeps)
  lags by that work too, and while no rows reach the output no part is open to close, so the point does not
  move. Rows listed but not yet in a published part are listed again by a resume. With 0, parts close by
  `--part-size` only and their boundaries do not depend on timing. It may change on `swath resume`.
- **Resume:** an S3 run keeps a journal in `DIR/.swath/` with the command that started it; after a crash or
  interruption, `swath resume DIR` continues it ([internals/durability.md](internals/durability.md)).
  `swath diag list --no-journal` turns it off (parts are then published only at the end).
- **Exit status and outcome.** A supervisor's outcome of record is the exit code plus the report at the path
  it names with `--report` (outside the output directory, a fresh path per attempt, or removed before the
  attempt starts): `outcome` (`class`, `exit_code`, `resumable`, `cause`, `unrecorded`; `status`, `complete` and
  `stop_reason` follow the class: a run that did not commit is `complete` false with its class as the
  `stop_reason`, and a committed run's agree with the manifest's `coverage`) and `attempt` (`id`,
  random, and `started`). For a run that did not complete, `DIR/error.json` holds the same outcome in the
  dataset (`class`, `exit_code`, `message`, `resumable`, `cause`: the provider's failure class such as
  `throttle` or `access_denied`, or `no_space` for a full disk; and the same `attempt`); it never needs stderr.
  The command classifies the outcome once, before any account is written, and that classification is frozen:
  `DIR/run.json` (a committed dataset), `error.json`, the report, the exit code and a forced exit's outcome agree
  on `class`, `exit_code`, `resumable` and `cause`, also when a stop cause (the deadline, a signal) arrives
  after the run settled (a dataset committed with partial coverage is `incomplete`, exit 1, in all of them).
  The report's `output` counts the parts that stay in the output on failure too: after a failure of a
  journaled run, `parts_written`, `output_bytes` and `rows_durable` are the parts the journal acknowledged (the append
  returned, a failed rename after it included: the resume renames them), the recovered ones included in
  `output_bytes` and `rows_durable`.
  `error.json` is a convenience: it is current only when its `attempt.id` is the report's. No program can
  promise a new file on storage that refuses writes, so an attempt whose `error.json` write fails leaves the
  previous one in place, possibly with the same class and exit code; its report names another attempt and says
  why in `outcome.unrecorded`. With no report (it could not be written either; a usage error, exit 2, before
  the flags were read), only the exit code is current. `error.json` and the
  progress file are written only while the run holds the output lock, and `error.json` is removed when a later
  run or resume completes. `error.json` is always the last attempt's outcome: a resume (or `--overwrite`) that
  fails before it runs (credentials that do not load, a damaged journal, a failed recovery) takes the lock and
  records its own outcome over the interrupted run's, if that run is still the one it read (its journal and
  `error.json` unchanged) and the dataset still uncommitted once it holds the lock. An attempt that cannot
  record it (another process owns the output, whose outcome stands; another process completed or replaced the
  run first; a refusal that leaves the directory as it was, such as a directory without a
  journal, where nothing is created, or an unusable output; a failed write) prints it on stderr as one line, `swath: outcome: {...}`, after the
  reason; the JSON report carries the reason as `outcome.unrecorded`, which is never empty when the outcome is
  not in `error.json`.

  | Code | Class | Meaning | Resumable |
  |---|---|---|---|
  | 0 | `completed` | the dataset is committed (or `swath count` finished), also by an earlier attempt: `swath resume` on a complete dataset | – |
  | 1 | `fatal`, `incomplete` | a run failure a resume will not fix (access denied, no such bucket, an invariant, another process owning the output; a protocol violation, `cause` `protocol`, which outranks a stop cause and which the journal records, so `swath resume` refuses the run; the part limit, `cause` `part_limit`, and an output layout swath did not make (a linked `data/`, workspace, journal or part), `cause` `unsafe_layout`, which both also outrank a stop cause; the refused paths are left as they were; the outcome is in the report, and in `error.json` only when swath could take the output lock to settle it: a refused linked workspace or journal can leave `error.json` unchanged, and the external `--report` carries the refusal), or a dataset committed with partial coverage (a key limit), so a partial dataset never exits 0 | no |
  | 2 | `usage`, `refused` | `usage`: a malformed command line or an unknown scheme; `refused`: a refusal that is the caller's to fix, before anything ran: a non-empty `-o` directory without `--overwrite`, `--overwrite` on a directory that is not this tool's dataset, a new run over an interrupted one, an output that is the listed root (or inside it, replaced), a resume whose identity changed, a resume with nothing to resume, an S3 directory bucket | `usage`: no; `refused`: the output is as it was, so yes when it holds an interrupted run |
  | 74 | `disk_full` | a part, a journal record or the manifest could not be written: its filesystem is full (`cause` `no_space`; spill waits instead of failing) | yes, journal intact, once there is space |
  | 75 | `stuck` | no progress (Liveness, below) for `--stall-timeout` (default 10 min; a warning at half; also after the 15 s shutdown grace); `cause` names what rose meanwhile: `stuck_empty_chain` (empty continuing pages past the first 1,024 of their run), `stuck_throttle` (throttle answers), `stuck_timeouts` (attempts past the request timeout, no throttle) or `stuck_no_rows` (none of these: failing or hung requests, or a stopped pipeline) | yes, journal intact |
  | 124 | `deadline` | `--max-duration` reached with the output unfinished (also after the 15 s shutdown grace) | yes |
  | 130 | `interrupted` | SIGINT (also after the 15 s shutdown grace) | yes |
  | 143 | `terminated` | SIGTERM, a graceful stop (also after the 15 s shutdown grace) | yes |

  "Resumable" holds when the output has an intact journal (S3); a filesystem run starts over. A source that
  stops early (a key limit) never exits 0 with a partial manifest unless its coverage says so.
- **Liveness.** The command alone decides that a run cannot go on: its watchdog watches the run's progress
  in key space (rows listed, ordered, encoded, parts published, and the listing's pages: a page that held an
  entry or ended a chain, probes included) and stops the run as stuck after `--stall-timeout` without a rise.
  So work queued behind a low request limit is alive while the service answers pages; a source that hangs, or
  only fails or throttles, is stuck. An empty continuing page (S3 and GCS may return them) counts only within
  the first 1,024 of its run (`keyrange.EmptyChainLive`, the token window below): a service that answers
  forever with empty pages and fresh tokens stops as stuck, `cause` `stuck_empty_chain`, about 1,024 pages
  plus `--stall-timeout` after the run of them began, without `--max-duration`. A legitimate run of empty
  pages longer than that is still followed while anything else progresses, and is stopped only if nothing
  does for the stall timeout. A source without the range engine (a supplied client) is live by its answered
  requests instead. Nothing in the pipeline gives up on a slow source (S3 requeues a page after its retries).
  With the watchdog off (`--stall-timeout 0`), only `--max-duration` bounds such a service. A run of empty
  continuing pages must not repeat any of its last 1,024 tokens (an invalid page: exit 1, `cause` `protocol`,
  the run marked unresumable); a cycle longer than that window is not detected, and is bounded as above. A stop (a signal, `--max-duration`, `--stall-timeout`) is graceful, and the process
  exits with its code 15 s later if the run has not stopped by then (I/O that does not return, say), first
  writing (bounded at 2 s, as the dump below) the same outcome the graceful stop would have to `error.json` and a
  minimal report without the run's statistics, its `message` naming the forced exit. The exit does not wait for
  any of that beyond 5 s after the grace (20 s after the stop in all): each line to stderr waits at most 250 ms
  and is dropped past it, so a stderr pipe nobody drains cannot hold the process (warnings are written the same
  way), and the process exits at that bound whatever still blocks. **Forensics:** at a stall, before the run is cancelled, and at a forced
  exit, every goroutine's stack and the progress counts are written whole to
  `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt` (replaced each time) while the run owns the output,
  else, or when that write fails or blocks for 2 s, to stderr, cut at 1 MiB.
- **What a hosted runner needs.** A finite `--max-duration` on every attempt (the default, 0, has no limit:
  the watchdog stops a service that answers forever with empty pages, but not one that keeps returning rows
  or pages forever, nor anything with `--stall-timeout 0`), and its own process deadline above
  `--max-duration` plus 20 s, which kills the process (that attempt's report may then be missing: it is treated
  as unavailable, never replaced by an older `error.json`). Exit 75 is retried with `swath resume`. Exit 74:
  retry with a larger workspace (`swath resume` on the same output, moved to a larger disk or given space). Exit 124 is a
  bounded stop: the run is resumable but did not finish in the time given, so it is retried only under a
  separate budget (a count of attempts or a total time), and past that budget it needs a person: a source that
  keeps the run alive without finishing hits 124 on every attempt. Exit 75 with `cause` `stuck_empty_chain` on
  every attempt is the same: the service no longer moves past a point in the key space.
- **Progress file.** While a run goes, `DIR/.swath/progress.json` is rewritten every 2 s by an atomic rename
  (the terminal display, `--progress`, samples the same snapshot, every second while it redraws)
  (while the run holds the output). This is its one schema (`listing.ProgressSnapshot`; `TestProgressSchemaDoc`
  keeps the two in step):

  | Field | Meaning |
  |---|---|
  | `objects` | rows listed so far |
  | `ordered`, `encoded` | rows handed to the encoders, rows written into parts |
  | `parts` | parts published |
  | `verified_bytes` | a resume's bytes of retained parts checked so far |
  | `advance` | the sum of the above; rises in every phase |
  | `answered` | requests the service answered usably (a sign the service is up, not progress) |
  | `frontier` | the listing's pages: those that held an entry or ended a chain, and empty continuing pages within the first 1,024 of their run (the watchdog's liveness with `advance`) |
  | `empty_chain_pages` | empty continuing pages past the first 1,024 of their run: answered, followed, not progress |
  | `throttles`, `timeouts` | throttle answers; attempts past the request timeout |
  | `part_age_ms` | time since the last part was published (or since the start): publication age, not the age of the rows a resume would list again |
  | `mutex_wait_ms`, `mutex_max_hold_ms` | the range engine's decision mutex: summed wait, longest hold |
  | `rows_retained` | rows a resume kept from the interrupted run (not in `objects`) |
  | `requests`, `retries` | HTTP attempts sent (pages, probes, client retries, engine requeues); attempts repeated (S3, GCS) |
  | `inflight`, `limit` | attempts in flight now; the adaptive limiter's current limit (0 without one) |
  | `cost_usd`, `cost_rate_per_1k_usd` | `requests` at the service's reference list rate, and that rate (absent on a custom endpoint) |
  | `order.nodes`, `order.markers`, `order.spill_readers`, `order.spill_live_bytes`, `order.ahead_rows` | the order stage's live resources, each with a `peak_` twin (`peak_spill_live_bytes` for spill bytes); `ahead_rows` is rows listed and not yet handed on (in memory or spilled) |
  | `started_at`, `as_of`, `duration_ms` | timing |

  A supervisor watches that file; `DIR/run.json` follows the
  commit. `--max-duration` and `--stall-timeout` are runtime flags: `swath resume` replays the recorded
  values unless given again.
- **Terminal** (Java's stderr surface). A live progress record on stderr, from the snapshot the progress file
  is written from: the phase (`starting`, `verifying` with a resume's bytes checked, `listing`), objects listed
  (with `+N recovered`: rows a resume kept), live and average keys/s, parts published, requests in flight,
  elapsed time since the command began, API calls and, on a known tariff, the labelled estimate; no percentage
  or ETA (a listing has no honest denominator), no key text. On a terminal it is redrawn in place (carriage
  return, `ESC[2K`; on Windows after enabling virtual-terminal processing, else plain lines), its trailing
  fields dropped whole to fit the width, every second; elsewhere it is appended as plain lines every 30 s.
  Shown by default when stderr is a terminal and neither `-q` nor `-v` is given; `--progress` forces it,
  `--no-progress` suppresses it, `--progress-interval D` (at least 1 s) sets the cadence and implies it.
  Warnings written while the line is drawn erase it and repaint it after. At the end, `--stats` prints Java's
  block on stderr: a disposition (`INCOMPLETE (class)` with `resume: swath resume DIR` for a resumable stop; a
  refused run prints only this line), `N objects in T · K keys/s` (with the whole invocation's time when it is
  over a second longer), `N API calls · R per 1k objects · in flight avg A · peak P`, throttles, retries and
  entry errors when any, the estimate, and `files · bytes written · peak RSS`. Without `--stats` it is printed
  for a run over 1.5 s, a run that wrote parts, and a run that did not complete; not with `-q`, not after a
  closed stdout reader; `--no-stats` never. The one-line summary stays on stdout. `-v` (`-vv`, `-vvv`: the same;
  Go has no finer log levels) prints a start line and shows progress as appended records; `-q` drops warnings
  and the automatic progress and statistics; `-qq` also drops the final error line (the exit code, `error.json`
  and `--report` stand). `--color auto|always|never` colours the progress line and the statistics (headline
  cyan, the rest dim, `INCOMPLETE` red): auto is on for a terminal unless `NO_COLOR` is set and not empty or
  `TERM=dumb`, and `CLICOLOR_FORCE` forces it. TTY detection and width: `golang.org/x/term`.
- **Environment:** `SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` name pprof output files;
  `SWATH_LOCALFS_ENGINE=portable` selects the portable filesystem engine; `SWATH_SPLIT_TRACE` traces range-engine splits on stderr, one line per split (range id,
  bounds, cursor, pivot, keys listed, the pivot's source, the children's depth, the range's gallop generation
  and the anchor its upper child carries).

## Run report

<!-- report:begin -->
One schema (`listing.Report`, `report_version` 3) for `run.json` beside a committed dataset, `--report FILE`,
and what `--report-format json` prints. It is operational: readers accept a dataset by its manifest, never by
this report. The schema is pinned by `internal/listing/testdata/report-schema.golden` (`TestReportSchemaGolden`;
a change bumps `report_version` and is listed in [upgrade.md](upgrade.md#run-report-and-counters)), and
`TestReportSchemaDoc` keeps this section naming every field. It accounts for this invocation (rows it fetched
and emitted, requests it made) separately from the dataset (rows retained from an interrupted run, durable,
committed). Durations are integers with the unit in the name; a measurement that was not taken is absent,
never zero.

| Field | Meaning |
|---|---|
| `report_version` | 3 |
| `build` (`tool`, `version`, `revision`) | the binary: `swath-go`, the Go module version (`devel` for a binary built from a source checkout), the VCS revision (`-dirty` if modified) |
| `attempt` (`id`, `started`) | this invocation: 128 random bits in hex, and when the command began; `error.json` carries the same |
| `invocation` | what was asked: `argv` (secret flags `[redacted]`), `source` (the root, narrowed by an anchored `--include`), `out`, `resume`, `schema_id`, `fields` (the metadata tier), `workers`, `encoders`, `row_budget_bytes` (the one row budget), `part_size_bytes`, `prior` (`--seed-from`: `path`, `source` `dataset` or `cuts`, `cuts` offered, `counted`) |
| `outcome` | `status` (`committed`, `written`, `listed`, `stopped`, `failed`), `error`, `complete`, `stop_reason`, `stop_source`, `entry_errors`, `class`, `exit_code`, `resumable`, `cause`, `unrecorded` (Exit status above) |
| `args_hash` | SHA-256 over the run's frozen identity (tool, format, schema, key cap, writer, part size, row bound, provider, the source's identity fields such as bucket, prefix, route and region, and the filters): the same for a run and its resumes, never a credential, not equal to Java's (which covered the scope only) |

**Timings** (`timings`). `started_at` is the run's start (after the command line was read), `finished_at` its
end, `wall_ms` between them; `session_ms` is the whole invocation, from `attempt.started` (credential lookup,
a resume's verification and publication included). The phases follow one another: `listing_ms` from
`started_at` to `listing_finished_at`, the end of the order stage (every row listed and handed on in order; it
includes the setup and the order stage's drain after the last request), then `drain_ms` until the encoders have
written and closed the last part, then `publication_ms` until the manifest is installed (`drain_ms` and
`publication_ms` absent without a committed dataset). `seed_ms` overlaps `listing_ms` at its start: the range
engine's seed, from its first delimiter request to its tiles (absent where none ran). `user_cpu_ms` and
`sys_cpu_ms` are the process's CPU times (absent where the platform does not measure them).

**Rows and bytes** (`output`). `rows_fetched` is what this invocation listed from the source, `rows_filtered`
what the filters dropped of it, `rows_emitted` what was handed on in order (swath count: counted). On a run that
succeeded, `rows_fetched` = `rows_filtered` + `rows_emitted`; a run that stopped has also fetched rows ahead of
the frontier that it never emitted. `rows_retained` is kept from an interrupted run, `rows_durable` in parts
that stay in the output (the committed dataset's, or after a failure the journaled parts'), `rows_committed` in
the committed dataset (`rows_retained` + `rows_emitted`). `parts_written` and `parts_retained` count parts.
`object_bytes` sums the sizes of the objects the order stage accepted (the size column; absent for a schema without one): on a committed run exactly those of `rows_emitted`, on a run that stopped also those accepted and never handed on;
`output_bytes` is the dataset's part bytes. `text` is a text run's: `format`, `destination`, `compression`,
`rows`, `bytes` (delivered, compressed), `sink_wait_ns`, `output_wait_ns`, `formatters`, and `compressor`
(`bytes_in`, `bytes_out`, `jobs`, `jobs_reason`: `cores`, `row_budget`, `encoders`, `gzip`).

For versions schemas only, `versions`, `distinct_keys` and `live_objects` describe this attempt's
rows after filtering at the order stage's handoff (swath count: validated, counted rows).
`versions` excludes S3 delete markers; S3 also reports `delete_markers`, with
`rows_emitted` = `versions` + `delete_markers`. GCS reports `noncurrent` among its
`versions`, with `versions` = `noncurrent` + `live_objects`. A live S3 object has
`is_latest` true and `is_delete_marker` false; a live GCS generation has null
`time_deleted`. Zero counts are present. Retained resume rows, held tails, relisting,
and raw overfetch are excluded. `distinct_keys` comes from `order.Stats.DistinctKeys`;
a failed order run can also have validated a pending batch that never reached its sink.

**Requests** (`requests`, `cost`, `efficiency`; absent for a source that makes no requests). `http_attempts`
is the one request count: every HTTP attempt sent (pages, probes, client retries, engine requeues); `retries`,
`throttles` and `timeouts` are attempts among them repeated after a retryable failure, answered with a throttle,
and past the per-request timeout. `cost_usd` is `http_attempts` at `basis` `rate_per_1k_usd` (0.005, `basis` `source`
`aws-list-reference-rate` for AWS S3, `gcs-class-a-reference-rate` for GCS's own endpoint); both `null` on a
custom endpoint, whose tariff is unknown. `keys_per_sec` is `rows_emitted` over `listing_ms`;
`http_attempts_per_1k_rows` is `http_attempts` per 1,000 `rows_fetched` (`null` without a row). Latency
percentiles and in-flight time are the provider's counters (`source.stats.counters.values` `inflight.p50_ns`,
`inflight.p90_ns`, `inflight.p99_ns`, `inflight.busy_ns`).

**Resources** (`resources`). `peak_rss_bytes` (with `rusage_available`), `peak_heap_live_bytes` (sampled every
100 ms), `max_buffered_bytes` (peak reorder storage, against `invocation.row_budget_bytes`), `free_disk_min_bytes`
(the least free space seen, every second, on the filesystem the run writes to: the spill directory's, else the
output's; absent for swath count), `peak_ahead_rows`, `frontier_stuck_max_ns`, `frontier_wait_ns`,
`budget_wait_ns` and `encoder_wait_ns` (summed over goroutines: not elapsed time), `peak_nodes`, `peak_markers`,
`part_time_cuts`, `part_row_cuts`, `features` (the pipeline's policies, on or off), and `spill` (absent with spill
off): `rows`, `bytes`, `logical_bytes`, `format`, `decode_refused`, `ranges`, `peak_bytes`, `refused`,
`max_bytes`, `reserve_bytes`, `peak_readers`, `dir`, `fs_type`.

**Source** (`source`: `name`, `stats`). The provider's own account: its counters under `counters` (one
namespace, [internals/counters.md](internals/counters.md)).

**History** (`history`). The run's progress over time, at most 300 samples (`listing.HistoryCap`, about 30 KB):
the first at the start of listing, then one per `interval_ms` (1 s until the history fills; then every other
sample is dropped and the interval doubles), and the last at the end. Each of `samples`: `at`, `rows_fetched`,
`rows_emitted`, `rows_durable` (in journaled or committed parts, retained ones included), `http_attempts`,
`inflight`, `limit` (the limiter's, 0 without one), `spill_bytes` (live), `free_disk_bytes` (absent without an
output) and `rss_bytes` (the resident set now; Linux only, absent elsewhere). Absent for a run that stopped
before listing.
<!-- report:end -->
