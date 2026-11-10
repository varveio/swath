# swath list

swath list lists every object (or entry) below SOURCE, as text rows or into a sorted Parquet dataset.

## Synopsis

```text
swath list SOURCE [OPTIONS]
swath list SOURCE -o DIR [OPTIONS]
swath list SOURCE -o NAME.tsv|NAME.jsonl[.gz|.zst] [OPTIONS]
swath ls ...
```

## Description

Without `-o`, or with `-o -`, the rows go to stdout: a table on a terminal, TSV in a pipe (`--format` chooses
`table`, `tsv` or `jsonl`). `-o NAME.tsv` or `-o NAME.jsonl`, optionally ending in `.gz` or `.zst`, writes
that file, compressed as its name says. Any other `-o` name, and any name ending in `/`, is a directory that
receives a [swath dataset](../dataset.md): sorted Parquet parts and a `manifest.json` that commits them.
A name ending in an extension swath does not write (`.csv`, `.json`, `.txt`, a bare `.gz`, ...) and a URL
(`s3://...`) are refused.

Every row has `key` first, then the source's columns under the dataset's names: `size`, `last_modified` and
the columns of the source's schema ([output.md](../output.md#text-rows)). Every format and every source
lists in ascending byte order of the key. A local tree lists its directories too (kind `DIR`, the key ending
in `/`) unless `--no-dirs`; a local SOURCE that is a file lists as its one row.

A dataset directory must be new or empty; `--overwrite` replaces a previous swath dataset there, and
nothing else. A text file is written beside its name and renamed into place when complete, so a failed run
leaves the previous file as it was. A reader that closes stdout early (`| head`) ends the run with exit 0.

Only a dataset run of an object store keeps a journal: if it is interrupted, [`swath resume`](swath-resume.md)
continues it. Text output and local trees are run again.

The [filters](../filters.md) keep or drop rows by key, size, time and storage class. A filtered dataset says
so in its manifest.

## Options

<!-- flags:begin -->
```text
Output:
  -o, --output PATH           a dataset DIR, a .tsv or .jsonl file (.gz, .zst), or - (stdout)
                                A trailing / names a directory whatever its name.
      --format auto|table|tsv|jsonl
                              text rows: auto is the table on a terminal, TSV otherwise
                                With -o FILE the file's name says the format; a --format that agrees
                                with it is accepted.
      --columns NAME[,NAME...]
                              TSV and JSONL: these columns, in order (repeatable)
                                By the dataset's column names; a name the source's schema lacks is
                                refused, listing its columns. Not with the table or a dataset; on a
                                terminal the rows are then TSV.
      --overwrite             replace a previous dataset in -o

Filters:
      --include GLOB          keep keys this glob matches (repeatable: any of them)
                                Matched against the key below SOURCE: with a '/' from SOURCE's root
                                (logs/*.gz), without one the last segment at any depth (*.gz). * and
                                ? stay within a segment, ** is whole segments, [a-z], [!a-z], {a,b},
                                \c. Includes sharing a literal prefix narrow the listed prefix.
                                Filters keep or drop rows; they do not prune the walk.
      --exclude GLOB          drop keys this glob matches (repeatable; beats any include)
                                Matched as --include is. dir/ drops a directory's own row, dir/**
                                the rows below it; the walk still lists them.
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
      --all-versions          S3 and GCS: list every version, S3 delete markers too (not Azure Blob)
                                S3: versions and delete markers (needs s3:ListBucketVersions). GCS:
                                generations, live and noncurrent. Refused on Azure Blob. A key's
                                rows are consecutive and never split across parts.

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
      --azure-assume-flat     assume a flat namespace where the account will not tell
                                For when Get Account Information and the Data Lake host's Filesystem
                                Get Properties are both refused or silent; a hierarchical namespace
                                they report still refuses. Frozen for resume and shown in the
                                report.

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
      --encoders N            Parquet encoders, or a .zst file's compression jobs (default: CPUs)
                                Lower it to leave cores to other work; the default follows
                                GOMAXPROCS, the row budget and the descriptor limit. An explicit
                                count is kept, with a warning when they cannot carry it.
      --row-budget SIZE       rows held between listing and encoding (default 128MiB)
                                Not a cap on resident memory. Raise it when resources.budget_wait_ns
                                is large or spill is heavy and memory allows; lower it on a small
                                host (the peak RSS is a few times it).
      --spill-budget SIZE     cap on spill (default: half the output filesystem's free space)
                                Set it when the output filesystem is shared or small. Past it, or at
                                the reserve, listing waits instead of spilling.
      --part-size SIZE        target size of a Parquet part (default 64MiB)
                                Smaller parts for readers that load whole parts, larger for fewer
                                files on a very large listing.
      --part-interval DURATION
                              close a part with rows after this long, full or not (default 30s)
                                0: by size only. Lower it to close parts sooner on a slow listing
                                (it bounds how long rows wait in an open part, not how much a resume
                                repeats: a closed part still publishes in order); 0 for part
                                boundaries independent of timing.
      --part-rotation-max-rows N
                              also close a part at N rows (0: by size and --part-interval only)
                                Set it when readers want parts of a bounded row count; it is fixed
                                for the run.
      --seed-from PATH        start from a previous listing's key distribution, not the seed
                                A committed dataset of the same source and scope, or a cut file (one
                                key per line, ascending bytewise, optionally TAB and the keys
                                expected up to it); the engine still splits adaptively.

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

Show the first rows of a prefix:

```bash
swath list s3://bucket/logs/ | head
```

Write a dataset with a run report beside it, stopping after six hours:

```bash
swath list s3://bucket/logs/ -o /data/logs-2026-11-10 --report /var/log/swath/attempt-1.json --max-duration 6h
```

Write every object's key and size as compressed JSONL:

```bash
swath list gs://bucket -o rows.jsonl.zst --columns key,size
```

List a public bucket without credentials:

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous
```

Keep the `.parquet` files modified in the last week of a local tree:

```bash
swath list /data --include '*.parquet' --modified-since 7d
```

List every version and delete marker:

```bash
swath list s3://bucket/logs/ --all-versions -o logs-versions
```

## Exit status

0 when the listing completed, or the reader closed stdout. 1 when it failed (access denied, no such bucket),
2 for a usage error or a refusal (a non-empty `-o` directory without `--overwrite`). A dataset run of an object
store that stops early exits 74, 75, 124, 130 or 143 and can be resumed. See
[reference/exit-codes.md](../reference/exit-codes.md).

## Environment

The provider's credential and endpoint variables ([sources.md](../sources.md)), and those of
[swath(1)](swath.md#environment).

## Files

A dataset directory holds:

```text
DIR/data/part-NNNNN.parquet   the parts, in key order
DIR/manifest.json             the commit: written last; a reader starts only once it exists
DIR/run.json                  the run report, after the commit
DIR/error.json                the outcome of an attempt that did not complete
DIR/.swath/                   swath's workspace: the lock, the journal, spill, progress.json
```

[dataset.md](../dataset.md) describes the parts and the manifest; [running.md](../running.md) the workspace
and its disk.

## See also

[swath-du](swath-du.md), [swath-resume](swath-resume.md), [swath-show](swath-show.md),
[output.md](../output.md), [filters.md](../filters.md), [sources.md](../sources.md),
[reference/run-report.md](../reference/run-report.md)
