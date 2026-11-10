# CLI

```text
swath list SOURCE -o DIR [OPTIONS]        list SOURCE into a sorted Parquet dataset in DIR (also: swath ls)
swath list SOURCE [-o FILE|-] [OPTIONS]   list SOURCE as text rows to a .tsv or .jsonl file (.gz, .zst) or stdout
swath du SOURCE [-d N] [OPTIONS]          total SOURCE's objects and bytes, per key prefix down to N levels
swath resume DIR [OPTIONS]                continue the interrupted run in DIR
swath show DIR [--journal]                show a dataset or an interrupted run
swath diag list|count SOURCE [OPTIONS]    list, or count without writing, with the diagnostic flags (experiments, comparisons)
swath completion bash|zsh|fish            print a shell completion script
swath version, swath -V, --version        print swath VERSION (REVISION)
```

`swath --help` prints that summary; `swath help COMMAND` is `swath COMMAND --help`. On every command `-h`
prints its flags one line each, `--help` adds each flag's further text (when to change it, its constraints);
both go to stdout and exit 0. Flags are parsed GNU-style: short flags bundle (`-vq`) and take attached values
(`-d1`, `-oPATH`), long ones take `--name value` or `--name=value`, flags and operands interleave, `--` ends the
flags, and a long name with one dash (`-depth`) is refused (a flag's value, `--include -verbose`, is a value). `-v`, `-q` and `--color` may also come before the
command word. An unknown flag is a usage error naming the nearest flag (`unknown flag --region (did you mean
--s3-region?)`) and the help to read; an error never repeats a typed value it cannot tell from a credential
(an unknown flag's value, a SAS in `-o` or an extra argument). A default-on switch is turned off by `--no-NAME`; the help never shows
`=false`. A URL scheme is matched in any case (`S3://b`), and an Azure Blob service URL
(`https://ACCOUNT.blob.core.windows.net/CONTAINER[/PREFIX]`) is the source `az://ACCOUNT/CONTAINER[/PREFIX]`.

SOURCE is a local directory or file, `s3://bucket[/prefix]`, `gs://bucket[/prefix]` or `az://account/container[/prefix]`. `swath list` writes a dataset to `-o DIR`
([internals/parquet-output.md](internals/parquet-output.md)), text rows to a file or stdout (**Text output**
below). `swath du` and `swath diag count` only list and count, writing no rows (rows are counted as they arrive,
never ordered, buffered or spilled: [internals/ordering.md](internals/ordering.md#counting-without-order)); they
take the source, filter, performance, limit and reporting flags, and refuse the output ones (`-o`, `--overwrite`,
`--part-size`, `--row-budget`, `--spill-budget`, `--encoders`; `--format` but for `swath du`'s totals) with exit 2.
`swath du` prints per-prefix totals on stdout (**Per-prefix totals** below), and only it takes their flags
(`--depth`/`-d`, `--max-prefixes`, `--allocated`, `--count-links-once`, `--max-inodes`). `swath diag count` measures
the listing: it prints one line, `rows=N object_bytes=B wall=...` (a local tree's line starts with its counters,
`files=N dirs=N bytes=N errors=N`). `swath list` with `-o` prints nothing on stdout (the account is on stderr).
Any of them prints the run report (`run.json`, below) on stdout instead with `--report -`, but where stdout
carries rows (`swath list` to stdout, `swath du`); `--report FILE` writes that report to
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
`--part-interval`, the diagnostic `--no-spill-compress`, `--spill-plain` and `--spill-reserve`, `--report`, `--max-duration`, `--stall-timeout`, `--seed-from`, and the terminal flags
`--progress`, `--no-progress`, `--progress-interval`, `--stats`, `--no-stats`, `-v`, `-q`, `--color`, `--log-format`) and the
operational ones, how requests are
authenticated, timed and admitted (the shared `--anonymous`, `--request-timeout`, `--no-adaptive-limit`,
`--adaptive-limit-slow-start` and `--request-rate`, and each provider's credential flags: `--azure-sas-token`,
`--gcs-credentials-file`, `--s3-bearer-token-command`, `--s3-profile`, `--s3-bearer-token-refresh-interval`), may be given again (the last value
wins; each such switch has its other spelling to undo a recorded one: `--no-anonymous` undoes `--anonymous`,
`--adaptive-limit` undoes `--no-adaptive-limit`); `swath resume --help` lists them. Any other flag is a usage
error (`--part-size is fixed for the run and cannot change on resume`). The journal records the command in
its canonical spelling (each flag `--name=value`, a switch `--name`, then SOURCE), which a resume re-parses. The source and output
identity stays frozen: a resume whose overrides resolve another route (a profile with another region, say) is
refused by the identity check. `swath resume` on a committed dataset with complete coverage has nothing to do
and exits 0 (`DIR is already complete: nothing to resume`; the report's outcome is `completed`, `complete`
true), so a supervisor may resume until exit 0; one committed with partial coverage stays `incomplete`, exit 1.
`swath show DIR` prints a committed dataset's manifest summary (its layers: the SeekParquet format and draft,
the swath schema id and the swath metadata version; then key, source, scope, coverage, parts, rows, key range) or an interrupted run's recorded command, frozen identity, committed parts and the key
`swath resume` would start after; `--journal` dumps the journal as JSON lines. `swath diag list` takes every
`list` flag and `swath diag count` every count flag, plus the diagnostic ones (the **Diagnostics** group below);
they exist to run an experiment or a comparison, not to operate the tool, and `swath list` and `swath du` refuse
each of them with exit 2 and a message naming `swath diag`. A flag has one long name, and the everyday ones a
short form too (`-o, --output`, `-d, --depth`, `-j, --workers`, `-v, --verbose`, `-q, --quiet`). A flag is bare
when it applies to every object store with the same meaning (`--endpoint-url`, `--anonymous`) or is a general or
local-tree flag; one only a provider has carries its prefix (`--s3-region`, `--gcs-credentials-file`,
`--azure-sas-token`), and is refused for another provider's source in either spelling. From the Java 0.3 releases, and renames since the unreleased builds of `main`: [upgrade.md](upgrade.md#changes-from-03-to-04). The
Java v0.3.2 distribution has its own launcher and different output contract. This unreleased Go
implementation will replace Java at release; the Java code has not been removed yet.
Every Java CLI option, output mode, exit code and report field against this tool: [cli-inventory.md](cli-inventory.md).
Running it in production (sizing, watching, resuming, reading the output): [operating.md](operating.md).

## Flags

Generated from the flag table, as `--help` prints it at 100 columns. The groups are, in order: Totals
(`swath du`), Output, Filters, Object stores (the flags every object store shares), S3, GCS, Azure Blob, Local,
Performance, Limits, Reporting, and for `swath diag` Diagnostics.
`TestCLIDocFlags` fails when this block
is stale (`UPDATE=1 go test ./cmd/swath` rewrites it), and `TestHelpGroupsCoverFlags` when a flag is in no
group:

<!-- flags:begin -->
```text
$ swath list --help
Usage: swath list SOURCE [OPTIONS]
       swath ls SOURCE [OPTIONS]

List SOURCE: into a sorted Parquet dataset with -o DIR, as text rows into -o NAME.tsv or NAME.jsonl
(optionally .gz or .zst), or as text rows on stdout without -o. SOURCE is a local directory or file,
az://account/container[/prefix], gs://bucket[/prefix] or s3://bucket[/prefix].

Examples:
  swath list s3://bucket/logs/ -o logs.parquet
  swath list s3://bucket/logs/ | head
  swath list gs://bucket -o rows.jsonl.zst

Output:
  -o, --output PATH           a dataset DIR, a .tsv or .jsonl file (.gz, .zst), or - (stdout)
                                A trailing / names a directory whatever its name.
      --format auto|table|tsv|jsonl
                              text rows: auto is the table on a terminal, TSV otherwise
                                With -o FILE the file's name says the format; a --format that agrees
                                with it is accepted.
      --columns NAME          TSV and JSONL: these columns, in order (key,size; repeatable)
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
      --exclude GLOB          drop keys this glob matches (repeatable; beats any include)
      --include-regex REGEX   keep keys in which this regex (Go RE2) matches anywhere (repeatable)
                                Any include, glob or regex, keeps a key. ^ with a literal prefix
                                narrows the listed prefix as a glob's does.
      --exclude-regex REGEX   drop keys in which this regex (Go RE2) matches anywhere (repeatable)
      --min-size SIZE         keep objects (local: entries) of at least this size (1K, 1.5GiB)
                                Every SIZE: bytes, or K, M, G, T and KiB, MiB, GiB, TiB (binary) or
                                KB, MB, GB, TB (decimal), any case.
      --max-size SIZE         keep objects or entries of at most this size
      --modified-since TIME   keep objects or entries modified at or after TIME (2026-01-01, 7d)
                                An offset or Z; UTC without one. A duration (7d, 12h) is that long
                                before the run started, recorded as the time it names.
      --modified-until TIME   keep objects or entries modified at or before this time
      --storage-class NAME    keep objects in these storage classes (STANDARD,GLACIER; repeatable)
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
                                then its first path segment (Azurite). Part of the resume identity.
      --ca-bundle FILE        trust the certificates in this PEM file besides the system's
                                For a service behind a private certificate authority. S3: default
                                AWS_CA_BUNDLE.
      --no-verify-ssl         do not verify the service's TLS certificate (warns)
                                For a test endpoint only. A resume replays it, and warns again.
      --all-versions          list every version of every key, not only the current objects
                                S3: versions and delete markers (needs s3:ListBucketVersions). GCS:
                                generations, live and noncurrent. A key's rows are consecutive and
                                never split across parts.

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
                              stop the run after this long (exit 124, resumable); 0: no limit
                                Give every unattended run one; a stopped object-store run resumes
                                where it stopped.
      --stall-timeout DURATION
                              stop as stuck after this long without progress (exit 75) (default 10m)
                                0: never. Raise it for a service that is slow to answer for minutes
                                at a time.
      --request-timeout DURATION
                              per-request timeout; a request over it is retried (default 30s)
                                Raise it for a slow endpoint or emulator whose pages take longer to
                                answer.
      --request-rate N        cap every HTTP attempt at N per second (0: no cap)
                                Set it to stay under a request quota shared with other clients.
                                Refused when the spacing exceeds half --stall-timeout.
      --no-adaptive-limit     turn off the adaptive concurrency limit that throttle answers lower
                                Turn it off (with --request-rate) when a run crawls under sustained
                                throttling with the limit near 1. Without it each of the --workers
                                listing workers waits on one page at a time, and seed listings and
                                structure probes run beside them.

Reporting:
      --report FILE           write the run report (JSON) to FILE, or - for stdout, at the end
                                Written on failure too, with the outcome and the attempt id. - is
                                refused where stdout carries rows.
  -v, --verbose               a start line, progress records; -vv, -vvv: the request log
                                -vv adds each failed request and retry with its cause, -vvv every
                                request (credentials and tokens redacted).
  -q, --quiet                 no warnings, no automatic progress or statistics (-qq: no errors)
                                With -qq the exit code and the reports stand alone.
      --progress              print live progress on stderr (default: on a terminal, without -q, -v)
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

swath resume --help lists the options a resume may change.
Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.

$ swath du --help
Usage: swath du SOURCE [OPTIONS]

Total SOURCE's objects (a local tree's files), bytes and newest time, and with -d N each prefix's
down to N levels below SOURCE, a parent including its children, as du. On stdout: a table on a
terminal (the total last), TSV otherwise or with --format jsonl (the total first, at Depth 0). Bytes
are the objects' sizes as listed, not storage or billing. SOURCE is a local directory or file,
az://account/container[/prefix], gs://bucket[/prefix] or s3://bucket[/prefix].

Examples:
  swath du s3://bucket/logs/
  swath du -d 1 /data
  swath du gs://bucket --format jsonl

Totals:
  -d, --depth N               also total each prefix down to N levels below SOURCE (0: the total)
                                A prefix ends at a '/'; each includes what is below it, as du.
      --allocated             local: total the files' allocated bytes instead of their apparent size
                                st_blocks x 512, as du; a directory's own blocks are not counted.
      --count-links-once      local: count a file with several hard links once, as du does
                                Under the first of its names in byte order.
      --sort name|size        order prefixes by name, or by size, largest first (default name)
                                By size the prefixes below each parent are ordered, each still above
                                its own; the total keeps its place (last in the table, first in TSV
                                and JSONL), and a prefix of unknown size comes last.
      --max-prefixes N        fail rather than hold more than N distinct prefixes (default 1000000)
                                Raise it for a deep --depth over a wide tree when memory allows
                                (about 150 bytes a prefix); it bounds memory, never the objects
                                counted.
      --max-inodes N          local: the most hard-linked files to remember (default 1000000)
                                With --count-links-once. Raise it for a tree of more hard-linked
                                files when memory allows (about 100 bytes each).

Output:
      --format auto|table|tsv|jsonl
                              text rows: auto is the table on a terminal, TSV otherwise
                                With -o FILE the file's name says the format; a --format that agrees
                                with it is accepted.

Filters:
      --include GLOB          keep keys this glob matches (repeatable: any of them)
                                Matched against the key below SOURCE: with a '/' from SOURCE's root
                                (logs/*.gz), without one the last segment at any depth (*.gz). * and
                                ? stay within a segment, ** is whole segments, [a-z], [!a-z], {a,b},
                                \c. Includes sharing a literal prefix narrow the listed prefix.
      --exclude GLOB          drop keys this glob matches (repeatable; beats any include)
      --include-regex REGEX   keep keys in which this regex (Go RE2) matches anywhere (repeatable)
                                Any include, glob or regex, keeps a key. ^ with a literal prefix
                                narrows the listed prefix as a glob's does.
      --exclude-regex REGEX   drop keys in which this regex (Go RE2) matches anywhere (repeatable)
      --min-size SIZE         keep objects (local: entries) of at least this size (1K, 1.5GiB)
                                Every SIZE: bytes, or K, M, G, T and KiB, MiB, GiB, TiB (binary) or
                                KB, MB, GB, TB (decimal), any case.
      --max-size SIZE         keep objects or entries of at most this size
      --modified-since TIME   keep objects or entries modified at or after TIME (2026-01-01, 7d)
                                An offset or Z; UTC without one. A duration (7d, 12h) is that long
                                before the run started, recorded as the time it names.
      --modified-until TIME   keep objects or entries modified at or before this time
      --storage-class NAME    keep objects in these storage classes (STANDARD,GLACIER; repeatable)
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
                                then its first path segment (Azurite). Part of the resume identity.
      --ca-bundle FILE        trust the certificates in this PEM file besides the system's
                                For a service behind a private certificate authority. S3: default
                                AWS_CA_BUNDLE.
      --no-verify-ssl         do not verify the service's TLS certificate (warns)
                                For a test endpoint only. A resume replays it, and warns again.
      --all-versions          list every version of every key, not only the current objects
                                S3: versions and delete markers (needs s3:ListBucketVersions). GCS:
                                generations, live and noncurrent. A key's rows are consecutive and
                                never split across parts.

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
      --seed-from PATH        start from a previous listing's key distribution, not the seed
                                A committed dataset of the same source and scope, or a cut file (one
                                key per line, ascending bytewise, optionally TAB and the keys
                                expected up to it); the engine still splits adaptively.

Limits:
      --max-duration DURATION
                              stop the run after this long (exit 124, resumable); 0: no limit
                                Give every unattended run one; a stopped object-store run resumes
                                where it stopped.
      --stall-timeout DURATION
                              stop as stuck after this long without progress (exit 75) (default 10m)
                                0: never. Raise it for a service that is slow to answer for minutes
                                at a time.
      --request-timeout DURATION
                              per-request timeout; a request over it is retried (default 30s)
                                Raise it for a slow endpoint or emulator whose pages take longer to
                                answer.
      --request-rate N        cap every HTTP attempt at N per second (0: no cap)
                                Set it to stay under a request quota shared with other clients.
                                Refused when the spacing exceeds half --stall-timeout.
      --no-adaptive-limit     turn off the adaptive concurrency limit that throttle answers lower
                                Turn it off (with --request-rate) when a run crawls under sustained
                                throttling with the limit near 1. Without it each of the --workers
                                listing workers waits on one page at a time, and seed listings and
                                structure probes run beside them.

Reporting:
      --report FILE           write the run report (JSON) to FILE, or - for stdout, at the end
                                Written on failure too, with the outcome and the attempt id. - is
                                refused where stdout carries rows.
  -v, --verbose               a start line, progress records; -vv, -vvv: the request log
                                -vv adds each failed request and retry with its cause, -vvv every
                                request (credentials and tokens redacted).
  -q, --quiet                 no warnings, no automatic progress or statistics (-qq: no errors)
                                With -qq the exit code and the reports stand alone.
      --progress              print live progress on stderr (default: on a terminal, without -q, -v)
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

swath resume --help lists the options a resume may change.
Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.

$ swath diag list --help
Usage: swath diag list SOURCE [OPTIONS]

Every swath list option (swath list --help), and the diagnostic ones below: they exist to run an
experiment or a comparison, not to operate the tool. SOURCE is a local directory or file,
az://account/container[/prefix], gs://bucket[/prefix] or s3://bucket[/prefix].

Examples:
  swath diag list s3://bucket/logs/ -o ds --no-seed
  swath diag list s3://bucket/logs/ -o ds --page-size 100

Diagnostics:
      --no-spill-compress   write spill frames uncompressed (not zstd level 1)
      --spill-plain         write spill frames without delta coding (a comparison arm)
      --no-journal          publish parts only at the end, without the journal resume needs
      --no-spill            wait on the row budget instead of spilling rows to disk
      --spill-reserve SIZE  free space spill leaves on its filesystem (0: derived)
                              Derived: one part per encoder, at least 1 GiB.
      --adaptive-limit-slow-start
                            start the limit at 4 in flight, doubling each second to --workers
      --no-seed             start from one range, without the delimiter-listing seed
      --structure[=auto|on|off]
                            probes whose folder boundaries splits pivot on (default auto)
                              The bare flag is on, so a mode needs '='. auto probes on the open
                              frontier while its cursor is in a folder, and in bounded ranges whose
                              chain stays inside one folder; on probes every range in a folder.
      --slow-split          split a range whose pages are slow without waiting for an idle worker
      --floors              never split a range of 4 pages or fewer, nor leave a child under 2
      --sequential          list with one continuation-token chain (no ranges; the reference)
      --max-keys N          with --sequential, stop after N objects (marked incomplete)
      --page-size N         ask for pages of N entries (1 to 1000; default the provider's)

$ swath diag count --help
Usage: swath diag count SOURCE [OPTIONS]

List SOURCE and count its rows and object bytes, writing nothing, to measure the listing: rows are
counted as they arrive, never ordered, buffered or spilled. Prints one line of counts and resources.
SOURCE is a local directory or file, az://account/container[/prefix], gs://bucket[/prefix] or
s3://bucket[/prefix].

Examples:
  swath diag count s3://bucket/logs/
  swath diag count s3://bucket/logs/ --sequential

Filters:
      --include GLOB          keep keys this glob matches (repeatable: any of them)
                                Matched against the key below SOURCE: with a '/' from SOURCE's root
                                (logs/*.gz), without one the last segment at any depth (*.gz). * and
                                ? stay within a segment, ** is whole segments, [a-z], [!a-z], {a,b},
                                \c. Includes sharing a literal prefix narrow the listed prefix.
      --exclude GLOB          drop keys this glob matches (repeatable; beats any include)
      --include-regex REGEX   keep keys in which this regex (Go RE2) matches anywhere (repeatable)
                                Any include, glob or regex, keeps a key. ^ with a literal prefix
                                narrows the listed prefix as a glob's does.
      --exclude-regex REGEX   drop keys in which this regex (Go RE2) matches anywhere (repeatable)
      --min-size SIZE         keep objects (local: entries) of at least this size (1K, 1.5GiB)
                                Every SIZE: bytes, or K, M, G, T and KiB, MiB, GiB, TiB (binary) or
                                KB, MB, GB, TB (decimal), any case.
      --max-size SIZE         keep objects or entries of at most this size
      --modified-since TIME   keep objects or entries modified at or after TIME (2026-01-01, 7d)
                                An offset or Z; UTC without one. A duration (7d, 12h) is that long
                                before the run started, recorded as the time it names.
      --modified-until TIME   keep objects or entries modified at or before this time
      --storage-class NAME    keep objects in these storage classes (STANDARD,GLACIER; repeatable)
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
                                then its first path segment (Azurite). Part of the resume identity.
      --ca-bundle FILE        trust the certificates in this PEM file besides the system's
                                For a service behind a private certificate authority. S3: default
                                AWS_CA_BUNDLE.
      --no-verify-ssl         do not verify the service's TLS certificate (warns)
                                For a test endpoint only. A resume replays it, and warns again.
      --all-versions          list every version of every key, not only the current objects
                                S3: versions and delete markers (needs s3:ListBucketVersions). GCS:
                                generations, live and noncurrent. A key's rows are consecutive and
                                never split across parts.

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
      --seed-from PATH        start from a previous listing's key distribution, not the seed
                                A committed dataset of the same source and scope, or a cut file (one
                                key per line, ascending bytewise, optionally TAB and the keys
                                expected up to it); the engine still splits adaptively.

Limits:
      --max-duration DURATION
                              stop the run after this long (exit 124, resumable); 0: no limit
                                Give every unattended run one; a stopped object-store run resumes
                                where it stopped.
      --stall-timeout DURATION
                              stop as stuck after this long without progress (exit 75) (default 10m)
                                0: never. Raise it for a service that is slow to answer for minutes
                                at a time.
      --request-timeout DURATION
                              per-request timeout; a request over it is retried (default 30s)
                                Raise it for a slow endpoint or emulator whose pages take longer to
                                answer.
      --request-rate N        cap every HTTP attempt at N per second (0: no cap)
                                Set it to stay under a request quota shared with other clients.
                                Refused when the spacing exceeds half --stall-timeout.
      --no-adaptive-limit     turn off the adaptive concurrency limit that throttle answers lower
                                Turn it off (with --request-rate) when a run crawls under sustained
                                throttling with the limit near 1. Without it each of the --workers
                                listing workers waits on one page at a time, and seed listings and
                                structure probes run beside them.

Reporting:
      --report FILE           write the run report (JSON) to FILE, or - for stdout, at the end
                                Written on failure too, with the outcome and the attempt id. - is
                                refused where stdout carries rows.
  -v, --verbose               a start line, progress records; -vv, -vvv: the request log
                                -vv adds each failed request and retry with its cause, -vvv every
                                request (credentials and tokens redacted).
  -q, --quiet                 no warnings, no automatic progress or statistics (-qq: no errors)
                                With -qq the exit code and the reports stand alone.
      --progress              print live progress on stderr (default: on a terminal, without -q, -v)
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

Diagnostics:
      --adaptive-limit-slow-start
                              start the limit at 4 in flight, doubling each second to --workers
      --no-seed               start from one range, without the delimiter-listing seed
      --structure[=auto|on|off]
                              probes whose folder boundaries splits pivot on (default auto)
                                The bare flag is on, so a mode needs '='. auto probes on the open
                                frontier while its cursor is in a folder, and in bounded ranges
                                whose chain stays inside one folder; on probes every range in a
                                folder.
      --slow-split            split a range whose pages are slow without waiting for an idle worker
      --floors                never split a range of 4 pages or fewer, nor leave a child under 2
      --sequential            list with one continuation-token chain (no ranges; the reference)
      --max-keys N            with --sequential, stop after N objects (marked incomplete)
      --page-size N           ask for pages of N entries (1 to 1000; default the provider's)
```
<!-- flags:end -->

## Shell completion

`swath completion bash|zsh|fish` prints a completion script for the commands, each command's flags (long and
short), the choices of an enumerated flag (`--format`, `--color`, `--metadata`), file and directory names where
a flag or SOURCE takes a path, and the URL schemes (`s3://`, `gs://`, `az://`). The scripts are generated from
the flag table of the build, so every flag is in them. Install one for the current shell, or put the line in
its startup file:

```text
source <(swath completion bash)                                  # bash; ~/.bashrc
source <(swath completion zsh)                                   # zsh, after compinit; ~/.zshrc
swath completion fish > ~/.config/fish/completions/swath.fish    # fish
```

`UPDATE=1 go test ./cmd/swath -run TestCompletion` rewrites the goldens in `cmd/swath/testdata` after the flag
table changes.

## Semantics worth stating

- **Sizes** (`--part-size`, `--row-budget`, `--spill-budget`, `--spill-reserve`, `--min-size`, `--max-size`) have
  one syntax: a decimal number and an optional unit, in any case: `B` or none bytes, `K`, `M`, `G`, `T` and `KiB`,
  `MiB`, `GiB`, `TiB` binary, `KB`, `MB`, `GB`, `TB` decimal (`64MB` is 64,000,000 bytes, `64M` 64 MiB), coming to a
  whole number of bytes (`1.5G`, not `1.3B`).
- **Durations** (every `DURATION` flag) are Go's (`90s`, `1h30m`) with `d` (24h) and `w` (7d) besides (`2d`,
  `1d12h`).
- **Provider flags** (`S3:`, `GCS:`, `Azure Blob:` and `local:` in the list) apply to their own sources only:
  one given for another source (`gs://... --s3-profile`, where GCS takes `--gcs-credentials-file`; an S3 flag on a local
  tree) is refused (exit 2) rather than ignored. The flags every object store has are one shared group, the same
  for S3, GCS and Azure Blob and refused for a local tree: `--endpoint-url` (an emulator or a compatible store in
  place of the provider's own service), `--request-timeout`, `--anonymous`, and the TLS trust flags
  `--ca-bundle FILE` (PEM certificates trusted besides the system's; for S3 `AWS_CA_BUNDLE` when the flag is
  absent) and `--no-verify-ssl` (no certificate check; it warns on every run that has it, a resume's replay
  included, and the report's `source.stats.no_verify_ssl` is true), beside the range engine's
  (`--all-versions`, `--no-adaptive-limit`, `--request-rate` and the diagnostic policy flags). Both trust flags
  are operational: a resume replays them and may give them again (`--verify-ssl` undoes `--no-verify-ssl`).
- **Azure Blob** (`az://ACCOUNT/CONTAINER[/PREFIX]`): `--endpoint-url URL` replaces
  `https://ACCOUNT.blob.core.windows.net` and takes the account as its first path segment (Azurite:
  `--endpoint-url http://127.0.0.1:10000`). Credentials: `--anonymous`, else a SAS (`--azure-sas-token`, the
  root's query when it names a SAS parameter, or `AZURE_STORAGE_SAS_TOKEN`), else an account key
  (`AZURE_STORAGE_KEY`), else Entra ID (`DefaultAzureCredential`). A SAS is never recorded or shown (the root is
  redacted to `?sig=[redacted]` in the terminal, the journal and the report): `swath resume` must be given one
  again, by `--azure-sas-token` or `AZURE_STORAGE_SAS_TOKEN`, whichever the run used, and may be given another. Only accounts that report a flat namespace are listed, by Get Account Information or,
  when that is refused (an anonymous caller always is) or silent, by the account's Data Lake host
  (`ACCOUNT.dfs.core.windows.net`, which answers anonymous callers; asked only for the default endpoint).
  A hierarchical-namespace account is refused before any listing request (exit 2, cause `unsupported`), as is
  `--all-versions`; so is one whose namespace neither answer gives, unless `--azure-assume-flat` asserts it flat (the
  assertion is frozen for resume and shown in the report's `namespace`, `asserted`). A signed caller that Get Account
  Information refuses (a SAS that fails authentication, 401 or 403) fails as that refusal, cause `auth` or
  `access_denied` (exit 1), as on any request, unless `--azure-assume-flat` is given. Names at U+E000 and above, which
  Azure lists in UTF-16 order, are listed in byte order like every other provider's, through a reordering adapter
  whose cost shows in the report's `order_*` counters
  ([internals/providers/azblob.md](internals/providers/azblob.md)).
- **Durations** are Go durations (`90s`, `1h30m`). A negative one is refused (exit 2) on every duration flag,
  by `swath list` and by `swath resume` before it settles anything;
  `0` turns off `--max-duration`, `--stall-timeout` and `--part-interval`'s time cut.
- **`--row-budget`** bounds the bytes of rows and scheduler metadata in flight (reorder storage and encoder
  queues); it is not a cap on resident memory. See [internals/ordering.md](internals/ordering.md).
- **`-o DIR`** is refused when non-empty unless `--overwrite`, which replaces only
  a previous dataset of this tool and never anything inside the listed root.
- **Text output** (every source, a local tree's included). The `-o` name decides: `NAME.tsv` or `NAME.jsonl`,
  optionally `.gz` or `.zst`, is one file in that
  format and compression; a name ending in `/`, or any other name, is the Parquet dataset (`out.parquet` too);
  a name ending in a data-file extension swath does not write (`.csv`, `.json`, `.ndjson`, `.txt`, `.bz2`, `.xz`,
  `.csv.gz`, a bare `.gz` or `.zst`) and a URL (`s3://`, `gs://`, `az://`, `https://`: object-store destinations
  are not supported) are refused with exit 2; no `-o`, or `-o -`, is stdout, where `--format
  auto|table|tsv|jsonl` chooses (`auto`, the default: the table on a terminal, TSV otherwise) and a pipe
  compresses (`| zstd`). With a `-o` file a `--format` that agrees with its name is accepted and one that does
  not is refused, as is `--format` with a dataset directory.
  Rows are the dataset's: `key`, then the source schema's columns in the schema's order under the dataset's
  column names ([internals/parquet-output.md](internals/parquet-output.md#schemas)), so a script and a DuckDB
  query use the same names. Columns after `key`, `size` and `last_modified` differ by schema; read them by name,
  not by position. Values are as the schema stores them (S3's `etag` without its quotes, `checksum_algorithm`
  its one value, the owner two columns), integers exact, unsigned columns (`uid`, `inode`, `dev`) unsigned,
  booleans `true` and `false`, every time column in UTC with six fraction digits
  (`2023-11-14T22:13:20.000000Z`). A null is not an empty string and not zero. Every source prints text rows,
  a local tree too (`swath list /data`; with `--metadata names` the fields it does not read are null). A local
  SOURCE that is a file lists as its one row, keyed by its name, with the fields its directory's listing gives
  it (the same tier; a symbolic link as itself, with `link_target`).
  `--columns key,size` (repeatable) prints those columns, by the dataset's names, in the order given, in TSV and
  JSONL; a name the source's schema lacks is refused with exit 2, the message listing its columns, before any
  request. The table's columns are fixed, so `--columns` is refused with `--format table` and with a dataset;
  on a terminal it makes `auto` TSV.
  - JSONL, one object per line, its members in column order, a null `null`, strings escaped as jq does. A string
    or binary value that is not UTF-8 (a local name's bytes) is `NAME_base64`, its exact bytes in standard
    base64, in place of `NAME` (`key_base64`, `link_target_base64`); no column name ends in `_base64`.

    ```text
    {"key":"logs/2023/app.log","size":434234,"last_modified":"2023-11-14T22:13:20.000000Z","etag":"fba9dede5f27731c9771645a39863328","storage_class":"STANDARD","owner_id":null,"owner_display_name":null,"checksum_algorithm":"CRC64NVME","checksum_type":"FULL_OBJECT"}
    ```
  - TSV: one header line of the column names, then a line per row. In a field `\\` is a backslash, `\t` a tab,
    `\n` a line feed, `\r` a carriage return and `\xHH` (two lowercase hex digits) the byte HH, written for
    every other byte below 0x20, for DEL and for every byte that is not part of valid UTF-8; every other byte
    stands for itself. A field that is exactly `\N` is null; an empty field is the empty string. Any bytes
    round-trip, a row is one line, and a terminal is never sent a control byte.

    ```text
    key	size	last_modified	etag	storage_class	owner_id	owner_display_name	checksum_algorithm	checksum_type
    logs/2023/app.log	434234	2023-11-14T22:13:20.000000Z	fba9dede5f27731c9771645a39863328	STANDARD	\N	\N	CRC64NVME	FULL_OBJECT
    ```
  - The table, for reading: a header, the time to the second in the reader's local time (`TZ` chooses it), the
    size in binary units as `swath du`'s table, then `kind` where the schema has it (a local tree), the version
    columns where the schema describes versions (`--all-versions`: the version ID column under its own name,
    `latest`, and `delete_marker` where the source has delete markers), and the key last, escaped as in TSV. A
    missing value is blank. The time and size columns have fixed widths; `kind` and the version ID are as wide
    as their widest value so far.

    ```text
    last_modified (local)      size  key
    2023-11-14 22:13:20     424 KiB  logs/2023/app.log
    ```
  A file is written to a temporary beside it and installed only when complete (synced, then renamed, the
  directory synced), replacing a previous file; a failed run leaves the destination as it was. A reader that closes
  stdout (`| head`) ends the run at once with exit 0, the report's outcome `stopped`, `stop_reason`
  `downstream_closed`, `complete` false. On stdout the one-line summary is not printed (stdout carries the
  rows; `--stats` prints the account on stderr) and `--report -` is refused (use `--report FILE`).
  Text runs keep no journal: `swath resume` refuses a text destination (stdout, a file, or a text name that is no directory: what is on disk decides, so `-o rows.tsv/` resumes as `swath resume rows.tsv/`), and an interrupted text run is run
  again. `output.text` in the report names the format, destination, compression, rows and bytes.
- **Per-prefix totals** (`swath du SOURCE [-d N]`, every source): how much is under a bucket, a prefix or a
  directory, and under each prefix down to N levels, as `du` (and `mc du`, `gsutil du`, `s5cmd du`) for buckets and
  directories alike; a local file is a source too, its one total row. Depth counts from SOURCE: from the prefix it names up to its last `/` (`s3://b/logs/2026/`
  from `logs/2026/`, `s3://b/logs` from the bucket's top; a local tree from its root), before an `--include`
  narrows the listed prefix. A prefix of depth k is a key up to and including the k-th `/` after that, and the
  total is depth 0. For the total and each prefix of depth 1 to N, `swath du` prints the objects under it, the
  directories below it (local trees), their bytes (the `size` column) and the newest modification time among
  them, a parent including its children; `-d 0`, the default, is the total alone, as `du -s`. A key with fewer
  than N `/` counts in the prefixes it has (`top.txt` only in the total). A local directory's row lists its own
  prefix (an empty directory shows 0) and counts in its parents' `dirs`, never as an object, and its time is not
  the newest's; an S3 delete marker lists its prefixes (they count toward `--max-prefixes`) and adds no object,
  bytes or time. A directory's own size is no object's (nor is it in the run report's `object_bytes`), so the
  totals' objects and bytes are the files, links and other entries. With `--all-versions` every version is an
  object. The filters apply first, to every row, directories included (**Filters** below): `--min-size 1M` keeps
  the large files and drops the directories, whose own size is small, so `dirs` counts only the directories that
  pass and a directory left with nothing is no row. The bytes are the objects' sizes as listed: not storage
  consumed, compression, replicas or a bill.

  Rows go to stdout in `--format` `auto` (the table on a terminal, TSV otherwise), `table`, `tsv` or `jsonl`. TSV
  and JSONL are sorted by prefix in byte order, the total first and a parent before its children; the table keeps
  that order for the other rows and puts the total last. `--sort size` (`name`, the default, is that order) orders
  the prefixes below each parent by size, the largest first, the prefix on a tie and a prefix of unknown size
  last, each still above the prefixes below it; the total keeps its place, in every format. Each row includes everything below it (the total
  includes every row), so add up rows of one depth only, never across depths: the depth-1 rows together leave
  out the objects directly under SOURCE, and all rows together count an object once per level it is under. The
  table is for reading: a heading of the fields' names, as the rows table's, sizes in binary units to three
  significant digits (`5.88 MiB`; bytes below 1 KiB), a local tree's files counted as `files` under `path`, the
  newest time to the second in the reader's local time (as `ls -l` and `du --time`; `TZ` chooses it), the prefix
  last, as `du` prints its paths, and the total last, named `(total)`, as `du` and `wc` end on theirs:

  ```text
      size  files  dirs  last_modified (local)  path
   695 KiB     73    11  2026-11-09 02:44:49    bench/
   429 KiB     48     2  2026-11-09 13:03:32    cmd/
  5.88 MiB    681    97  2026-11-09 13:03:32    (total)
  ```

  TSV and JSONL are for programs and are the same for every source: exact bytes and the times of text rows
  (UTC, six fraction digits), fields `depth`, `objects`, `dirs` (local trees), `size` (`allocated_size` with
  `--allocated`), `unsized` (local trees), `last_modified` and `prefix`, in that order in TSV (the numbers first
  and the name last, as `du` prints a size and then its path). `depth` is the row's level below SOURCE: 0 is the
  total, which stays the first row, in byte order; select it by `depth`, not by its place or its prefix
  (`jq 'select(.depth == 0) | .size'`; `jq 'select(.depth == 1)'` for the children). The total's `prefix` is
  SOURCE's prefix up to its last `/` (empty for a whole bucket or a local tree, so a TSV total line ends with an
  empty field: split TSV at tabs, `awk -F'\t'` or `cut`, never at runs of blanks). The fields say objects for a
  local tree's files too, so a script reads every source with the same names; only the table says `files` and
  `path`. `size` is the bytes of the objects that had a size, and `unsized` counts the files that had none
  (`--metadata names` reads no sizes), as `rclone size --json`'s `sizeless`: a size with `unsized` above 0 leaves
  those files out, and the table shows it as `-`. A time is null when no object under the prefix had one: `\N`
  in TSV, `null` in JSONL, blank in the table. TSV takes text rows' grammar, so any name round-trips; in JSONL a
  prefix that is not UTF-8 (a local name's bytes, which a JSON string cannot carry exactly) is `prefix_base64`,
  its exact bytes in standard base64, instead of `prefix`:

  ```text
  $ swath du s3://bucket/logs/ -d 2 --format tsv
  depth	objects	size	last_modified	prefix
  0	4	9	2026-01-02T04:04:05.000000Z	logs/
  1	2	7	2026-01-02T04:04:05.000000Z	logs/a/
  2	1	5	2026-01-02T04:04:05.000000Z	logs/a/b/
  1	1	1	2026-01-02T03:04:05.000000Z	logs/c/
  ```

  ```text
  $ swath du /data -d 2 --format jsonl
  {"depth":0,"prefix":"","objects":4,"dirs":4,"size":9,"unsized":0,"last_modified":"2026-01-02T04:04:05.000000Z"}
  {"depth":2,"prefix_base64":"bS//YmluLw==","objects":1,"dirs":0,"size":300,"unsized":0,"last_modified":"2026-11-09T11:38:25.000000Z"}
  ```

  Rows are written once the count completes, before the run's end is stamped and its outcome settled, so the
  report's times include the writing: a run that fails or stops before prints none; a failed write fails the run
  (`status` `failed`, a full disk `disk_full`), and so does a stop that comes while the rows are written (a signal,
  `--max-duration`), since they may be cut short; a reader that closes stdout ends it with exit 0, `status`
  `stopped`, `stop_reason` `downstream_closed`, as text output's. stdout carries the rows, so `--report -` is
  refused (use `--report FILE`, whose `output.totals` names the depth, the prefixes, their caps and the total;
  the rows are only on stdout); `--stats` prints the account on stderr. Entries a local listing could not read
  (`errors` in the source's counters: permissions, entries that vanished) are not in the totals: the run still
  exits 0, as a listing does, and `swath du` says so on stderr whatever the statistics' gating (not with `-q`, which prints no warnings).
  The count is the unordered one ([internals/ordering.md](internals/ordering.md#counting-without-order)): there is
  no faster way to a prefix's total than listing it, since a delimiter listing returns the prefixes, not their
  sizes (swath already seeds its parallel listing from them). Memory grows with the distinct prefixes, never the
  objects, and `--max-prefixes N` (default 1,000,000, about 150 MB) caps them: a run that would hold more fails
  (exit 1, cause `prefix_limit`), naming `--depth` and `--max-prefixes`, rather than print partial or rolled-up
  totals. A flat listing holds one prefix whatever the depth; a bucket of millions of top-level "folders" at
  `-d 1` is what the cap is for.
- **du's local flags** (an object store refuses them with exit 2, as it refuses the local provider's own flags;
  `swath list` and `swath diag count` refuse them as `swath du`'s). `--allocated` sums `allocated_bytes`
  (`st_blocks` × 512: what the files take on disk, as `du` reports by default; a sparse file counts its allocated
  blocks, not its length) instead of the apparent size, and the field is then `AllocatedSize`. Directories' own
  blocks are not counted (they are no objects), so `--allocated` totals are `du`'s less the directories' blocks.
  `--count-links-once` counts a file with more than one hard link once per device and inode, as `du` does by
  default (swath's default counts each name, as `du -l`): its object and bytes go to the prefixes of its first
  name in byte order, so the result does not depend on which name a worker reached first, while each name still
  adds its time. Only those inodes are remembered, and `--max-inodes N` (default 1,000,000) caps them (exit 1,
  cause `inode_limit`). Both read each entry's stat: `--metadata names` is refused (exit 2), and a platform whose
  engine does not read the values (the portable engine on Windows) fails the run at the first entry without them.
- **`--one-file-system`** (local sources, `swath list`, `swath du` and `swath diag count`): a directory on another device than the
  root (a mount point) is listed, its row kept, but not walked, as `du -x` and `find -xdev`. The listing says so:
  the provider's counter `other_device_dirs`, and a dataset's `coverage.filters` holds `one_filesystem`, so a
  reader that needs the whole tree refuses it, as it refuses a filtered one. It needs each directory's device:
  `--metadata names` is refused, and so is a platform whose engine does not read it.
- **`--no-dirs`** (local sources): directories are walked but their rows are not written. A dataset's
  `coverage.filters` holds `no_dirs`, so a reader that needs the whole tree refuses it, as it refuses a filtered
  one.
- **Filters.** `--include GLOB` and `--exclude GLOB` keep or drop keys a glob matches; `--include-regex REGEX` and
  `--exclude-regex REGEX` keep or drop keys in which the expression matches anywhere in the full key (a substring
  search). Each is repeatable, and a row is kept when there is no include or any include, glob or regex, matches,
  and no exclude matches (an exclude always wins), in whatever order they are given. A glob matches the whole of
  the key below SOURCE (the source's prefix up to its last `/`: `s3://b/logs/20` matches below `logs/`; a local
  tree's path relative to its root); rows still print full keys. A glob with a `/` is anchored there (`logs/*.gz`;
  a leading `/` only anchors: `/a.txt`), one without matches the last segment at any depth (`*.gz` keeps
  `a/b/c.gz`), and a key ending in `/` (a directory's row) matches as its name. `*` and `?` never cross `/`, `**`
  is any number of whole segments and only a segment of its own (`a/**/b`, `**/x`, `logs/**`), `[abc]`, `[a-z]`
  and `[!a-z]` are one character (never `/`), `{a,b}` is either (not nested), `\c` is `c`; an empty or
  malformed pattern is refused (exit 2). A glob filters rows, not the walk: `--exclude node_modules` drops that
  directory's row, `--exclude '**/node_modules/**'` what is below it. `--min-size` and `--max-size` keep sizes
  within the inclusive bounds, `--modified-since` and `--modified-until` keep modification times within the
  inclusive bounds (`2026-01-01`, `2026-01-01T12:00:00` or with `Z` or an offset; UTC without one; or a duration
  before the run started, `7d`, which the run records as the time it names, so a resume keeps the bound; a row
  without a time is kept, as Java's), `--storage-class A,B` (repeatable) keeps the listed classes
  (case-insensitive; a row without a class is dropped): the S3 or GCS storage class, or the Azure Blob access
  tier (`Hot`, `Cool`, `Cold`, `Archive`). A filter on a value the source does not read is refused before the run (exit 2): sizes and
  times with `--metadata names`, storage classes on a local tree. They apply to every row: on a local tree,
  directories too, by their own size and time, as `find -size` and `-newer` do. They reduce the rows written, not
  the requests made, with one exception on S3 and GCS: when every include has a literal prefix (an anchored glob's
  up to its first wildcard, a regex's `^literal`), the longest prefix common to them narrows the listed prefix
  (`--include 'logs/2026/*' --include-regex '^logs/2025/'` lists `logs/202`; the same rows, fewer requests;
  `invocation.source` names the narrowed root). A glob without `/` or a regex without `^literal` turns it off;
  excludes never change it. The report's `invocation.narrowing` says what happened: `narrowed`,
  `include_without_prefix`, `no_longer_prefix` (the common prefix does not extend the source's) or
  `provider_lists_root` (Azure and local trees filter rows but do not narrow the listing). The
  dropped rows are counted in `output.rows_filtered` (`rows_emitted` is what passed; `rows_fetched` both). The filters are part of the
  resume identity (and of `args_hash`): a resume keeps them and refuses others. A filtered dataset says so in its
  manifest: `coverage.filters` holds one description a rule (`include=…`, `include_regex=…`, `exclude=…`,
  `exclude_regex=…`, each flag's patterns sorted and once each, then `min_size=…`, `max_size=…`,
  `modified_after=…`, `modified_before=…`, `storage_class=…`), and a reader that needs the whole source refuses a
  dataset whose `coverage.filters` is not empty ([dataset.md](dataset.md#the-swath-extension)). **Regular expressions are Go's RE2,
  not Java's:** the common syntax is the same, but backreferences (`\1`), lookahead and lookbehind, possessive
  quantifiers and atomic groups do not exist and are refused (exit 2); `\Z` is not supported (`\z` is), and
  some character-class and Unicode-property details differ. RE2 runs in time linear in the key, globs included,
  so no pattern can stall a listing.
- **S3 modes.** The default is the parallel core, seeded: before any range lists, delimiter listings of the
  folder hierarchy (the top level up to 4 pages, levels narrower than `--workers` descended up to 4 deep, at
  most 64 requests in at most 6 serial waves) tile the scope, or a resumed scope after its last published key.
  The first wave also carries the scope's first data page: a complete page (the whole scope) lists a bucket of
  one page in one round trip; a truncated one is discarded, and the first tile starts at the scope's start (or
  the resumed key) as without it; a delimiter chain whose page held no folder is not
  followed ([internals/range-engine.md](internals/range-engine.md));
  `swath diag list --no-seed` starts from one
  range instead (an experiment: [measurements](measurements.md) has the seed's effect on split depth);
  `swath diag list --sequential` is the one-chain oracle and the only mode where `--max-keys` stops early (the
  dataset is then marked incomplete; such a run keeps no journal and cannot be resumed). `--endpoint-url` selects an S3-compatible store, addressed path-style by default, as Java's
  `--force-path-style` default with a custom endpoint; `--no-s3-path-style` uses virtual-host addressing there.
  Without the flag, `AWS_ENDPOINT_URL_S3`, else `AWS_ENDPOINT_URL`, is the endpoint exactly as `--endpoint-url`
  would be (path-style, the resume identity, no reference tariff) unless `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS`
  is `true`; `AWS_USE_FIPS_ENDPOINT` and `AWS_USE_DUALSTACK_ENDPOINT` set `--s3-fips` and `--s3-dualstack`
  unless the flag is given. FIPS or dual-stack with a custom endpoint, from a flag or a variable, is refused
  (exit 2): they select among AWS's own endpoints. A request answered with the bucket's region names the fix
  (`give --s3-region eu-west-1`); the region is not discovered. S3 directory buckets (Express One Zone, names ending in
  `--x-s3`) are refused before any request (exit 2, class `refused`).
- **Warm start:** `--seed-from PATH` (S3, GCS and Azure Blob) starts the range engine from a previous listing's key
  distribution instead of the delimiter seed: no seed request, and the scope tiled into up to
  `min(1000, 4 × --workers)` ranges of about equal expected keys. PATH is either a committed dataset directory of
  the same source and scope (the manifest's source kind, bucket/container and prefix must be the run's;
  its parts' bounds, row counts and key page index give the cuts). Azure's account URI is not compared:
  verify the account yourself; endpoint and region are not checked either. Alternatively, PATH can be
  a cut file: one key per line, ascending bytewise without repeats, each inside the scope, optionally followed by a tab and the keys expected since the previous cut (every line or none), at most
  262,144 lines and 16 MiB (thin a larger one: every nth line, its count the sum of those it replaces). Anything
  else is refused before listing pages (exit 2, class `refused`); Azure namespace admission requests
  can already have run. Refusals include another bucket or prefix, an unsorted or repeated
  cut, a cut outside the scope, a cut file past those bounds, the output directory itself, `--sequential`, a filesystem source. Production should
  seed from the previous dataset, which carries identity and counts; a cut file is for priors from elsewhere (an
  S3 Inventory, a prefix list, another tool's segments). A prior changes only how the work is first divided: the
  splits stay adaptive, the rows are the same. It is a runtime flag: `swath resume` reads it again and may change
  or drop it (`--seed-from=`); a resume whose prior is unreadable warns and lists without it. Counters
  `seed.from.*`; the report's `invocation.prior` ([internals/range-engine.md](internals/range-engine.md)).
- **Workers:** `--workers` (default 64 whatever the CPU count, Java's default; listing waits on the service,
  not the CPU) is the listing goroutines and the request limiter's start; `--encoders` stays CPU-derived.
- **Request admission.** S3, GCS and Azure Blob share `--no-adaptive-limit`, `--request-rate` and
  `--request-timeout`. See [object-store requests](operating.md#object-store-requests) for defaults,
  retry behavior and tuning, and the provider pages linked there for service-specific differences.
- **Requester pays:** `--s3-requester-pays` sends `x-amz-request-payer: requester` (signed) on every request, so
  a requester-pays bucket can be listed at the caller's charge.
- **Credentials (all object stores):** `--anonymous` (one flag for S3, GCS and Azure Blob) sends no
  credentials whatever the environment holds, and it is the only way a request goes unauthenticated: a
  public bucket or container needs it. Without it, a source with no usable credentials (none configured,
  or configured and broken) stops the run before listing; there is no fallback to anonymity.
  Credential failures have cause `auth` (exit 1), Azure's during namespace admission included, and read
  `S3 client: credentials (SOURCE): ...`, `GCS client: ...` or `Azure Blob client: ...`. `--anonymous` beside an
  explicit credential (`--s3-profile`, `--s3-bearer-token-command`, `--gcs-credentials-file`,
  `--azure-sas-token`, a SAS in the source URL) is refused (exit 2); credentials only the environment holds are
  ignored with one warning naming the variables (`--anonymous: ignoring the GCS credentials in the environment
  (GOOGLE_APPLICATION_CREDENTIALS)`).
- **S3 credentials:** the AWS SDK's default credential chain applies: environment, shared files and profiles
  (`--s3-profile`), SSO, web identity, credential processes, AssumeRole, container and instance roles. Its first
  lookup is waited for up to 30 s (a web-identity token file mounted late included); a refresh that fails
  transiently later is retried with each request. Secrets are never taken from flags (they would show in `ps`). Details:
  [internals/providers/s3.md](internals/providers/s3.md).
- **Bearer tokens** (S3 client; Java's flags with the s3- prefix): `--s3-bearer-token-command CMD` runs `CMD` with `/bin/sh -c`, or on
  Windows with `cmd.exe /d /s /c "CMD"` (`%ComSpec%`, so write it in cmd syntax there); its
  stdout (trimmed, one line) is a bearer token sent as `Authorization: Bearer TOKEN` on every request instead of a
  SigV4 signature, and no AWS credential is looked up (it excludes `--anonymous` and `--s3-profile`). The use is an
  S3-compatible endpoint that takes OAuth tokens, such as GCS's XML API: `swath list s3://bucket --endpoint-url
  https://storage.googleapis.com --s3-bearer-token-command 'gcloud auth print-access-token' -o DIR` (the native
  `gs://` provider is the alternative). The token is fetched again after `--s3-bearer-token-refresh-interval`
  (default 45 m, under a one-hour token lifetime), one run of the command at a time however many requests need
  it; a command that exits non-zero, runs past 30 s, prints nothing or more than one line fails the request
  with cause `auth` (fail closed: a stale token is never reused); its message carries the exit code and up to
  4,096 characters of its stderr, control characters escaped. The command is never recorded: the journal,
  `run.json`, the report, `error.json` and the stderr outcome line show `--s3-bearer-token-command=[redacted]`, so
  `swath resume DIR` must be given it again (`swath resume DIR --s3-bearer-token-command CMD`) and refuses with a
  message saying so otherwise (exit 2).
- **Spill:** when the reorder budget is full, rows of ranges ahead of the frontier go to disposable files
  under `DIR/.swath/spill/` (a text file's in a scratch directory of its attempt beside it,
  `.NAME.swath-ATTEMPT-RANDOM/spill/`, removed when the attempt ends; stdout's under `$TMPDIR`) instead of
  stalling the workers. `--spill-budget` is the one disk budget: spill stops
  (and listing waits instead) at that many bytes on disk (default 0: half the space free on the spill
  filesystem when the run starts, after the reserve) or when the filesystem would drop below the reserve, one
  part per encoder at full size and at least 1 GiB (room for the parts being written on the same filesystem).
  `swath diag list --no-spill` turns spill off and `--spill-reserve` sets the reserve; `--no-spill-compress`
  writes the frames uncompressed (by default zstd level 1 runs over the delta-coded frames); `--spill-plain` writes each key and payload whole
  instead of delta-coding them, the comparison arm
  ([internals/ordering.md](internals/ordering.md)).
- **Encoders:** the default count is GOMAXPROCS lowered to what the resources carry: one writer's working set
  and one state of the shared codec, priced from the provider's schema
  ([per-schema sizes](internals/parquet-output.md#writer-buffers-and-admission)), each from an allowance
  equal to `--row-budget`, apart from the rows, and the descriptor limit (after the spill readers,
  the workers' connections and a reserve), never fewer than one. An explicit `--encoders N` is kept as given;
  it bypasses default resource admission, with a warning when the resources cannot carry it.
  For a `.zst` text file the same flag is the zstd jobs compressed at once, 16 MiB each from that allowance (no descriptor each); one job is a
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
  journal, where nothing is created, or an unusable output; a failed write) warns of it on stderr with the
  reason (with `-v` only for a one-line failure, below); the JSON report carries the reason as
  `outcome.unrecorded`, which is never empty when the outcome is not in `error.json`. The outcome as one JSON
  line on stderr, `swath: outcome: {...}` before the error line, comes with `-v`; under `--log-format json`
  it is the `outcome` event (Log events).
  A refusal or usage error (exit 2), and a failure before any listing request was answered or directory read
  (a missing directory or bucket, credentials that do not load), print one line on stderr, `swath: MESSAGE`
  (`swath: nosuchdir: no such file or directory`), and nothing else: no statistics, no `INCOMPLETE`; a usage
  error adds its `See 'swath CMD --help'.` hint. A stop (a signal, `--max-duration`, `--stall-timeout`, a full
  disk) and a failure after requests were answered keep the statistics. `--stats` still prints them.

  | Code | Class | Meaning | Resumable |
  |---|---|---|---|
  | 0 | `completed` | the dataset is committed (or `swath du` or `swath diag count` finished), also by an earlier attempt: `swath resume` on a complete dataset | – |
  | 1 | `fatal`, `incomplete` | a run failure a resume will not fix (access denied, no such bucket, an invariant, another process owning the output; a protocol violation, `cause` `protocol`, which outranks a stop cause and which the journal records, so `swath resume` refuses the run; the part limit, `cause` `part_limit`, and an output layout swath did not make (a linked `data/`, workspace, journal or part), `cause` `unsafe_layout`, which both also outrank a stop cause; the refused paths are left as they were; the outcome is in the report, and in `error.json` only when swath could take the output lock to settle it: a refused linked workspace or journal can leave `error.json` unchanged, and the external `--report` carries the refusal), or a dataset committed with partial coverage (a key limit), so a partial dataset never exits 0 | no |
  | 2 | `usage`, `refused` | `usage`: a malformed command line or an unknown scheme; `refused`: a refusal that is the caller's to fix, before anything ran: a non-empty `-o` directory without `--overwrite`, `--overwrite` on a directory that is not this tool's dataset, a new run over an interrupted one, an output that is the listed root (or inside it, replaced), a resume whose identity changed, a resume with nothing to resume, an S3 directory bucket | `usage`: no; `refused`: the output is as it was, so yes when it holds an interrupted run |
  | 74 | `disk_full` | a part, a journal record or the manifest could not be written: its filesystem is full (`cause` `no_space`; spill waits instead of failing) | yes, journal intact, once there is space |
  | 75 | `stuck` | no progress (Liveness, below) for `--stall-timeout` (default 10 min; a warning at half; also after the 15 s shutdown grace); `cause` names what rose meanwhile: `stuck_empty_chain` (empty continuing pages past the first 1,024 of their run), `stuck_throttle` (throttle answers), `stuck_timeouts` (attempts past the request timeout, no throttle) or `stuck_no_rows` (none of these: failing or hung requests, or a stopped pipeline) | yes, journal intact |
  | 124 | `deadline` | `--max-duration` reached with the output unfinished (also after the 15 s shutdown grace) | yes |
  | 130 | `interrupted` | SIGINT (also after the 15 s shutdown grace) | yes |
  | 143 | `terminated` | SIGTERM, a graceful stop (also after the 15 s shutdown grace) | yes |

  "Resumable" holds when the output has an intact journal (S3, GCS or Azure Blob); a filesystem run starts over. A source that
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
  way), and the process exits at that bound whatever still blocks. **Forensics:** at a stall, before the run is canceled, and at a forced
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
  | `rows` | rows listed so far (a local tree's entries, directories included; with `--all-versions`, versions and delete markers) |
  | `ordered`, `encoded` | rows handed to the encoders, rows written into parts |
  | `parts` | parts published |
  | `verified_bytes` | a resume's bytes of retained parts checked so far |
  | `advance` | the sum of the above; rises in every phase |
  | `answered` | requests the service answered usably (a sign the service is up, not progress) |
  | `frontier` | the listing's pages: those that held an entry or ended a chain, and empty continuing pages within the first 1,024 of their run (the watchdog's liveness with `advance`) |
  | `empty_chain_pages` | empty continuing pages past the first 1,024 of their run: answered, followed, not progress |
  | `throttles`, `timeouts` | throttle failures; timeout failures, including credential waits before HTTP accounting starts |
  | `part_age_ms` | time since the last part was published (or since the start): publication age, not the age of the rows a resume would list again |
  | `mutex_wait_ms`, `mutex_max_hold_ms` | the range engine's decision mutex: summed wait, longest hold |
  | `rows_retained` | rows a resume kept from the interrupted run (not in `rows`) |
  | `requests`, `retries` | HTTP attempts; retry-loop repetitions, including credential-wait failures before HTTP accounting starts ([accounting](internals/clients.md#retry-policy-and-accounting)) |
  | `inflight`, `limit` | attempts in flight now; the adaptive limiter's current limit (0 without one) |
  | `cost_usd`, `cost_rate_per_1k_usd` | `requests` at the service's reference list rate, and that rate (absent without a known tariff, including Azure Blob and custom endpoints) |
  | `order.nodes`, `order.markers`, `order.spill_readers`, `order.spill_live_bytes`, `order.ahead_rows` | the order stage's live resources, each with a `peak_` twin (`peak_spill_live_bytes` for spill bytes); `ahead_rows` is rows listed and not yet handed on (in memory or spilled) |
  | `started_at`, `as_of`, `duration_ms` | timing |

  A supervisor watches that file; `DIR/run.json` follows the
  commit. `--max-duration` and `--stall-timeout` are runtime flags: `swath resume` replays the recorded
  values unless given again.
- **Terminal** (Java's stderr surface). A live progress record on stderr, from the snapshot the progress file
  is written from: the phase (`starting`, `verifying` with a resume's bytes checked, `listing`), objects listed
  (`N objects`; a local tree's `N entries`, `N rows` with `--all-versions`; with `+N recovered`: rows a resume
  kept), the live and average rate in the same noun (`keys/s` for a store, `entries/s`, `rows/s`), the parts
  published (a dataset run's), requests in flight,
  elapsed time since the command began, API calls and, on a known tariff, the labeled estimate; no percentage
  or ETA (a listing has no honest denominator), no key text. On a terminal it is redrawn in place (carriage
  return, `ESC[2K`; on Windows after enabling virtual-terminal processing, else plain lines), its trailing
  fields dropped whole to fit the width, every second; elsewhere it is appended as plain lines every 30 s.
  Shown by default when stderr is a terminal, neither `-q` nor `-v` is given and stdout is not a terminal that
  carries rows (`swath list` without `-o`, `swath du`: the two share the screen, as aws, rclone and fd show no
  progress there); `--progress` forces it,
  `--no-progress` suppresses it, `--progress-interval D` (at least 1 s) sets the cadence and implies it.
  Warnings written while the line is drawn erase it and repaint it after. At the end, `--stats` prints Java's
  block on stderr: a disposition (`INCOMPLETE (class)` with `resume: swath resume DIR` for a resumable stop; a
  refused run prints only this line), `N objects in T · K keys/s` (the progress record's nouns; with the whole
  invocation's time when it is over a second longer), `N API calls · in flight avg A · peak P` (the calls per
  1,000 rows are the report's `efficiency.http_attempts_per_1k_rows`), throttles, retries and entry errors when
  any, the estimate, and what was written: `N parts` (Parquet) or `1 file` (a `.tsv` or `.jsonl` output), the
  bytes written and the peak RSS, every size in binary units (`3.7 KiB`, as the tables). Without `--stats` it is printed
  for a run over 1.5 s, a run that wrote parts, and a run that did not complete; not with `-q`, not after a
  closed stdout reader; `--no-stats` never. stdout carries rows or nothing: outside `swath diag count` no
  account is printed there (a program reads `--report`). Verbosity: by default
  stderr carries warnings and errors; `-v` adds a start line saying where the rows go (`swath: listing SOURCE
  to stdout`, `to DIR`, `to FILE`, `as totals`, `as a count`, with the attempt id) and shows progress as
  appended records (not when stdout is a terminal that carries rows, unless `--progress` is given, as for the
  automatic line); `-vv` adds each failed request (no answer, or a status of 400 or more) and each failed
  attempt of the retry loop (retried, throttled, or the last) with its cause; `-vvv` every request
  (method, host, path and query with every value but the listing's own parameters, such as `prefix`,
  redacted, so no SAS, token or credential, then status and latency). `-q` drops warnings and the automatic
  progress and statistics; `-qq` also drops the final error line (the exit code, `error.json` and `--report`
  stand). Without `-v`, a request failure that persists is shown: when requests have failed with one
  transport or service cause (a connection refused, a 503) for 10 s with none answered, one warning names
  the provider, the endpoint host and the cause (`swath: warning: S3 127.0.0.1:9: connection refused
  (retrying; 14 attempts)`), and again at most once a minute while it lasts; a run that then ends stuck or
  at its deadline names the cause in its final error line, and `requests.failure_warnings` in the report
  counts the warnings. A stderr that does not drain never holds the run: a line waits at most 250 ms, later
  ones queue (up to 256) without waiting while the write is stuck and are then dropped, counted in
  `requests.log_dropped`; the final error line gets its own 250 ms. A local listing that could not read entries names up to five of them with their cause,
  then the count of the rest, and still exits 0; the report's `outcome.unreadable` is then true. On Unix,
  SIGUSR1 (and SIGINFO, Ctrl-T, on macOS and the BSDs) prints one progress record on stderr in the log
  format, whatever the progress settings (as GNU dd). `--color auto|always|never` colors the progress line
  and the statistics (headline cyan, the rest dim, `INCOMPLETE` red): auto is on for a terminal unless
  `NO_COLOR` is set and not empty or `TERM=dumb`, and `CLICOLOR_FORCE` forces it. TTY detection and width:
  `golang.org/x/term`.
- **Log format.** `--log-format text|json` (before or after the command word, and again on `swath resume`)
  chooses how those lines are written: `text`, the prose above; `json`, one JSON object per line and nothing
  else, no colour, no redrawn line and no statistics block (the `outcome` event and `--report` stand for it),
  with progress records appended at `--progress-interval` (30 s by default) whether or not stderr is a
  terminal, under `-v` or `--progress`. The verbosity flags gate the events as they gate the text, except
  the `outcome` event, which ends every run and every failure unless `-qq` is given.
- **Environment:** `SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` name pprof output files;
  `SWATH_LOCALFS_ENGINE=portable` selects the portable filesystem engine; `SWATH_SPLIT_TRACE` traces range-engine splits on stderr, one line per split (range id,
  bounds, cursor, pivot, keys listed, the pivot's source, the children's depth, the range's gallop generation
  and the anchor its upper child carries).

## Log events

Under `--log-format json` each stderr line is one object: `ts` (RFC 3339, UTC, milliseconds), `level`
(`debug`, `info`, `warn`, `error`), `event`, `attempt` (the attempt id, once a run has one), `msg` (the text
line's words), then the event's fields:

| Event | Level | Fields |
|---|---|---|
| `start` | `info` (`-v`) | `source`, `to` (where the rows go) |
| `progress` | `info` | the progress snapshot's fields, as `.swath/progress.json` has them (`rows`, `requests`, `duration_ms`, ...) |
| `warning` | `warn` | none, or: a persistent failure's `host`, `cause`, `attempts`; unreadable entries' `unreadable` (their count) and `samples`; a goroutine dump's `stacks` |
| `request` | `debug` (`-vv` failed, `-vvv` all) | `op` (the method), `host`, `path` (redacted as above), `status` (absent without an answer), `cause` (without an answer), `latency_ms` |
| `retry` | `debug` (`-vv`) | `try` (the call's attempt, from 1), `class` (the diagnostic label), `status`, `code` (the provider's), `cause`, `retry`, `wait_ms` |
| `outcome` | `info`, `error` on failure | `class`, `exit_code`, `resumable`, `cause`, `message` (the report's error) |

Pinned by `TestEventGolden` (`UPDATE=1 go test ./cmd/swath` rewrites this block):

<!-- events:begin -->
```json
{"ts":"2026-11-10T12:00:00.000Z","level":"info","event":"start","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"listing s3://bucket/logs/ to stdout","source":"s3://bucket/logs/","to":"to stdout"}
{"ts":"2026-11-10T12:00:00.000Z","level":"info","event":"progress","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"listing · 120,000 objects · 2,000 keys/s (avg 2,000)","rows":120000,"ordered":0,"encoded":0,"parts":0,"verified_bytes":0,"advance":120000,"answered":121,"frontier":121,"empty_chain_pages":0,"throttles":0,"timeouts":0,"rows_retained":0,"requests":122,"retries":1,"inflight":8,"limit":64,"mutex_wait_ms":0,"mutex_max_hold_ms":0,"started_at":"2026-11-10T11:59:00Z","as_of":"2026-11-10T12:00:00Z","duration_ms":60000,"part_age_ms":60000}
{"ts":"2026-11-10T12:00:00.000Z","level":"warn","event":"warning","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"S3 127.0.0.1:9: connection refused (retrying; 11 attempts)","host":"127.0.0.1:9","cause":"connection refused","attempts":11}
{"ts":"2026-11-10T12:00:00.000Z","level":"debug","event":"request","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"request GET acct.blob.core.windows.net/c?comp=list&marker=[redacted]&prefix=logs%2F&restype=container&sig=[redacted]&sv=[redacted]: 200 (42ms)","op":"GET","host":"acct.blob.core.windows.net","path":"/c?comp=list&marker=[redacted]&prefix=logs%2F&restype=container&sig=[redacted]&sv=[redacted]","status":200,"latency_ms":42}
{"ts":"2026-11-10T12:00:00.000Z","level":"debug","event":"retry","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"attempt 2 failed (throttle): 503 SlowDown: Please reduce your request rate.; retrying in 400ms","try":2,"class":"throttle","status":503,"code":"SlowDown","cause":"503 SlowDown: Please reduce your request rate.","retry":true,"wait_ms":400}
{"ts":"2026-11-10T12:00:00.000Z","level":"error","event":"outcome","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"stuck: no progress within the stall timeout (stuck_no_rows)","class":"stuck","exit_code":75,"resumable":true,"cause":"stuck_no_rows","message":"stuck: no progress within the stall timeout (stuck_no_rows)"}
```
<!-- events:end -->

## Run report

<!-- report:begin -->
One schema (`listing.Report`, `report_version` 4) for `run.json` beside a committed dataset, `--report FILE`,
and what `--report -` prints. It is operational: readers accept a dataset by its manifest, never by
this report. The schema is pinned by `internal/listing/testdata/report-schema.golden` (`TestReportSchemaGolden`;
a change bumps `report_version` and is listed in [upgrade.md](upgrade.md#run-report-and-counters)), and
`TestReportSchemaDoc` keeps this section naming every field. It accounts for this invocation (rows it fetched
and emitted, requests it made) separately from the dataset (rows retained from an interrupted run, durable,
committed). Durations are integers with the unit in the name; a measurement that was not taken is absent,
never zero.

| Field | Meaning |
|---|---|
| `report_version` | 4 |
| `build` (`tool`, `version`, `revision`) | the binary: `swath-go`, the Go module version (`devel` for a binary built from a source checkout), the VCS revision (`-dirty` if modified) |
| `attempt` (`id`, `started`) | this invocation: 128 random bits in hex, and when the command began; `error.json` carries the same |
| `invocation` | what was asked: `argv` (secret flags and endpoint values carrying user information `[redacted]`), `source` (the root, narrowed by the includes' literal prefix on S3 and GCS), `narrowing` (with includes: `narrowed`, or why not, **Filters** above), `out`, `resume`, `schema_id`, `fields` (the metadata tier), `workers`, `encoders`, `row_budget_bytes` (the one row budget), `part_size_bytes`, `prior` (`--seed-from`: `path`, `source` `dataset` or `cuts`, `cuts` offered, `counted`) |
| `outcome` | `status` (`committed`, `written`, `listed`, `stopped`, `failed`), `error`, `complete`, `stop_reason`, `stop_source`, `entry_errors`, `unreadable` (some entries could not be read: the rows and totals leave them out, exit 0), `class`, `exit_code`, `resumable`, `cause`, `unrecorded` (Exit status above) |
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
what the filters dropped of it, `rows_emitted` what was handed on in order (swath du, swath diag count: counted). On a run that
succeeded, `rows_fetched` = `rows_filtered` + `rows_emitted`; a run that stopped has also fetched rows ahead of
the frontier that it never emitted. `rows_retained` is kept from an interrupted run, `rows_durable` in parts
that stay in the output (the committed dataset's, or after a failure the journaled parts'), `rows_committed` in
the committed dataset (`rows_retained` + `rows_emitted`). `parts_written` and `parts_retained` count parts.
`object_bytes` sums the sizes of the objects the order stage accepted (the size column; absent for a schema without one; a local directory's own size is no object's, so a local tree's is the apparent bytes of the files accepted after the filters): on a committed run exactly those of `rows_emitted`, on a run that stopped also those accepted and never handed on;
`output_bytes` is the dataset's part bytes. `text` is a text run's: `format`, `destination`, `compression`,
`rows`, `bytes` (delivered, compressed), `sink_wait_ns`, `output_wait_ns`, `formatters`, and `compressor`
(`bytes_in`, `bytes_out`, `jobs`, `jobs_reason`: `cores`, `row_budget`, `encoders`, `gzip`). `totals` is a
`swath du` run's (absent otherwise): `depth`, `prefixes` (the rows printed, the total included),
`max_prefixes` (the cap), `bytes_column` (the column summed: `size`, or `allocated_bytes` with `--allocated`),
`objects`, `bytes` and `dirs` (the total's; `objects` excludes directories), `not_objects` (rows counted as no object: directories, delete
markers), `unsized` (objects without a value in the bytes column: the sizes are then unknown), `links_once`
(`--count-links-once`), `linked_rows` (rows with more than one hard link), `inodes` (the distinct inodes among
them, each counted once) and `max_inodes` (their cap).

For versions schemas only, `versions`, `distinct_keys` and `live_objects` describe this attempt's
rows after filtering at the order stage's handoff (swath du, swath diag count: validated, counted rows).
`versions` excludes S3 delete markers; S3 also reports `delete_markers`, with
`rows_emitted` = `versions` + `delete_markers`. GCS reports `noncurrent` among its
`versions`, with `versions` = `noncurrent` + `live_objects`. A live S3 object has
`is_latest` true and `is_delete_marker` false; a live GCS generation has null
`time_deleted`. Zero counts are present. Retained resume rows, held tails, relisting,
and raw overfetch are excluded. `distinct_keys` comes from `order.Stats.DistinctKeys`
and excludes pending batches that never reached the sink, including on a failed run.

Object-store failure diagnostics retain `auth` (unusable credentials), `access_denied` (a listing
permission refusal), `throttle`, `timeout`, `parse` and `unsupported`. These labels describe the failure;
retry and engine requeue use its stored transient signal. Other service errors retain their HTTP status
and provider code or reason in the message without a separate report label. Invalid pages remain protocol
violations; caller cancellation keeps its stop disposition.

**Requests** (`requests`, `cost`, `efficiency`; absent for a source that makes no requests). `http_attempts`
is the one request count: every counted HTTP attempt (pages, probes, client retries, engine requeues).
`retries` counts retry-loop repetitions, `throttles` counts throttle failures and `timeouts` counts
timeout failures. Credential waits can fail before HTTP accounting starts, so `retries` and `timeouts`
can increase with zero `http_attempts`; they are not subsets of the HTTP count. `failure_warnings` counts the persistent-failure warnings printed on stderr (Terminal), `log_dropped` the
stderr lines dropped because stderr did not drain. See
[retry accounting](internals/clients.md#retry-policy-and-accounting). `cost_usd` is `http_attempts` at `basis` `rate_per_1k_usd` (0.005, `basis` `source`
`aws-list-reference-rate` for AWS S3, `gcs-class-a-reference-rate` for GCS's own endpoint, however
`--endpoint-url` spells it); both `null` on a
custom endpoint, whose tariff is unknown, and for every Azure Blob run (no tariff estimate). `keys_per_sec` is `rows_emitted` over `listing_ms`;
`http_attempts_per_1k_rows` is `http_attempts` per 1,000 `rows_fetched` (`null` without a row). Latency
percentiles and in-flight time are the provider's counters (`source.stats.counters.values` `inflight.p50_ns`,
`inflight.p90_ns`, `inflight.p99_ns`, `inflight.busy_ns`).

**Resources** (`resources`). `peak_rss_bytes` (with `rusage_available`), `peak_heap_live_bytes` (sampled every
100 ms), `max_buffered_bytes` (peak reorder storage, against `invocation.row_budget_bytes`), `free_disk_min_bytes`
(the least free space seen, every second, on the filesystem the run writes to: the spill directory's, else the
output's; absent for swath du and swath diag count), `peak_ahead_rows`, `frontier_stuck_max_ns`, `frontier_wait_ns`,
`budget_wait_ns` and `encoder_wait_ns` (summed over goroutines: not elapsed time), `peak_nodes`, `peak_markers`,
`part_time_cuts`, `part_row_cuts`, `features` (the pipeline's policies, on or off), and `spill` (absent with spill
off): `rows`, `bytes`, `logical_bytes`, `format`, `decode_refused`, `ranges`, `peak_bytes`, `refused`,
`max_bytes`, `reserve_bytes`, `peak_readers`, `dir`, `fs_type`, and with durable spill
([internals/ordering.md](internals/ordering.md#durable-spill)) `durable`: `slots` (the certificate cap),
`history` and `history_cap` (ledger records against H), `captured_ranges`, `capture_at` (by where the capture
began), `capture_pending_dropped`, `no_slot`, `history_exhausted`, `abandoned` (by reason), `sealed_files`,
`sealed_durable`, `flush_ns`, `flush_cycles`, `ledger_failed`, `retained_bytes`, `peak_retained_bytes`, `retired`
(by reason), `retire_deferred`, and on a resume `resume`: `ledger_dropped_bytes`, `ledger_cut`, `accepted`,
`accepted_bytes`, `rejected` (by reason), `orphans`, `replayed_rows`, `replay_skipped_rows`, `gaps`,
`requests_saved_est`. `durable_off` says why durable spill is off when it could have run. `drain` (sorted Parquet; absent otherwise) is the
drain: its tail once listing is done, and the drainer's times over the whole run
([internals/ordering.md](internals/ordering.md), every field in
[internals/counters.md](internals/counters.md)): `tail_ns`,
`worker_idle_after_listing_done_ns`, the drainer's `replay_read_ns`, `replay_decode_ns`, `ahead_wait_ns`,
`order_check_ns` and `sink_write_ns`; `read_ahead` (`frames`, `rows`, `drainer_frames`, `unplanned_frames`,
`drainer_waits`, `capped_waits`, `read_ns`, `decode_ns`, `max_reads`); `segments`, the parallel tail (`reason`,
`segments`, `workers`, `rows`, `max_rows`, `seam_checks`, `time_ns`, `max_time_ns`, `plan_ns`, `read_ns`,
`decode_ns`, `write_ns`, `max_workers`, `budget_waits`, `budget_wait_ns`, `max_held_bytes`, `workers_by`,
`per_worker_bytes`, `free_bytes`, `tree_bytes`, `durable_files_read`, `durable_boundaries`, `deferred_rows`); and `encoders_at_start`, `encoders_after_listing`,
`encoder_admission`. In `timings`, `listing_done_at` is when the provider finished (every range expanded).

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
