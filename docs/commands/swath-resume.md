# swath resume

swath resume continues an interrupted dataset run with the command its journal recorded.

## Synopsis

<!-- synopsis:begin -->
```text
swath resume DIR [OPTIONS]
```
<!-- synopsis:end -->

## Description

A dataset run of an object store (`swath list s3://..., gs://... or az://... -o DIR`) records its command in a
journal under `DIR/.swath/` and publishes parts as it goes. When it stops before committing (a signal,
`--max-duration`, `--stall-timeout`, a full disk, a crash), `swath resume DIR` keeps every published part,
checks each against the journal (size and SHA-256), and lists from after the last published key:

```text
$ swath list s3://bkt/logs/ -o logs-inventory ...
^C  INCOMPLETE (interrupted) — resume: swath resume logs-inventory
  1,000 objects in 6.0s · 167 keys/s
  60 API calls · in flight avg 0.07 · peak 1
  1 part · 3.19 KiB written · peak RSS 44.9 MiB
swath: stopped by interrupt
$ swath resume logs-inventory
  59,000 objects in 19.6s · 3,006 keys/s
  153 API calls · in flight avg 0.11 · peak 1
  4 parts · 111 KiB written · peak RSS 69.3 MiB
$ swath resume logs-inventory
logs-inventory is already complete: nothing to resume
```

The `resume:` advice quotes a DIR the shell would split or expand (a path with a space; single quotes, double
quotes on Windows), and spells
one that begins with `-` as `./-DIR`, so it can be pasted as printed. [`swath show DIR`](swath-show.md) prints where a resume will continue before you run it.

A run that writes to a bucket (`-o gs://BUCKET/PREFIX`, `-o s3://BUCKET/PREFIX` or `-o az://ACCOUNT/CONTAINER/PREFIX`, with `--work-dir DIR`) is resumed by its work directory:
`swath resume DIR` replays the recorded command with that work directory (it may have moved; the bucket and
prefix stay as recorded). Each retained part's object is read whole and checked against its record (its
version: the GCS generation, the S3 version id or ETag, the Azure ETag; its size and SHA-256), and a resume of a run that reached
its commit first reads the bucket's manifest back: the run's own means the run is complete, and the resume first
finishes what the commit left (this run's leftover parts deleted, `run.json` beside the manifest). A commit that
ended unresolved (exit 75) is resumed the same way: the resume creates the very manifest the run saved. The
destination's credentials may be given again (`--output-gcs-credentials-file`, `--output-s3-profile`), and an
Azure SAS must be (`--output-azure-sas-token`, never recorded); its bucket (Azure: account and container),
prefix, endpoint and S3 region are fixed. A lost work directory loses the
resume: the parts already uploaded are not reused. A new run at the same prefix is not refused (only a prefix
holding a `manifest.json` is), but nothing removes the lost run's parts, so start it at a new prefix
([production.md](../production.md#on-a-preemptible-or-spot-machine)).

Rows listed but not yet in a published part are listed again, except those a validated spill file still
holds ([running.md](../running.md#stopping-and-resuming)).

The source, the route (endpoint, addressing, region), the filters, the schema and the part size are fixed for
the run. Only the options below may be given again; the last value wins. A switch the run was started with is
undone by its other spelling: `--no-anonymous` undoes `--anonymous`, `--adaptive-limit` undoes
`--no-adaptive-limit`. Any other option is a usage error:

```text
$ swath resume logs-inventory --part-size 1MiB
swath: --part-size is fixed for the run and cannot change on resume (see 'swath resume --help')
See 'swath resume --help'.
```

Secrets are never recorded: a run started with `--azure-sas-token` (or a SAS in `AZURE_STORAGE_SAS_TOKEN`) or
`--s3-bearer-token-command` must be given it again.

A resume of a dataset that is already complete has nothing to do and exits 0, so a supervisor may resume until
exit 0. A dataset committed with partial coverage (a `swath diag list --max-keys` sample) is a finalized
partial sample: nothing continues it, and its resume exits 1 (class `incomplete`), saying so:

```text
swath: sample holds a finalized partial sample (coverage complete=false stop=max_keys): it cannot be resumed
```
 Local trees and text output keep no journal; resume refuses them, and they are run again. DIR is
judged by what is on disk, not by its name: a directory named `rows.tsv/` that holds an interrupted dataset run
resumes, and a text file `rows.tsv` is refused as text output.

## Options

<!-- flags:begin -->
```text
Output:
      --output-gcs-credentials-file FILE
                              credentials for a GCS -o (default: Application Default Credentials)
                                A service account key or authorized-user JSON, with the read-write
                                scope. The destination's own: the source's credentials and
                                --anonymous never apply to it. It may change on resume.
      --output-s3-profile NAME
                              the shared config profile an S3 -o takes credentials (and region) from
                                Default: the AWS SDK chain (environment, AWS_PROFILE or default,
                                SSO, web identity, IMDS). The destination's own: the source's
                                credentials and --anonymous never apply to it, and its requests are
                                always signed. It may change on resume.
      --output-azure-sas-token TOKEN
                              a shared access signature for an Azure -o; never recorded
                                Default: Entra ID (DefaultAzureCredential). The destination's own:
                                the source's SAS, key and --anonymous never apply to it. Give it
                                again to swath resume; it may change there.

Object stores:
      --anonymous             send no credentials (a public bucket or container)
                                Ignores any configured. Without it a source with no usable
                                credentials fails (auth) before listing.
      --ca-bundle FILE        trust the certificates in this PEM file besides the system's
                                For a service behind a private certificate authority. S3: default
                                AWS_CA_BUNDLE.
      --no-verify-ssl         do not verify the service's TLS certificate (warns)
                                For a test endpoint only. A resume replays it, and warns again.
      --no-anonymous          undo a recorded --anonymous
      --verify-ssl            undo a recorded --no-verify-ssl

S3:
      --s3-profile NAME       the shared config profile to take credentials and region from
                                Default: the AWS SDK chain (environment, AWS_PROFILE or default,
                                SSO, web identity, IMDS).
      --s3-bearer-token-command COMMAND
                              a shell command printing a bearer token, sent instead of SigV4
                                For GCS's XML API: 'gcloud auth print-access-token'. Never recorded:
                                swath resume needs it again.
      --s3-bearer-token-refresh-interval DURATION
                              run --s3-bearer-token-command again after this long (default 45m)

GCS:
      --gcs-credentials-file FILE
                              a service-account or authorized-user JSON file (default: ADC)
                                Without it, Application Default Credentials.

Azure Blob:
      --azure-sas-token TOKEN
                              a shared access signature (a query string); never recorded
                                Else a SAS in the source's query, else AZURE_STORAGE_SAS_TOKEN; with
                                none, an account key in AZURE_STORAGE_KEY (Shared Key), else Entra
                                ID (DefaultAzureCredential). Give it again to swath resume.

Performance:
  -j, --workers N             listing workers (default 64; a local tree: 4 per CPU, at most 64)
                                Raise it (128, 256) for a large listing the service does not
                                throttle: listing waits on round trips, not the CPU; lower it when
                                it throttles at the default.
      --encoders N            Parquet encoders, or a .zst file's compression jobs (default: CPUs)
                                Lower it to leave cores to other work; the default follows
                                GOMAXPROCS, the descriptor limit and an encoder allowance of 1/16 of
                                memory (as the default sort buffer, whatever --sort-buffer says; at
                                most 1GiB). An explicit count is kept, with a warning when they
                                cannot carry it.
      --sort-buffer SIZE      bytes buffered for sorted output; not a total-memory limit
                                Rows listed ahead of the output, the output queues and the
                                scheduler's metadata. Past it rows spill to disk; with no spill
                                space listing waits. Request pages, directory reads, writers,
                                compression and the Go runtime are extra, and the tail after listing
                                holds about one more; encoders are admitted against an allowance of
                                their own, so this sizes rows only. Default: 1/16 of the least of
                                detected physical memory, the cgroup's limit and GOMEMLIMIT, at most
                                1GiB (128MiB when none is detected; invocation.sort_buffer says
                                which). Raise it when spill is heavy and memory is free; to use less
                                memory lower it with --workers and --encoders.
      --spill-budget SIZE     cap on spill (default: half the output filesystem's free space)
                                Set it when the output filesystem is shared or small. Past it, or at
                                the reserve, listing waits instead of spilling.
      --part-interval DURATION
                              close a part with rows after this long, full or not (default 30s)
                                0: by size only. Lower it to close parts sooner on a slow listing
                                (it bounds how long rows wait in an open part, not how much a resume
                                repeats: a closed part still publishes in order); 0 for part
                                boundaries independent of timing.
      --seed-from PATH        object stores: start from a previous listing's key distribution
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
      --adaptive-limit        undo a recorded --no-adaptive-limit

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

Continue an interrupted run:

```bash
swath resume logs-inventory
```

Continue it with more workers, a deadline and a fresh report path:

```bash
swath resume logs-inventory --workers 128 --max-duration 1h --report /var/log/swath/attempt-2.json
```

Continue a run whose SAS token is not recorded:

```bash
swath resume container-inventory --azure-sas-token "$SAS"
```

## Exit status

The codes of [`swath list`](swath-list.md#exit-status). 2 also when the directory holds no interrupted run, or
when the environment resolves another route than the run's (another region or endpoint): the directory is
left as it was. 0 when the dataset is already complete; 1 when it is a finalized partial sample. See [reference/exit-codes.md](../reference/exit-codes.md).

## Environment

The provider's credential variables ([sources.md](../sources.md)); a resume reads them again.

## Files

`DIR/.swath/journal` (the recorded command and the published parts; `swath show DIR --journal` prints it) and
`DIR/error.json` (the last attempt's outcome).

## See also

[swath-list](swath-list.md), [swath-show](swath-show.md), [running.md](../running.md#stopping-and-resuming),
[reference/exit-codes.md](../reference/exit-codes.md)
