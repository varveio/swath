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
^C  interrupted: 1,000 saved, 25,000 listed in 6.0s · 4,173 keys/s → logs-inventory (1 part, 6.05 KiB)
  next: swath resume logs-inventory
swath: stopped by interrupt
$ swath resume logs-inventory
  complete: 60,000 objects (1,000 from earlier attempts), 559 GiB in 24.0s · 2,457 keys/s → logs-inventory (7 parts, 281 KiB)
  next: swath show logs-inventory · swath du --from-dataset logs-inventory
$ swath resume logs-inventory
logs-inventory is already complete: nothing to resume
```

The rows saved are the ones in published parts, which the resume keeps; the rows listed and not yet published
are listed again, except those a validated spill file still holds. The `next:` line quotes a DIR the shell would split or expand (a path with a space; single quotes, double
quotes on Windows), and spells
one that begins with `-` as `./-DIR`, so it can be pasted as printed. [`swath show DIR`](swath-show.md) prints where a resume will continue before you run it.

The source, the route (endpoint, addressing, region), the filters, the schema and the part size are fixed for
the run. Only the options below may be given again; the last value wins. A switch the run was started with is
undone by its other spelling: `--no-anonymous` undoes `--anonymous`, `--adaptive-limit` undoes
`--no-adaptive-limit`. Any other option is a usage error:

```text
$ swath resume logs-inventory --part-size 1MiB
swath: --part-size is fixed for the run and cannot change on resume (see 'swath resume --help')
See 'swath resume --help'.
```

What a resume keeps and lists again, secrets, a dataset in a bucket, complete and partial datasets, and the
refused resumes are in [Stop, resume and refresh](../resume-and-refresh.md#stopping-and-resuming).

## Options

<!-- flags:begin -->
### Output

- <a id="option-output-gcs-credentials-file"></a>[`--output-gcs-credentials-file FILE`](#option-output-gcs-credentials-file)\
  credentials for a GCS -o (default: Application Default Credentials)

  A service account key or authorized-user JSON, with the read-write scope. The destination's own: the source's credentials and --anonymous never apply to it. It may change on resume.

- <a id="option-output-s3-profile"></a>[`--output-s3-profile NAME`](#option-output-s3-profile)\
  the shared config profile an S3 -o takes credentials (and region) from

  Default: the AWS SDK chain (environment, AWS_PROFILE or default, SSO, web identity, IMDS). The destination's own: the source's credentials and --anonymous never apply to it, and its requests are always signed. It may change on resume.

- <a id="option-output-azure-sas-token"></a>[`--output-azure-sas-token TOKEN`](#option-output-azure-sas-token)\
  a shared access signature for an Azure -o; never recorded

  Default: Entra ID (DefaultAzureCredential). The destination's own: the source's SAS, key and --anonymous never apply to it. Give it again to swath resume; it may change there.

### Object stores

- <a id="option-anonymous"></a>[`--anonymous`](#option-anonymous)\
  send no credentials (a public bucket or container)

  Ignores any configured. Without it a source with no usable credentials fails (auth) before listing.

- <a id="option-ca-bundle"></a>[`--ca-bundle FILE`](#option-ca-bundle)\
  trust the certificates in this PEM file besides the system's

  For a service behind a private certificate authority. S3: default AWS_CA_BUNDLE.

- <a id="option-no-verify-ssl"></a>[`--no-verify-ssl`](#option-no-verify-ssl)\
  do not verify the service's TLS certificate (warns)

  For a test endpoint only. A resume replays it, and warns again.

- <a id="option-no-anonymous"></a>[`--no-anonymous`](#option-no-anonymous)\
  undo a recorded --anonymous

- <a id="option-verify-ssl"></a>[`--verify-ssl`](#option-verify-ssl)\
  undo a recorded --no-verify-ssl

### S3

- <a id="option-s3-profile"></a>[`--s3-profile NAME`](#option-s3-profile)\
  the shared config profile to take credentials and region from

  Default: the AWS SDK chain (environment, AWS_PROFILE or default, SSO, web identity, IMDS).

- <a id="option-s3-bearer-token-command"></a>[`--s3-bearer-token-command COMMAND`](#option-s3-bearer-token-command)\
  a shell command printing a bearer token, sent instead of SigV4

  For GCS's XML API: 'gcloud auth print-access-token'. Never recorded: swath resume needs it again.

- <a id="option-s3-bearer-token-refresh-interval"></a>[`--s3-bearer-token-refresh-interval DURATION`](#option-s3-bearer-token-refresh-interval)\
  run --s3-bearer-token-command again after this long (default `45m`)

### GCS

- <a id="option-gcs-credentials-file"></a>[`--gcs-credentials-file FILE`](#option-gcs-credentials-file)\
  a service-account or authorized-user JSON file (default: ADC)

  Without it, Application Default Credentials.

### Azure Blob

- <a id="option-azure-sas-token"></a>[`--azure-sas-token TOKEN`](#option-azure-sas-token)\
  a shared access signature (a query string); never recorded

  Else a SAS in the source's query, else AZURE_STORAGE_SAS_TOKEN; with none, an account key in AZURE_STORAGE_KEY (Shared Key), else Entra ID (DefaultAzureCredential). Give it again to swath resume.

### Performance

- <a id="option-workers"></a>[`-j, --workers N`](#option-workers)\
  listing workers (default 64; a local tree: 4 per CPU, at most 64)

  Raise it (128, 256) for a large listing the service does not throttle: listing waits on round trips, not the CPU; lower it when it throttles at the default.

- <a id="option-encoders"></a>[`--encoders N`](#option-encoders)\
  Parquet encoders, or a .zst file's compression jobs (default: CPUs)

  Lower it to leave cores to other work; the default follows GOMAXPROCS, the descriptor limit and an encoder allowance of 1/16 of memory (as the default sort buffer, whatever --sort-buffer says; at most 1GiB). An explicit count is kept, with a warning when they cannot carry it.

- <a id="option-sort-buffer"></a>[`--sort-buffer SIZE`](#option-sort-buffer)\
  bytes buffered for sorted output; not a total-memory limit

  Rows listed ahead of the output, the output queues and the scheduler's metadata. Past it rows spill to disk; with no spill space listing waits. Request pages, directory reads, writers, compression and the Go runtime are extra, and the tail after listing holds about one more; encoders are admitted against an allowance of their own, so this sizes rows only. Default: 1/16 of the least of detected physical memory, the cgroup's limit and GOMEMLIMIT, at most 1GiB (128MiB when none is detected; the report's diagnostics.sort_buffer says which). Raise it when spill is heavy and memory is free; to use less memory lower it with --workers and --encoders.

- <a id="option-spill-budget"></a>[`--spill-budget SIZE`](#option-spill-budget)\
  fixed cap on spill (default: follows the spill disk's free space)

  By default spill may use the spill filesystem's free space after a reserve (half of it unless the dataset is in a bucket, as the output shares it), re-read while it holds listing back, so a disk grown under a live run is used. Set it when the filesystem is shared with other work. Past it, or at the reserve, listing waits instead of spilling.

- <a id="option-part-interval"></a>[`--part-interval DURATION`](#option-part-interval)\
  close a part with rows after this long, full or not (default `30s`)

  0: by size only. Lower it to close parts sooner on a slow listing (it bounds how long rows wait in an open part, not how much a resume repeats: a closed part still publishes in order); 0 for part boundaries independent of timing.

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

- <a id="option-spill-wait-timeout"></a>[`--spill-wait-timeout DURATION`](#option-spill-wait-timeout)\
  stop after waiting this long on a full spill cap (exit 74); 0: wait

  Listing waits on the output while spill is refused, which on a large bucket can be close to a stall; after 2 minutes swath says so. Set it on an unattended run to stop instead (cause spill_budget), then resume with more disk or a larger --spill-budget. Only a dataset run of an object store (one with a journal) resumes where it stopped.

- <a id="option-request-timeout"></a>[`--request-timeout DURATION`](#option-request-timeout)\
  per-request timeout; a request over it is retried (default `30s`)

  Raise it for a slow endpoint or emulator whose pages take longer to answer.

- <a id="option-request-rate"></a>[`--request-rate N`](#option-request-rate)\
  cap every HTTP attempt at N per second (0: no cap)

  Set it to stay under a request quota shared with other clients. Refused when the spacing exceeds half --stall-timeout.

- <a id="option-no-adaptive-limit"></a>[`--no-adaptive-limit`](#option-no-adaptive-limit)\
  do not lower concurrency when the service throttles requests

  Turn it off (with --request-rate) when a run crawls under sustained throttling with the limit near 1. Without it each of the --workers listing workers waits on one page at a time, and seed listings and structure probes run beside them.

- <a id="option-adaptive-limit"></a>[`--adaptive-limit`](#option-adaptive-limit)\
  undo a recorded --no-adaptive-limit

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

Continue an interrupted run:

```bash
swath resume logs-inventory
```

Continue it with more workers, a deadline and a fresh report path:

```bash
swath resume logs-inventory --workers 128 --max-duration 1h --report /var/log/swath/attempt-2.json
```

A resume writes a report only when it is given `--report`: the run's recorded `--report` is not replayed, so
the earlier attempt's report stays as it was.

Continue a run whose SAS token is not recorded:

```bash
swath resume container-inventory --azure-sas-token "$SAS"
```

## Exit status

The codes of [`swath list`](swath-list.md#exit-status). 2 also when the directory holds no interrupted run, or
when the environment resolves another route than the run's (another region or endpoint): the directory is
left as it was. 0 when the dataset is already complete; 3 when it is committed with partial coverage (a finalized
partial sample, or entries that could not be read). See [Exit codes](../reference/exit-codes.md).

## Environment

The provider's credential variables ([Sources](../sources/README.md)); a resume reads them again.

## Files

`DIR/.swath/journal` (the recorded command and the published parts; `swath show DIR --journal` prints it) and
`DIR/error.json` (the last attempt's outcome).

## See also

[swath list](swath-list.md), [swath show](swath-show.md), [Stop, resume and refresh](../resume-and-refresh.md#stopping-and-resuming),
[Exit codes](../reference/exit-codes.md)
