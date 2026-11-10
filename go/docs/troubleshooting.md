# Troubleshooting

Start from the exit code and the last line on stderr. With `--report FILE`, the report's `outcome.class` and
`outcome.cause` say the same in a form a program reads ([reference/exit-codes.md](reference/exit-codes.md)).

## Credentials could not be loaded

```text
swath: S3 client: credentials (environment): AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY (a public bucket needs --anonymous)
```

Exit 1, cause `auth`. Without `--anonymous` swath needs working credentials before it sends a request, and
it never falls back to unsigned requests. For a public bucket or container, add `--anonymous`. Otherwise fix
the credential source the message names: a key without its secret, a profile that does not exist, an expired
SSO login (`aws sso login`), a `GOOGLE_APPLICATION_CREDENTIALS` that names a missing file. Where each provider
looks: [sources.md](sources.md).

## Access denied

Exit 1, cause `access_denied`. The credentials loaded but may not list: S3 needs `s3:ListBucket` on the bucket
(`s3:ListBucketVersions` with `--all-versions`), GCS `storage.objects.list`, Azure Blob the permission to list
the container. swath reads no object, so it needs no read permission on objects.

## The bucket is in another region

The error names the bucket's region and the option to give, such as `give --s3-region eu-west-1`. swath does not
follow S3's redirects or discover a bucket's region; pass `--s3-region` (or set `AWS_REGION`).

## Unknown flag

```text
swath: unknown flag --region (did you mean --s3-region?)
See 'swath list --help'.
```

Exit 2. An option only one provider has carries its prefix (`--s3-region`, `--gcs-credentials-file`,
`--azure-sas-token`), and a long option takes two dashes. An option given for another provider's source is
refused rather than ignored:

```text
swath: --s3-profile applies to S3 sources, not to GCS sources
```

## The output directory is refused

```text
swath: nonempty: output directory is not empty (use --overwrite to replace a previous dataset)
```

Exit 2, and nothing was changed. `-o DIR` takes a new or empty directory. `--overwrite` replaces a previous
swath dataset there and nothing else; a directory holding other files stays refused:

```text
swath: -o: nonempty: output directory does not hold a previous swath dataset
```

A directory that holds an interrupted run is continued with `swath resume DIR`, or replaced with `--overwrite`:

```text
swath: l2: output directory is not empty: it holds an interrupted run (swath resume DIR continues it; --overwrite starts over)
```

## A `.parquet` output became a directory

`-o NAME.parquet` writes a dataset: a directory of parts and a manifest, whatever its name. Only a name
ending in `.tsv` or `.jsonl` (optionally `.gz` or `.zst`) is a single file. Read the dataset through its
manifest ([getting-started.md](getting-started.md#6-query-it-with-duckdb)).

## `-o` names a format swath does not write

```text
swath: -o x.csv: swath does not write .csv files; -o takes a dataset directory (any other name, or one ending in /), NAME.tsv or NAME.jsonl (optionally .gz or .zst), or - for stdout
```

Write `.tsv` and convert it, or read the TSV directly ([output.md](output.md#reading-text-rows-in-duckdb)).

## The run was interrupted

A dataset run of an object store stopped by a signal (130, 143), `--max-duration` (124), `--stall-timeout`
(75) or a full disk (74) keeps its published parts and journal. `swath show DIR` prints where it stopped,
`swath resume DIR` continues it. A local tree, text output and `swath du` keep no journal: run them again.

On Windows a journaled run and `swath resume` are refused ([running.md](running.md#known-limits)).

## The resume is refused

| Message or exit | Meaning | What to do |
|---|---|---|
| exit 2, `--NAME is fixed for the run` | the option cannot change on resume | give only the options `swath resume --help` lists |
| exit 2, identity mismatch | the environment or `--s3-profile` resolves another region or endpoint than the run's | restore the original environment, or start over with `swath list --overwrite` |
| exit 2, `nothing to resume` | no interrupted run in the directory | start a fresh `swath list` |
| exit 1, another swath process owns this output directory | a process still holds the output lock: the stopped run has not exited yet, or a wrapper was killed and swath under it lives on | wait for that process to exit (stop a wrapper's whole process group), then resume |
| exit 1, `cause` `protocol` | the service returned an invalid page; the run cannot be resumed | `swath list --overwrite` |
| exit 1, journal version | the run was written by another build | finish with that build, or `--overwrite` |
| exit 1, part altered or journal corrupt | a retained part or the journal changed on disk | `--overwrite` |
| exit 1, ambiguous tail | the part set aside in `.swath/suspect/` cannot be reproduced: the source changed in that part's range | `--overwrite`, or remove `.swath/suspect/` to accept the source as it is now |

A run started with `--azure-sas-token` or `--s3-bearer-token-command` must be given it again: secrets are
never recorded.

## The run stops as stuck (exit 75)

Nothing progressed for `--stall-timeout` (10 minutes). The `cause` says what kept rising: `stuck_throttle`
(the service throttled), `stuck_timeouts` (requests ran past `--request-timeout`), `stuck_empty_chain` (the
service answered only empty pages), `stuck_no_rows` (requests failed or hung). swath wrote every goroutine's
stack to `DIR/.swath/stacks-stuck.txt`. Resume once; if it happens again on the same output, keep that file and
the report: a person needs to look. A service that is slow for minutes at a time needs a larger
`--stall-timeout`.

## The run crawls under throttling

`throttles` rises in `progress.json` and the request limit falls below `--workers`. For a quota shared with
other clients, set `--request-rate N` below it. If the limit stays near 1 under sustained throttling, use
`--no-adaptive-limit --request-rate N` ([running.md](running.md#throttling)).

## A warning repeats about failing requests

```text
swath: warning: S3 127.0.0.1:9: connection refused (retrying; 14 attempts)
```

Every request has failed with one cause for 10 seconds. swath keeps retrying until `--stall-timeout` or
`--max-duration`; check the endpoint, the network and `--endpoint-url`. A run that ends without any request
answered names the cause in its final error:

```text
swath: deadline: --max-duration reached; every request was failing: S3 127.0.0.1:1: connection refused (30 attempts)
```

## The disk filled (exit 74)

A part, the journal or the manifest could not be written. Free space, or move the output directory to a
larger disk, and `swath resume DIR`. A sorted run of a large bucket spills almost every row once on the
output's filesystem: size the disk as [production.md](production.md#disk-sizing) says, or cap spill with
`--spill-budget` (listing then waits instead of spilling).

## Memory is higher than `--sort-buffer`

`--sort-buffer` bounds the rows held between listing and encoding, not the process's memory: pages in flight,
directories being read, the encoders, the tail after listing and the garbage collector come on top, and swath
sets no total-memory limit. Read `resources.peak_rss_bytes` in the report of a representative run. To use less,
lower the budget together with `--workers` and `--encoders`: a lower budget alone can spill more without
lowering the peak ([running.md](running.md#memory)).

## `swath du` shows a size of 0, or `-`

With `--metadata names` a local walk reads no sizes: `unsized` counts the files without one, and the table
shows `-`. With `--allocated` the bytes are in `allocated_size`, not `size`
([output.md](output.md#du-rows)).

## Entries could not be read

A local walk that cannot read some entries (permissions, entries removed during the walk) names up to five on
stderr with their cause, counts the rest, leaves them out of the rows and totals, and still exits 0. The
report's `outcome.unreadable` is then true, and a dataset's coverage counts them in `skipped_entries`.

## Reporting a problem

Include `swath --version`, the command line, the provider and region, and the `--report` file (redact bucket
names and keys). For a stall or a forced exit, add `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt`,
and `DIR/error.json`. `-vv` on stderr shows each failed request and retry with its cause.

The internals of these records, `error.json`'s rules, the watchdog and the forced exit, are in
[internals/run-lifecycle.md](internals/run-lifecycle.md); the workspace under `.swath/` in
[internals/durability.md](internals/durability.md).
