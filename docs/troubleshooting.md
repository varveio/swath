# Troubleshooting

Start from the exit code and the last line on stderr. With `--report FILE`, the report's `outcome.class` and
`outcome.cause` say the same in a form a program reads ([reference/exit-codes.md](reference/exit-codes.md)).

| Exit | Start with |
|---|---|
| 1 | [credentials](#credentials-could-not-be-loaded), [access denied](#access-denied), [the bucket is in another region](#the-bucket-is-in-another-region), [the resume is refused](#the-resume-is-refused), [a dataset in a bucket fails](#a-dataset-in-a-bucket-fails) |
| 2 | [unknown flag](#unknown-flag), [the output directory is refused](#the-output-directory-is-refused), [`-o` names a format swath does not write](#-o-names-a-format-swath-does-not-write), [the resume is refused](#the-resume-is-refused) |
| 74 | [the disk filled](#the-disk-filled-exit-74) |
| 75 | [the run stops as stuck](#the-run-stops-as-stuck-exit-75), or `unresolved` under [a dataset in a bucket fails](#a-dataset-in-a-bucket-fails) |
| 124, 130, 143 | [the run was interrupted](#the-run-was-interrupted) |
| 0, but rows are missing | [entries could not be read](#entries-could-not-be-read), and the dataset's [coverage](concepts.md#coverage) |

The first lines of the messages quoted under [Unknown flag](#unknown-flag), [The output directory is refused](#the-output-directory-is-refused)
and [`-o` names a format](#-o-names-a-format-swath-does-not-write) are checked against the binary, with their
exit code 2, by `TestTroubleshootingMessages` in `go test ./cmd/swath`; the others are not.

## Credentials could not be loaded

```text
swath: could not obtain credentials (Entra ID, DefaultAzureCredential): DefaultAzureCredential: failed to acquire a token. For public storage use --anonymous; -v shows the details
swath: could not obtain credentials (environment): AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY
```

Exit 1, cause `auth`. Without `--anonymous` swath needs working credentials before it sends a request, and
it never falls back to unsigned requests. The line names the credential source and the first line of the
cause. When swath was searching for credentials on its own (the AWS default chain with no profile, keys or
bearer token command; GCS Application Default Credentials; Azure's `DefaultAzureCredential`) and found none
that work, a public bucket or container may be the reason: the line says to add `--anonymous`. A credential
you configured (keys in the environment, a profile by `--s3-profile` or `AWS_PROFILE`, a bearer token command,
a SAS, a GCS credentials file) or an AWS configuration that does not load gets no such advice: fix it.

The whole cause, which can be long (an SDK's report of every credential it tried, a token endpoint's
answer), is the outcome's message: in the outcome line `-v` prints first (the `outcome` event's `message`
under `--log-format json -v`) and in the report's `outcome.message`. "-v shows the details" says the line
left some of it out:

```text
swath: outcome: {"class":"fatal","exit_code":1,"message":"Azure Blob client: credentials (Entra ID, DefaultAzureCredential): DefaultAzureCredential: failed to acquire a token.\nAttempted credentials:\n\tEnvironmentCredential: missing environment variable AZURE_TENANT_ID\n...",...}
swath: could not obtain credentials (Entra ID, DefaultAzureCredential): DefaultAzureCredential: failed to acquire a token. For public storage use --anonymous
```

Common causes: a key without its secret, a profile that does not exist, an expired SSO login (`aws sso login`),
a `GOOGLE_APPLICATION_CREDENTIALS` that names a missing file, no Entra ID credential on a host outside Azure.
Where each provider looks: [sources.md](sources.md). A credential lookup that failed only transiently, and
credentials the service refused ([access denied](#access-denied)), are not this failure: their errors are
printed whole.

## Access denied

Exit 1, cause `access_denied`. The credentials loaded but may not list: S3 needs `s3:ListBucket` on the bucket
(`s3:ListBucketVersions` with `--all-versions`), GCS `storage.objects.list`, Azure Blob the permission to list
the container. swath reads no object, so it needs no read permission on objects.

## The bucket or container does not exist

Exit 1, cause `not_found`, not resumable. The bucket (S3 `NoSuchBucket`, GCS `notFound`) or the Azure container
(`ContainerNotFound`) named in SOURCE does not exist; the message keeps the service's status and code. Check the
name, and for S3 the endpoint. A prefix with no object under it is not this failure: it lists nothing and
commits an empty dataset. Only Azure's `ContainerNotFound` is `not_found`: an anonymous caller (`--anonymous`)
may get a generic `ResourceNotFound` instead, which carries no cause. A missing output bucket
(`-o s3://…`) carries no cause either.

## The bucket is in another region

Exit 1, cause `region_redirect`, not resumable. S3 answered that the bucket is in another region and named it
(`X-Amz-Bucket-Region`); the error says which and the option to give, such as `give --s3-region eu-west-1`.
swath does not follow S3's redirects or discover a bucket's region; pass `--s3-region` (or set `AWS_REGION`).

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
swath: inventory: output directory is not empty (use --overwrite to replace a previous dataset)
```

Exit 2, and nothing was changed (the messages here are for `-o inventory`). `-o DIR` takes a new or empty directory. `--overwrite` replaces a previous
swath dataset there and nothing else; a directory holding other files stays refused:

```text
swath: -o: inventory: output directory does not hold a previous swath dataset
```

A directory that holds an interrupted run is continued with `swath resume DIR`, or replaced with `--overwrite`:

```text
swath: inventory: output directory is not empty: it holds an interrupted run (swath resume DIR continues it; --overwrite starts over)
```

## `-o` names a format swath does not write

```text
swath: -o x.csv: swath does not write .csv files; -o takes a dataset directory (any other name, or one ending in /), a GCS, S3 or Azure Blob prefix with --work-dir, NAME.tsv or NAME.jsonl (optionally .gz or .zst), or - for stdout
```

Write `.tsv` and convert it, or read the TSV directly ([output.md](output.md#reading-text-rows-in-duckdb)).

## The run was interrupted

A dataset run of an object store stopped by a signal (130, 143), `--max-duration` (124), `--stall-timeout`
(75) or a full disk (74) keeps its published parts and journal. `swath show DIR` prints where it stopped,
`swath resume DIR` continues it. A local tree, text output and `swath du` keep no journal: run them again.

On Windows, journaled dataset runs of S3, GCS and Azure Blob sources are refused for both local and
bucket destinations: a bucket destination still keeps its journal in a local `--work-dir`, whose
directory entries cannot be synced. `swath resume DIR` cannot continue a run or finish a bucket commit's
cleanup there; the CLI still handles an already committed, finalized dataset as having no work to
continue. Use Linux or macOS for recovery. Local filesystem scans can write datasets locally or
to a bucket without resume; text output and diagnostic `swath diag list --no-journal` runs also work
([platform limits](install.md#platforms)).

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

## A dataset in a bucket fails

| Message or exit | Meaning | What to do |
|---|---|---|
| exit 2, `-o gs://...` (or `s3://...`, `az://...`)`: a dataset in a bucket needs --work-dir DIR` | the run has nowhere to keep its lock, journal and spill | give `--work-dir DIR`, a local directory of its own |
| exit 2, `a credentials file ... is for a GCS destination`, `a profile and a region ... are for an S3 destination`, `a shared access signature ... is for an Azure destination` | a destination option of another provider | drop it, or give the one for the destination's provider |
| exit 2, `the run in DIR was started with --output-azure-sas-token, which is never recorded` | the Azure destination's SAS is not in the journal | give `--output-azure-sas-token` to `swath resume DIR` again (a rotated one is fine) |
| an Azure destination's `AuthorizationPermissionMismatch` or `AuthorizationFailure` (403) | the SAS lacks a permission, or the Entra ID identity a data role ([production.md](production.md#writing-to-a-bucket)) | grant it, then `swath resume DIR` |
| `authorized requests are not permitted for non-TLS protected (https) endpoints` | Entra ID (no `--output-azure-sas-token`) on an `http://` endpoint, such as an emulator's | give the emulator a SAS |
| `destination: remote: destination configuration: ...` | the S3 destination's AWS configuration does not load: a missing `--output-s3-profile`, an unreadable CA bundle | fix the profile or the shared config files |
| exit 2, `the destination already holds a dataset` | the prefix has a `manifest.json`; datasets in a bucket are not replaced | write to another prefix |
| exit 2, `the destination overlaps the listed source` | the listing would list the dataset's own objects, or the dataset would sit inside the listed prefix | write to another bucket or a prefix outside the source's |
| exit 75, class `unresolved` | the manifest's creation failed in a way that does not say whether it happened, and no manifest is there yet; every part is kept | `swath resume DIR` creates the same manifest again; without a journal (a local tree), check the prefix for a manifest before deleting anything under it |
| exit 1, `cause` `lost_race` | another run created the prefix's manifest first; this run deleted its own parts (`output.remote.orphans_left` counts any it could not; resuming the work directory tries again) | write to another prefix |
| exit 2, `--report` names the work directory's `commit.json` | the commit marker's name is reserved | write the report elsewhere |
| exit 1, `cause` `remote_altered` | a part the journal records was replaced, deleted or changed in the bucket | start over at a new prefix |
| exit 1, `cause` `candidate_mismatch` | the work directory's saved manifest names other parts than the resume recovered (its journal lost the final record) and may still be created | read the prefix's manifest once no request can still be pending: if there is none, start over at a new prefix |
| exit 1, `cause` `unsafe_layout` | something in the work directory's `.swath/remote/` is a link or not what swath made | inspect the work directory; start over with a new one |
| a warning that the commit's aftermath is not finished | the dataset is committed, but deleting this run's leftover parts or writing `run.json` failed | `swath resume DIR` finishes it and exits 0 |
| `output.remote.aborts_unconfirmed` above 0 (S3) | an aborted multipart upload's abort failed or could not be confirmed: its parts stay stored, unlisted and billed | nothing in the run: the bucket's lifecycle rule for incomplete multipart uploads removes them ([production.md](production.md#writing-to-a-bucket)) |

Errors never quote a resumable upload's session URL (its query is a credential for the upload), an S3
multipart upload's id, an Azure destination's SAS or its block ids: a URL in an error shows its scheme, host and
path only, an upload id is shown as `[upload-id]`, a block id as `[block-id]`, and a SAS signature an Azure
error body quotes back as `[redacted]`.

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

A local walk that cannot read some entries (permissions, entries removed during the walk, a link target it
cannot read) names up to five on stderr with their cause, counts the rest, and still exits 0. Local entry errors
can leave rows out or metadata unavailable: a symbolic link whose target cannot be read keeps its row.
`outcome.unreadable` in the report and the dataset's `coverage.unreadable` count the entries left out: not
entries found missing during stat or failed link-target reads, but failed directory opens.

## Reporting a problem

Include `swath --version`, the command line, the provider and region, and the `--report` file (redact bucket
names and keys). For a stall or a forced exit, add `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt`,
and `DIR/error.json`. `-vv` on stderr shows each failed request and retry with its cause.

The internals of these records, `error.json`'s rules, the watchdog and the forced exit, are in
[internals/run-lifecycle.md](internals/run-lifecycle.md); the workspace under `.swath/` in
[internals/durability.md](internals/durability.md).
