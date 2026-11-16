# Troubleshooting

Start from the exit code and the last line on stderr. With `--report FILE`, the report's `outcome.class` and
`outcome.cause` say the same in a form a program reads ([Exit codes](reference/exit-codes.md)).

| Exit | Start with |
|---|---|
| 1 | [credentials](#credentials-could-not-be-loaded), [access denied](#access-denied), [the bucket is in another region](#the-bucket-is-in-another-region), [the resume is refused](#the-resume-is-refused), [a dataset in a bucket fails](#a-dataset-in-a-bucket-fails) |
| 2 | [unknown flag](#unknown-flag), [the output directory is refused](#the-output-directory-is-refused), [`-o` names a format swath does not write](#-o-names-a-format-swath-does-not-write), [the resume is refused](#the-resume-is-refused) |
| 3 | [entries could not be read](#entries-could-not-be-read), or `--max-keys` stopped a diagnostic listing; the dataset's [coverage](results.md#coverage-and-unreadable-entries) says which |
| 74 | [the disk filled](#the-disk-filled-exit-74) |
| 75 | [the run stops as stuck](#the-run-stops-as-stuck-exit-75), or `unresolved` under [a dataset in a bucket fails](#a-dataset-in-a-bucket-fails) |
| 124, 130, 143 | [the run was interrupted](#the-run-was-interrupted) |

The first lines of the messages quoted under [Unknown flag](#unknown-flag), [The output directory is refused](#the-output-directory-is-refused)
and [`-o` names a format](#-o-names-a-format-swath-does-not-write) are checked against the binary, with their
exit code 2, by `TestDocsTroubleshootingMessages` in `go test ./cmd/swath`; the others are not.

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
Where each provider looks: [Sources](sources/README.md). A credential lookup that failed only transiently, and
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
`--azure-sas-token`), and a long option takes two dashes. `swath du` takes GNU `du`'s `-x` and `--max-depth N`
(the same as `--depth N`), and a GNU `du` option it takes another way says so:

```text
swath: unknown flag --summarize (the total alone is the default: --depth 0)
```

A 0.3 option names what replaced it (`--force-path-style` is `--s3-path-style`, `--idle-timeout` is
`--stall-timeout`, `--compression` is the `-o` name).

An option given for another provider's source is refused rather than ignored:

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

An output inside the local tree being listed (`swath list tree -o tree/inventory`) is refused when it is not empty, with or without `--overwrite`,
which never replaces anything inside the listed tree. Write the dataset to a new directory outside it:

```text
swath: -o: tree/inventory: output directory is not empty, and cannot replace an output inside the listed root (write the dataset to a new directory outside the listed root)
```

When that directory holds an interrupted run (of another source), `swath resume DIR` still continues it; it is
never replaced there:

```text
swath: -o: tree/inventory: output directory is not empty, and cannot replace an output inside the listed root: it holds an interrupted run (swath resume DIR continues it; a new dataset goes to a new directory outside the listed root)
```

A directory that holds an interrupted run is continued with `swath resume DIR`, or replaced with `--overwrite`:

```text
swath: inventory: output directory is not empty: it holds an interrupted run (swath resume DIR continues it; --overwrite starts over)
```

A dataset run of an S3, GCS or Azure Blob source keeps a journal, and it and `swath resume` need an output
directory (for a bucket destination, the `--work-dir`) on a filesystem that syncs directories. On Windows
swath checks this when the run starts, and where the filesystem refuses, the run is refused before it writes
anything, the filesystem's error in parentheses:

```text
swath: journaled output needs directory syncs, which the output's filesystem refused, so a power loss could lose acknowledged parts; write the output on another filesystem (...)
```

Put the output on a local NTFS volume. Every dataset run syncs its output's directories, so no dataset run,
journaled or not, writes to such a filesystem; text output does.

On Windows a file another program holds open cannot be replaced, and a directory holding one cannot be
renamed. A run that must replace one (`--overwrite` retiring a dataset whose part a reader has open, a
`--report` file open in an editor) waits up to two seconds, then fails naming the file, with
`another program may hold it, or a file in it, open: close that program and run again`. A refused
`--overwrite` puts back what it had moved and leaves the previous dataset as it was, readable, with the
replacement committed beside it (`swath show DIR` says so); once the reader is closed, `swath resume DIR`
installs the replacement. If something it moved cannot come back either (another program holds a file in
it), the error says `the rollback is incomplete`: the directory holds no dataset until `swath resume DIR`,
run once those programs are closed, finishes the replacement.

## `-o` names a format swath does not write

```text
swath: -o x.csv: swath does not write .csv files; -o takes a dataset directory (any other name, or one ending in /), a GCS, S3 or Azure Blob prefix with --work-dir, NAME.tsv or NAME.jsonl (optionally .gz or .zst), or - for stdout
```

Write `.tsv` and convert it, or read the TSV directly ([Output](output.md#reading-text-rows-in-duckdb)).

## The run was interrupted

A dataset run of an object store stopped by a signal (130, 143), `--max-duration` (124), `--stall-timeout`
(75), a full disk or `--spill-wait-timeout` (74) keeps its published parts and journal. `swath show DIR` prints where it stopped,
`swath resume DIR` continues it. A local tree, text output and `swath du` keep no journal: run them again.
A text file (`-o FILE.tsv`) is written only when the listing finishes; a stopped run discards it, and its
closing lines say `no file written`. After `--max-duration` the last line says which applies:

```text
swath: deadline: --max-duration reached (swath resume inv --max-duration 0 continues it without a deadline; a plain resume keeps the recorded one)
swath: deadline: --max-duration reached (it cannot be resumed: run it again with a longer --max-duration, or without one)
```

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
| exit 2, `-o gs://...` (or `s3://...`, `az://...`): `a dataset in a bucket needs --work-dir DIR` | the run has nowhere to keep its lock, journal and spill | give `--work-dir DIR`, a local directory of its own |
| exit 2, `a credentials file ... is for a GCS destination`, `a profile and a region ... are for an S3 destination`, `a shared access signature ... is for an Azure destination` | a destination option of another provider | drop it, or give the one for the destination's provider |
| exit 2, `the run in DIR was started with --output-azure-sas-token, which is never recorded` | the Azure destination's SAS is not in the journal | give `--output-azure-sas-token` to `swath resume DIR` again (a rotated one is fine) |
| an Azure destination's `AuthorizationPermissionMismatch` or `AuthorizationFailure` (403) | the SAS lacks a permission, or the Entra ID identity a data role ([Running swath in production](production.md#writing-to-a-bucket)) | grant it, then `swath resume DIR` |
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
| `output.remote.aborts_unconfirmed` above 0 (S3) | an aborted multipart upload's abort failed or could not be confirmed: its parts stay stored, unlisted and billed | nothing in the run: the bucket's lifecycle rule for incomplete multipart uploads removes them ([Running swath in production](production.md#writing-to-a-bucket)) |

Errors never quote a resumable upload's session URL (its query is a credential for the upload), an S3
multipart upload's id, an Azure destination's SAS or its block ids: a URL in an error shows its scheme, host and
path only, an upload id is shown as `[upload-id]`, a block id as `[block-id]`, and a SAS signature an Azure
error body quotes back as `[redacted]`.

## The run stops as stuck (exit 75)

Nothing progressed for `--stall-timeout` (10 minutes). The `cause` says what kept rising: `stuck_throttle`
(the service throttled), `stuck_timeouts` (requests ran past `--request-timeout`), `stuck_empty_chain` (the
service answered only empty pages), `stuck_no_rows` (requests failed or hung). swath wrote every goroutine's
stack to `DIR/.swath/stacks-stuck.txt` (without an output directory, to the file in the temporary directory
that its warning names; `-v` prints the stacks on stderr too). Resume once; if it happens again on the same output, keep that file and
the report: a person needs to look. A service that is slow for minutes at a time needs a larger
`--stall-timeout`. An Azure account busy before the listing starts (503 `ServerBusy` on its namespace check)
stops the same way: run the command again once the service answers.

## The run crawls under throttling

`throttles` rises in `progress.json` and the request limit falls below `--workers`. For a quota shared with
other clients, set `--request-rate N` below it. If the limit stays near 1 under sustained throttling, use
`--no-adaptive-limit --request-rate N` ([Throttling](watch-a-run.md#throttling)).

## A warning repeats about failing requests

```text
swath: warning: S3 127.0.0.1:9: 503 Service Unavailable (retrying; 14 attempts; the run stops as stuck after 10m0s without progress)
```

Every request has failed with one cause for 10 seconds. swath keeps retrying until `--stall-timeout` or
`--max-duration`, as the warning says; check the endpoint, the network and `--endpoint-url`. A connection that
nothing has ever answered at is the next section's. A run that ends without any request
answered names the cause in its final error:

```text
swath: deadline: --max-duration reached; every request was failing: S3 127.0.0.1:1: connection refused (30 attempts)
```

## The endpoint cannot be reached

```text
swath: warning: S3 127.0.0.1:9000: connection refused (retrying; 25 attempts; nothing has answered: the run stops as unreachable 30s after the first attempt)
swath: list s3://bucket/: endpoint http://127.0.0.1:9000 cannot be reached (connection refused): nothing has answered in 30.4s; check the endpoint address, and that the service is running
```

Exit 75, cause `unreachable`: the run is stuck, found early. Nothing in the run has been answered, and for 30
seconds (at most half `--stall-timeout`) every connection was refused, its host was not found, or no route led
to it: a mistyped `--endpoint-url` (or `AWS_ENDPOINT_URL`, `STORAGE_EMULATOR_HOST`), or an emulator (MinIO,
Azurite, the GCS emulator) that is not running yet. Nothing was listed, so a dataset run removes its journal
and work files: run the corrected command again on the same `-o` (the outcome is in `--report`, not in
`error.json`). A resume whose run had published parts keeps its journal: once the endpoint answers,
`swath resume DIR` continues it. Once anything has answered,
connections that fail later are an outage instead: swath retries them until `--stall-timeout`.

## The disk filled (exit 74)

A part, the journal or the manifest could not be written. Free space, or move the output directory to a
larger disk, and `swath resume DIR`. A sorted run of a large bucket spills almost every row once on the
output's filesystem, unless it is seeded from the previous dataset (`--seed-from`): size the disk as [disk sizing](size-a-run.md#disk-sizing) says, or cap spill with
`--spill-budget` (listing then waits instead of spilling).

A run that is far short of spill room keeps going at the output's pace, which on a very large bucket can be a
few rows a second. After 2 minutes of it swath prints, and repeats every 30 minutes while it lasts:

```text
swath: spill cap too small: listing has waited on the output for 2m0s: the spill cap cannot hold the rows listed ahead of it (spill 91.0 GiB, cap 91.0 GiB, free-space reserve 1.00 GiB); the output writes 9,800 rows/s; the cap follows the spill disk: grow the disk, or free space on it, and listing continues, or let it run: it finishes, slowly
```

With a `--spill-budget` the remedy reads `stop the run and resume it with more disk or a larger --spill-budget (swath resume inventory --spill-budget SIZE)`.

`progress.json` has `order.spill_pressure_since` meanwhile, and the report `diagnostics.resources.spill.refused` above 0
([the first bad pattern](watch-a-run.md#three-bad-patterns)). With the default cap, grow the spill filesystem
under the running process: the cap follows the disk, and listing continues once the next frame finds the room
(`diagnostics.resources.spill.cap_raises`). With a `--spill-budget`, stop it and resume with a larger one on a disk that has
the room (a plain resume keeps the recorded cap; with the default cap it derives one from the free space it
finds), or let it finish. With `--spill-wait-timeout DURATION` the run stops by itself once
the wait has lasted that long: exit 74, `cause` `spill_budget`, the error ending
`stopped by --spill-wait-timeout 30m0s: resume it with more disk or a larger --spill-budget (swath resume inventory --spill-budget SIZE)`.

## Memory is higher than `--sort-buffer`

`--sort-buffer` bounds the rows held between listing and encoding, not the process's memory: pages in flight,
directories being read, the encoders, the tail after listing and the garbage collector come on top, and swath
sets no total-memory limit. Read `process.peak_rss_bytes` in the report of a representative run. To use less,
lower the budget together with `--workers` and `--encoders`: a lower budget alone can spill more without
lowering the peak ([Memory](size-a-run.md#memory)).

## `swath du` shows a size of `-`

With `--metadata names` a local walk reads no sizes or blocks: `size` and `allocated_bytes` are null, the
table shows `-`, and `size_missing` and `allocated_bytes_missing` count the files without them. Windows reads
no blocks, and a bucket has none: `allocated_bytes` is null there, and the line under a local table says
`on disk unknown`. A figure shown as `>= 12.0 KiB` leaves out
the files that had no value for it ([Output](output.md#du-rows)).

## Entries could not be read

```text
$ swath list /data -o inventory
swath: warning: unreadable entries:
  locked: permission denied
  partial: 1 unreadable entry; …
```

A local walk that cannot read some entries (permissions, a directory replaced while the walk ran) leaves them
out, names up to five on stderr with their cause (a path below SOURCE relative to it), and exits 3 (class
`incomplete`); the closing line's verdict counts them. Under `-q`, which prints no closing line, the last line
says it instead: `swath: 1 entry could not be read and is missing from the output`. `swath du`'s view marks
the lines whose sums leave them out with `!`, and says so in one line under it (`-v` names them). The rows, totals or dataset hold everything else. A symbolic link whose target cannot be read keeps
its row, an entry removed during the walk is no longer part of the tree, and one the include and exclude rules
leave out by its name is not selected: none of them counts. A root directory that
cannot be opened stops the run instead (exit 1). `outcome.unreadable` in the report and the dataset's
`coverage.unreadable` hold the count; `source.stats` in the report breaks the errors down.

## Known limits

The limits of this build, and what has not been measured:

- **Windows:** journaled runs and resume work as on Linux and macOS, on an output whose filesystem
  syncs directories (swath checks when the run starts:
  [the output directory is refused](#the-output-directory-is-refused)). The test suite, its crash and kill
  matrices included, runs there in CI; nothing is measured there.
- **Windows power loss:** a power loss keeps the latest part names on local NTFS only; on another filesystem
  (ReFS, an SMB share) a crash or kill is still recovered, but a power loss may lose the latest part names.
- **macOS:** the test suite runs there in CI; nothing is measured there. APFS refuses names that are not
  UTF-8, so a filesystem run never meets one.
- **Power loss or a kernel crash:** recovery relies on the filesystem honoring `fsync` on files and directories
  (ext4, XFS, APFS do). Process crashes are tested at every crash point; power loss is not.
- **Live scans, not snapshots:** objects added, removed or changed during the run change what it observes. A
  committed dataset is the complete result of a live listing, exactly once per key (with `--all-versions`, once
  per version), not a point-in-time view.
- **Versioned listing** (`--all-versions`) was checked against an independent serial listing on small public
  buckets (seven S3, one GCS; up to 40,000 versions, pages of 1 to 1,000 entries, interrupted and resumed runs);
  large versioned buckets are unmeasured.
- **Memory:** `--sort-buffer` bounds rows in flight, not resident memory ([Size a run](size-a-run.md#memory)).
- **Scale:** key lists of up to 593M keys were measured through a key-list fake at 50 ms, and one live bucket
  of 1.09 billion objects at 256 and 1,024 workers. Production listings of public buckets of billions of
  objects (35 completed, 50.6 billion objects in all) are individual runs, not a benchmark
  ([measurements](internals/measurements.md#spill-at-billion-scale-and-a-spill-cap-too-small-2026-11-13));
  other gaps are in [measurements](internals/measurements.md#known-gaps).

## Reporting a problem

Report a bug or a confusing message as a [GitHub issue](https://github.com/varveio/swath/issues/new). Attach
the output of `swath --version` and the run report: rerun with `--report run.json`, or take `DIR/run.json`
beside a dataset, or `DIR/error.json` when the run did not complete.

Include `swath --version`, the command line, the provider and region, and the `--report` file (redact bucket
names and keys). For a stall or a forced exit, add `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt`
(or the temporary file the warning named), and `DIR/error.json`. `-vv` on stderr shows each failed request and retry with its cause.

The internals of these records, `error.json`'s rules, the watchdog and the forced exit, are in
[Run lifecycle (internals)](internals/run-lifecycle.md); the workspace under `.swath/` in
[Durability and resume (internals)](internals/durability.md).
