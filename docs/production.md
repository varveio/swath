# Run on a schedule

A short guide for whoever schedules and supervises swath: the nightly command, what to do with each exit code,
and writing the dataset to a bucket from a machine that may not last. One run's command, outcome and limits are
in [Run large listings](running.md); every option is in the [command reference](commands/swath.md). Numbers below are
observations on the stated runs ([Measurements (internals)](internals/measurements.md)), not limits or promises.

## The nightly command

```bash
swath list s3://bucket/prefix/ -o /data/inv/2026-11-08 \
  --seed-from /data/inv/2026-11-07 \
  --max-duration 6h
```

- `-o DIR` writes a sorted Parquet dataset. `DIR` must be new or empty. Write each night to a new directory and
  point readers at it once its `manifest.json` exists ([Overwrite and refresh](resume-and-refresh.md#overwrite-and-refresh)).
- `--seed-from` names the previous night's dataset ([warm start](resume-and-refresh.md#warm-start-for-repeat-listings)). Leave it out on the first night.
- The exit code and the outcome in `DIR` are the outcome of record: `DIR/run.json` beside a committed dataset
  (its `outcome`), else `DIR/error.json`, each with `class`, `exit_code` and `resumable`. An `error.json` whose
  `attempt.started_at` is before the attempt began is an earlier attempt's: one that was killed, or could not
  write it, left it ([the outcome of record](reference/exit-codes.md#the-outcome-of-record)). After exit 0 or 3
  the dataset is committed even if neither file is there: the commit removes `error.json`, and `run.json` is
  written after the commit, so a failed write leaves it out with the warning `run_report_unwritten` on stderr,
  without changing the exit code; `manifest.json` then records the coverage. To keep each
  attempt's full report, a resume's included, add `--report FILE` at a fresh path for each attempt, outside the
  output directory.
- `--max-duration` stops a run that takes too long. Give every unattended run one. Set the supervisor's own
  kill deadline above `--max-duration` plus 20 s.

What the supervisor does with the exit code:

| Exit | Do |
|---|---|
| 0 | done |
| 3 (partial) | accept the partial output, or fix the permissions and run again |
| 129, 130, 143 (signals) | `swath resume DIR` |
| 75 (stuck) | resume once; a second 75 in a row on the same output needs a person |
| 74 (disk full) | free space or move the directory to a larger disk, then `swath resume DIR` |
| 124 (deadline) | resume only within a separate budget of attempts or total time; past it, a person |
| 1 (failed) | a person fixes the cause; then `swath resume DIR` when the outcome is `resumable`, else a new run |
| 2 (usage, refused) | a person fixes the command; with no outcome or report, a crash: resume as after a kill |

The reference table, [what a supervisor does with each code](reference/exit-codes.md#what-a-supervisor-does-with-each-code),
adds the cases this short form leaves out: 75 with cause `unreachable` or class `unresolved`, and 74 with
cause `spill_budget`.

The rest of a scheduled run has its own pages:

- <a id="three-flags-on-a-small-runner"></a><a id="a-healthy-run-report"></a><a id="three-bad-patterns"></a>[Watch and tune a run](watch-a-run.md):
  [three flags on a small runner](watch-a-run.md#three-flags-on-a-small-runner),
  [a healthy run report](watch-a-run.md#a-healthy-run-report) and
  [three bad patterns](watch-a-run.md#three-bad-patterns).
- <a id="warm-start-for-repeat-listings"></a><a id="resume-after-a-kill"></a><a id="where-things-are"></a>[Stop, resume and refresh](resume-and-refresh.md):
  [resume after a kill](resume-and-refresh.md#stopping-and-resuming), where the workspace and spill are, and
  the [warm start for repeat listings](resume-and-refresh.md#warm-start-for-repeat-listings).
- <a id="disk-sizing"></a>[Size a run](size-a-run.md): [disk sizing](size-a-run.md#disk-sizing) and memory.

## Writing to a bucket

`swath list SOURCE -o gs://BUCKET/PREFIX --work-dir DIR` (or `-o s3://BUCKET/PREFIX`, or
`-o az://ACCOUNT/CONTAINER/PREFIX`) puts the dataset in GCS (or S3, or Azure Blob Storage) and keeps nothing
large on the runner: the parts go up as they close, and `DIR` holds only spill and the
run's metadata.

- **The destination's own options:** credentials (GCS: `--output-gcs-credentials-file`, else Application
  Default Credentials; S3: `--output-s3-profile`, else the AWS SDK's chain, always signed; Azure:
  `--output-azure-sas-token`, never recorded, else Entra ID), region (`--output-s3-region`) and endpoint
  (`--output-endpoint-url`: path-style for S3; for Azure the account is its first path segment).
- **Per store:** credentials, permissions, region and endpoint, and what each store needs for uploads that
  did not finish, are on the store's page: [Amazon S3](sources/s3.md#writing-a-dataset-to-s3) (a lifecycle rule
  that aborts incomplete multipart uploads is required), [Google Cloud Storage](sources/gcs.md#writing-a-dataset-to-gcs),
  [Azure Blob Storage](sources/azure.md#writing-a-dataset-to-azure-blob-storage).
- **One prefix and one work directory per run:** a prefix that holds a `manifest.json` is refused; datasets in
  a bucket are not replaced. Write each run to a dated prefix and let readers find the newest by its manifest.
  A work directory belongs to its run (`--overwrite` is refused with a bucket): a new run takes a new or empty
  one.
- **Supervision:** as for a local dataset, `swath resume DIR` until exit 0. Give every attempt the same work
  directory; if it is lost the resume is lost with it ([below](#on-a-preemptible-or-spot-machine)). Two
  runners sharing one work directory are refused by its lock; two work directories writing one prefix race
  at the commit, and exactly one wins (the other exits with a lost race and deletes its parts).
- **Lifecycle rules:** never let a lifecycle rule or a cleanup job delete or replace objects under a committed
  dataset's prefix: the manifest would name parts that are gone, and swath does not notice. Expire objects
  only under the prefixes of runs you have abandoned; a rule that aborts incomplete uploads is safe, and on S3
  required ([Amazon S3](sources/s3.md#writing-a-dataset-to-s3)).
- **Debug logging:** swath turns the storage SDKs' own logging off, so no SDK setting logs a token, a signed
  assertion or a session URL; with `GOOGLE_SDK_GO_LOGGING_LEVEL=debug`, a GCS destination may still log request
  lines: the method, the object name and a URL without credentials.
- **Leftovers:** a run that is abandoned (never resumed to its commit) leaves its uploaded parts under
  `PREFIX/data/RUN/` with no manifest, named in `DIR/.swath/remote/allocated`; delete them, or set a lifecycle
  rule on the bucket. A run whose work directory was lost is abandoned the same way, without that list. A new
  run at the same prefix does not clean them: it is not refused (only a `manifest.json` is), its manifest
  names its own parts only, and the parts the abandoned run uploaded stay under its own `PREFIX/data/RUN/`
  beside them, so start it at a new prefix. Unfinished resumable upload sessions expire on their own (GCS keeps them a week), as do
  Azure's uncommitted blocks (a week); S3's incomplete multipart uploads do not ([the rule](sources/s3.md#writing-a-dataset-to-s3)).
- **An unresolved commit** (`commit: unresolved`: the manifest's creation failed in a way that does not say
  whether it happened, and no manifest is there yet) keeps every part: resume the work directory, which
  creates the very same manifest again. A run without a journal (a local tree) cannot resume: check the prefix
  for a manifest before deleting anything under it.
- **Report:** `output.remote` in the report ([run report](reference/run-report.md)): the commit's outcome, the
  uploads, retries by class, `upload_wait_ns` (the encoders' time waiting on uploads), the orphans deleted and,
  for S3, `aborts_unconfirmed` (multipart uploads the lifecycle rule must remove).
  Uploads happen inside the run, so the bucket's bandwidth is part of its wall time.

### On a preemptible or Spot machine

A bucket run resumes from its work directory only. `DIR/.swath/` holds the journal (the recorded command and
one record per published part: its object, version, size and SHA-256), the durable spill and its ledger
(`spill/durable/`), and the claim receipts, the allocation log and the candidate manifest (`remote/`). The
parts are not kept there: they stream to the bucket, which holds nothing else of the run until its commit. A
lost work directory cannot be resumed, and the parts the run published are not reused
([leftovers](#writing-to-a-bucket), above).

To survive a preempted VM, put `--work-dir` on a disk that outlives the VM (one not deleted with the
instance), attach it to the replacement machine and run `swath resume DIR` at its mount point there. The
resume replays the recorded command with `DIR` as the work directory and the recorded bucket and prefix.
Nothing binds the work directory to a machine or a path: the lock is a `flock`, and the run's identity
(source, schema, part size, filters, destination) names neither the machine nor a local path. On the new
machine:

- **Paths the command recorded** are replayed as written, a relative one against the new working directory.
  `--gcs-credentials-file`, `--output-gcs-credentials-file`, the directory of `--report` and a `--ca-bundle`
  (unless the command also has `--no-verify-ssl`, which skips it) must exist there, or be given again to
  `swath resume` (each may change on resume); a `--seed-from` that is missing only warns, and the resume lists
  without it.
- **Secrets** are never recorded (`--azure-sas-token`, `--output-azure-sas-token`,
  `--s3-bearer-token-command`): give them again, or the resume is refused. The source's `--azure-sas-token`
  may come from `AZURE_STORAGE_SAS_TOKEN` instead; `--output-azure-sas-token` has no such variable.
- **The spill cap** is a positive `--spill-budget` the run recorded or the resume gives; without one (or with
  0) it is the default, the free space on the new disk after the reserve when the resume starts, the certified
  spill files the resume keeps counted as free (on the same disk the cap is then the stopped run's, less what
  the work directory took meanwhile), and it follows the disk from there. Certified spill files that do not
  fit under the cap the resume starts with are listed again (`rejected.budget` in
  `diagnostics.resources.spill.durable.resume`).
- **Verification:** before listing continues, the resume reads every part the journal records back from the
  bucket, one after another, whole, and checks its version, size and SHA-256 (`output.remote.objects_verified`):
  everything published so far is downloaded once, at the bucket's read rate and price.
