# Run on a schedule

A short guide for whoever schedules and supervises swath: the nightly command, what to do with each exit code,
and writing the dataset to a bucket from a machine that may not last. Listing a bucket once, start to finish, is
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

To keep the dataset in a bucket instead of on the runner, give `-o` a bucket prefix and `--work-dir` a local
directory:

```bash
swath list s3://bucket/prefix/ -o gs://inventory/2026-11-08 --work-dir /data/work/2026-11-08 \
  --max-duration 6h
```

`-o s3://BUCKET/PREFIX` writes to S3 and `-o az://ACCOUNT/CONTAINER/PREFIX` to Azure Blob Storage the same way.
The parts are uploaded as they close and nothing large stays on the runner. The closing line names the prefix,
and `swath show DIR`, given the work directory, prints the dataset's summary.

### What it needs

- **A work directory** (`--work-dir DIR`), local, new or empty, one per run. It holds the run's lock, journal
  and spill, never a part, so it needs room for spill and the run's metadata, not for the dataset
  ([disk sizing](size-a-run.md#disk-sizing)). Without `--work-dir` the command is refused (exit 2). Every
  resume of the run uses the same directory; if the machine may not last, put it on a disk that outlives it
  ([below](#on-a-preemptible-or-spot-machine)).
- **Credentials and permissions on the destination**, its own, never the source's (a source's `--anonymous`
  does not reach it):

  | Store | Credentials (default) | Permissions | Unfinished uploads |
  |---|---|---|---|
  | [Amazon S3](sources/s3.md#writing-a-dataset-to-s3) | `--output-s3-profile NAME` (the AWS SDK's default chain) | list the bucket; put, get and delete objects; abort multipart uploads | kept until a lifecycle rule aborts them: the rule is required |
  | [Google Cloud Storage](sources/gcs.md#writing-a-dataset-to-gcs) | `--output-gcs-credentials-file FILE` (Application Default Credentials) | create, get and delete objects | expire on their own |
  | [Azure Blob Storage](sources/azure.md#writing-a-dataset-to-azure-blob-storage) | `--output-azure-sas-token TOKEN`, never recorded (Entra ID) | read, write and delete blobs | expire on their own |

  The exact permission and role names, and each store's region and endpoint rules, are on the store's page.
  `--output-s3-region` and `--output-endpoint-url` (an emulator, or an S3-compatible store) are fixed for the
  run; the credentials are resolved again on every resume.
- **Logging:** swath turns the storage SDKs' own logging off, so no SDK setting logs a token, a signed
  assertion or a session URL; with `GOOGLE_SDK_GO_LOGGING_LEVEL=debug`, a GCS destination may still log
  request lines: the method, the object name and a URL without credentials.

### What appears in the bucket, and when

- **During the run**, parts appear under `PREFIX/data/RUN/ATTEMPT/` as they close. There is no
  `PREFIX/manifest.json` yet, so the prefix holds no dataset: a reader that finds no manifest must not read
  the parts.
- **At the end**, the run creates `PREFIX/manifest.json`, only if it is absent: that creation is the commit,
  and the dataset exists from then on. `run.json` and, for an object-store source, `cuts.txt` follow beside
  it. The uploads happen inside the run, so the bucket's bandwidth is part of its wall time.
- **Readers** take the parts from the manifest, never from a listing of `data/`
  ([a dataset in a bucket](dataset.md#a-dataset-in-a-bucket)).
- **One prefix per run.** A prefix that already holds a `manifest.json` is refused before the run starts
  (exit 2), as are `--overwrite` and a prefix that overlaps the listed source: a dataset in a bucket is never
  replaced. Write each run to a dated prefix and let readers find the newest by its manifest.
- **Two runs, one prefix.** Two runners sharing one work directory are refused by its lock. Two work
  directories writing one prefix race at the commit and exactly one wins; the other exits 1 (`lost_race`) and
  deletes its parts.

### Interrupted, then resumed

As for a local dataset, `swath resume DIR` (the work directory) continues the run until exit 0; the exit codes
mean what [the table above](#the-nightly-command) says. Before it lists, the resume downloads every part the
run uploaded and checks its version, size and SHA-256, so everything published so far is read once, at the
bucket's read rate and price.

- **An unresolved commit** (exit 75, class `unresolved`: the manifest's creation failed in a way that does not
  say whether it happened) keeps every part: resume the work directory, which creates the very same manifest
  again. A local tree written to a bucket keeps no journal and cannot resume: check the prefix for a manifest
  before deleting anything under it.
- **An Azure destination's SAS** is never recorded: give `--output-azure-sas-token` to every resume (no
  variable stands in for it), or the resume is refused. The source's secrets are given again too
  ([resume it](resume-and-refresh.md#resume-it)).
- **A lost work directory** cannot be resumed, and the parts the run uploaded are not reused: start again at a
  new prefix, and clean up the old one ([what is left behind](#what-is-left-behind)).

### On a preemptible or Spot machine

To survive a preempted VM, put `--work-dir` on a disk that outlives the VM (one not deleted with the
instance), attach it to the replacement machine and run `swath resume DIR` at its mount point there. The
resume replays the recorded command with `DIR` as the work directory and the recorded bucket and prefix.
Nothing binds the work directory to a machine or a path: the lock is a `flock`, and the run's identity
(source, schema, part size, filters, destination) names neither the machine nor a local path. On the new
machine:

- **Paths the command recorded** are replayed as written, a relative one against the new working directory.
  `--gcs-credentials-file`, `--output-gcs-credentials-file` and a `--ca-bundle` (unless the command also has
  `--no-verify-ssl`, which skips it) must exist there, or be given again to `swath resume`; a `--seed-from`
  that is missing only warns, and the resume lists without it.
- **The spill cap** is the run's `--spill-budget`, or one given to the resume; without either it is the new
  disk's free space after the reserve, the spill files the resume keeps counted as free. Spill files that do
  not fit under it are listed again ([how a resume reuses them](internals/ordering.md#durable-spill)).

### What is left behind

- **A committed run cleans up after itself:** after the commit it deletes the parts its earlier attempts
  uploaded that the manifest does not name. A part whose upload it could not confirm is left, and counted in
  the report's `output.remote.orphans_unproven`.
- **An abandoned run** (never resumed to its commit) leaves its uploaded parts under `PREFIX/data/RUN/` with
  no manifest; `DIR/.swath/remote/allocated` names them. Delete them, or set a lifecycle rule on that prefix. A
  run whose work directory was lost is abandoned the same way, without that list. A new run at the same prefix
  does not clean them: it is not refused (only a `manifest.json` is), and its manifest names only its own
  parts, so start it at a new prefix.
- **Lifecycle rules:** never let a lifecycle rule or a cleanup job delete or replace objects under a committed
  dataset's prefix: the manifest would name parts that are gone, and swath does not notice. Expire objects
  only under the prefixes of runs you have abandoned. A rule that aborts incomplete uploads is safe, and on S3
  required ([the rule](sources/s3.md#writing-a-dataset-to-s3)).

### Limits

- `swath show DIR` takes the work directory, not the bucket URL. `swath du --from-dataset` refuses a dataset
  in a bucket and its work directory (exit 2). `--seed-from` reads a local path: to seed the next run, copy
  the dataset's `cuts.txt` ([the cut file](dataset.md#the-cut-file)).
- Datasets written to GCS have run against the real service
  ([measurements](internals/measurements.md#spill-at-billion-scale-and-a-spill-cap-too-small-2026-11-13));
  writing to S3 and to Azure Blob Storage is tested against emulators and in-process fakes only.
- Each store's own limits are on its page.

The report's `output.remote` holds the commit's outcome, the uploads and their retries, the parts a resume
verified and the orphans deleted or left ([run report](reference/run-report.md)).
