# Running swath in production

A short guide for whoever schedules and watches swath. The full reference is [running.md](running.md);
every option is in the [command reference](commands/swath.md). Numbers below are observations on the stated runs
([internals/measurements.md](internals/measurements.md)), not limits or promises.

## The nightly command

```bash
swath list s3://bucket/prefix/ -o /data/inv/2026-11-08 \
  --seed-from /data/inv/2026-11-07 \
  --report /var/log/swath/inv-2026-11-08.attempt-1.json \
  --max-duration 6h
```

- `-o DIR` writes a sorted Parquet dataset. `DIR` must be new or empty. Write each night to a new directory and
  point readers at it once its `manifest.json` exists ([running.md](running.md#overwrite-and-refresh)).
- `--seed-from` names the previous night's dataset (warm start, below). Leave it out on the first night.
- `--report FILE` is the outcome of record. Use a fresh path for each attempt, outside the output directory.
- `--max-duration` stops a run that takes too long. Give every unattended run one. Set the supervisor's own
  kill deadline above `--max-duration` plus 20 s.

What the supervisor does with the exit code:

| Exit | Do |
|---|---|
| 0 | done |
| 130, 143 (signals) | `swath resume DIR --report NEW-FILE` |
| 75 (stuck) | resume once; a second 75 in a row on the same output needs a person |
| 74 (disk full) | free space or move the directory to a larger disk, then `swath resume DIR` |
| 124 (deadline) | resume only within a separate budget of attempts or total time; past it, a person |
| 1, 2 | a person: a resume will not fix it |

## Three flags on a small runner

On a 1-vCPU machine with 1 to 2 GB of memory the defaults are the place to start. Change these three only
when the report says so.

- **`--workers`** (default 64 for an object store; for a local path 4 per CPU, so 4 here). Listing waits on request round trips, not the CPU. On one core the sorted
  output (ordering, spill and one Parquet encoder) is the limit, so more workers do not help there. Raise it
  (128, 256) only on a machine with spare cores and a service that does not throttle. Lower it if the service
  throttles at 64.
- **`--sort-buffer`** (default 1/16 of memory, at most 1 GiB). The bytes of rows held in memory between listing
  and encoding. Memory is the least of physical memory, the cgroup's limit and `GOMEMLIMIT`, and 128 MiB stands
  in when none is found: a 2 GiB container gets 128 MiB, an 8 GiB host 512 MiB, 16 GiB or more 1 GiB
  (`invocation.sort_buffer`: `reason`, `bound_by` and the limits found). Rows listed ahead of the output past it
  spill to disk: a listing whose waiting rows fit in the budget never spills. It is
  not a memory cap, and swath sets none: pages in flight, writers, the tail and the runtime come on top. The
  writers are admitted against their own allowance, the same 1/16 of memory whatever `--sort-buffer` says
  (`resources.drain.encoder_allowance_bytes`), so a smaller sort buffer does not mean fewer encoders. The measurements
  that follow were taken at a 128 MiB budget, the default before 0.4.0: sorted Parquet over the 251 production key lists through a 50 ms fake peaked at a median 0.46 GB and at most
  0.78 GB RSS (`head1`, `7b05605`) and 0.90 GB (`e2b`, `7fb447f8`, on ooni-data, 15.5M keys); six giants of
  267M to 523M keys peaked at 0.6 to 0.7 GB (`huge2`, `bf5e6d52`). Those runs were on machines of 16 to 32
  vCPU, and no run has been measured on a 1.8 GB machine; the 1 GiB default on such hosts has not been measured
  over those lists. If `resources.peak_rss_bytes` comes near the machine's memory, lower the budget. If spill is
  heavy (`resources.spill.rows` near the rows listed) or `resources.budget_wait_ns` is large and memory is free,
  raise it: past what a listing holds ahead of its output, more buys only tail workers.
- **`--spill-budget`** and the disk. Rows that are listed ahead of the point the output has reached go to
  disposable spill files on the output's filesystem. By default spill may use half the free space at the
  start, minus a reserve of at least 1 GiB. Set `--spill-budget` when the filesystem is shared. Size the disk
  as described below.

## Warm start for repeat listings

`--seed-from PREVIOUS-DATASET` divides the bucket into ranges from the previous dataset's key distribution.
The run then sends no delimiter requests and makes far fewer early splits. On 251 production key lists
served through a fake at 50 ms, a seeded run took a median 0.76x the cold run's wall time and sent 1.02 requests
per 1,000 keys (`warm1`, 2026-11-07). A prior that is 45 % out of date still listed at 1.11 to 1.15x the
64-worker ideal (`stale4`, 2026-11-07). The prior must be the same bucket and prefix, or the run is refused
(exit 2). The output is the same with or without a prior.

## A healthy run report

Five fields to check in the `--report` file of a sorted listing of a million keys or more:

| Field | Normal |
|---|---|
| `outcome.class` | `completed` (exit 0), with `outcome.complete` true; for the whole source, also the dataset's coverage ([check](getting-started.md#5-check-that-it-is-complete)) |
| `efficiency.http_attempts_per_1k_rows` | 1.0 to 1.4. On lists under a million keys, up to about 2 (the seed and first splits are a large share of a few hundred requests) |
| `resources.spill.refused` | 0 |
| `resources.budget_wait_ns` | 0 or close to it |
| `resources.frontier_stuck_max_ns` | a few request round trips: a median 0.28 s and at most 1 s on sorted runs of the 64 lists of 20M keys or more through a 50 ms fake (candidate 4, run `c4big`: its resumed and spill-capped arms) |

Spill is normal. A sorted run of a large bucket spills almost every row once (a median 96 % of rows on the 64
lists of 20M keys or more: candidate 4, run `c4big`, plain frames), because ranges finish in roughly
key-uniform order and the output waits for the leftmost one.

## Three bad patterns

**Spill refused, workers waiting.** `resources.spill.refused` is above 0 and `resources.budget_wait_ns` is
large. `resources.free_disk_min_bytes` is close to `resources.spill.reserve_bytes`, or the spill reached
`resources.spill.max_bytes`. Listing then runs at the output's pace. Give it disk: a larger filesystem for
the output, or a larger `--spill-budget` when the filesystem has the room. An interrupted run resumes with the
new value: `swath resume DIR --spill-budget 200GiB`.

**A stuck frontier.** `resources.frontier_stuck_max_ns` is seconds to minutes. In `progress.json`, `rows`
rises while `encoded` and `parts` stay flat and `part_age_ms` grows. One range at the front of the output is
slow: a slow, retried or hung request. Check `inflight.p99_ns` against `inflight.p50_ns` and the retries in
the report's `source.stats.counters.values`. After `--stall-timeout` (10 min) of no progress the run stops
with exit 75 and writes every goroutine's stack to `DIR/.swath/stacks-stuck.txt`. Resume it, and keep that file
if it happens again.

**Throttling.** `requests.throttles` rises, the counter `limiter.decreases` is above 0, and `progress.json`'s
`limit` falls below `--workers`. A background under 15 % of answers is tolerated without a cut. For a quota
shared with other clients, set `--request-rate N` below it. If a run crawls with the limit near 1 under
sustained throttling, use `--no-adaptive-limit --request-rate N` ([running.md](running.md#throttling)).

## Resume after a kill

`swath resume DIR` continues an S3, GCS or Azure Blob dataset run after a signal, a crash, an OOM kill or a full disk. A local
directory listing keeps no journal and starts over.

- **Kept:** every published part, verified by size and SHA-256 before listing continues. Verification read
  247 to 376 MiB/s with a cold page cache and 511 MiB/s warm on a 4-vCPU VM
  ([measurements](internals/measurements.md#recovery-and-part-count)), so 10,000 parts of 64 MiB take roughly 30 to 45
  minutes.
- **Replayed:** the rows of every certified spill file the stopped run sealed (key intervals listed ahead of
  the output), read back from `DIR/.swath/spill/durable/` instead of listed again.
- **Listed again:** the keys after the last published key that no certified file holds. The report's
  `resources.spill.durable.resume` says what was reused (`replayed_rows`, `gaps`, `requests_saved_est`);
  [running.md](running.md#stopping-and-resuming) owns the rules.

The resume replays the recorded command. Only the resource, reporting and request options may change
([swath-resume](commands/swath-resume.md)). Give each attempt a fresh `--report` path.

## Where things are

The output directory's layout is in [swath-list](commands/swath-list.md#files). Spill lives under
`DIR/.swath/spill/`, on the output's filesystem: disposable spill is removed by the next run, and certified
spill (`spill/durable/`) is kept after a stop for the resume and removed when the dataset commits
([running.md](running.md#stopping-and-resuming)); text on stdout spills under
`$TMPDIR`. On tmpfs spill uses memory: the report's `resources.spill.fs_type` names the filesystem and the run
warns at start.

## Writing to a bucket

`swath list SOURCE -o gs://BUCKET/PREFIX --work-dir DIR` (or `-o s3://BUCKET/PREFIX`) puts the dataset in GCS
(or S3) and keeps nothing large on the runner: the parts go up as they close, and `DIR` holds only spill and the
run's metadata.

- **Credentials (GCS):** the destination's are its own: `--output-gcs-credentials-file FILE` (a service account
  key or authorized-user JSON) or Application Default Credentials, with the read-write scope. It needs
  `storage.objects.create`, `get` and `delete` on the prefix (delete removes this run's own orphans, by
  generation). The source's `--anonymous` or credentials never apply to it. A rotated key file is given to
  `swath resume DIR` again; the endpoint (`--output-endpoint-url`) is fixed for the run.
- **Credentials (S3):** `--output-s3-profile NAME` (a shared config profile: keys, SSO, an assumed role, a
  credential process) or the AWS SDK's default chain (environment, `AWS_PROFILE`, web identity, container and
  instance roles); requests are always signed, whatever the source's `--anonymous` or `--s3-profile`. Note that
  the default chain reads the same environment as the source's: give the destination a profile when the two
  must differ. It needs, on the bucket (`arn:aws:s3:::BUCKET`), `s3:ListBucket`: without it S3 answers a
  missing `manifest.json` with 403, not 404, and the fresh-prefix check fails. On the dataset's objects
  (`arn:aws:s3:::BUCKET/PREFIX/*`) it needs `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` (orphans),
  `s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts` (an abort's confirmation); in a versioned bucket
  also `s3:GetObjectVersion` (reads pinned to a version id) and `s3:DeleteObjectVersion` (orphans, by version
  id). The region is `--output-s3-region`, else `AWS_REGION`,
  `AWS_DEFAULT_REGION` or the profile's, else `us-east-1`; the one resolved, and `--output-endpoint-url`
  (an S3-compatible store, addressed path-style), are fixed for the run. The environment's endpoint variables
  (`AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3`, FIPS, dual-stack) are never applied to the destination; a CA
  bundle is (`AWS_CA_BUNDLE`, or the profile's `ca_bundle`). The profile may change on resume.
- **S3 multipart uploads and the lifecycle rule (required):** a part larger than 8 MiB is a multipart upload.
  A failed or cancelled upload is aborted and the abort confirmed, but one a killed process leaves is not (its
  id dies with it), nor one whose abort the bucket refused or did not confirm (the report's
  `aborts_unconfirmed`), and S3 keeps an incomplete upload's parts, billed and invisible to listings, until it
  is aborted. Set a lifecycle
  rule on the bucket that aborts incomplete multipart uploads (`AbortIncompleteMultipartUpload`,
  `DaysAfterInitiation` 1 or more, on the prefix or the bucket) before writing to it.
- **One prefix and one work directory per run:** a prefix that holds a `manifest.json` is refused; datasets in
  a bucket are not replaced. Write each run to a dated prefix and let readers find the newest by its manifest.
  A work directory belongs to its run (`--overwrite` is refused with a bucket): a new run takes a new or empty
  one.
- **Supervision:** as for a local dataset, `swath resume DIR` until exit 0. Give every attempt the same work
  directory; if the runner's disk is lost the resume is lost with it, and a new run starts over at a new
  prefix. Two runners sharing one work directory are refused by its lock; two work directories writing one
  prefix race at the commit, and exactly one wins (the other exits with a lost race and deletes its parts).
- **Windows:** a bucket run of an object store keeps a journal, which Windows refuses (directories cannot be
  synced there: [running.md](running.md#known-limits)), so it is refused there, as a journaled local dataset is;
  a local tree, which keeps no journal, can be written to a bucket on Windows, without resume.
- **Lifecycle rules:** never let a lifecycle rule or a cleanup job delete or replace objects under a committed
  dataset's prefix: the manifest would name parts that are gone, and swath does not notice. Expire objects
  only under the prefixes of runs you have abandoned; a rule that aborts incomplete uploads is safe, and on S3
  required (above).
- **Debug logging:** the destination's credentials and clients are built with a logger that discards
  everything, as the GCS source's client is, so `GOOGLE_SDK_GO_LOGGING_LEVEL=debug` never logs a token, a signed
  assertion or a session URL. The storage SDK (v1.70.0) does not hand that logger to the JSON API client it
  builds inside, so with the variable set that client may still log request lines: the method, the object name
  and a URL without credentials. An S3 destination's AWS SDK logs nothing: its logger discards everything and
  its request logging is off, whatever the AWS configuration says.
- **Leftovers:** a run that is abandoned (never resumed to its commit) leaves its uploaded parts under
  `PREFIX/data/RUN/` with no manifest, named in `DIR/.swath/remote/allocated`; delete them, or set a lifecycle
  rule on the bucket. Unfinished resumable upload sessions expire on their own (GCS keeps them a week); S3's
  incomplete multipart uploads do not (the rule above).
- **An unresolved commit** (`commit: unresolved`: the manifest's creation failed in a way that does not say
  whether it happened, and no manifest is there yet) keeps every part: resume the work directory, which
  creates the very same manifest again. A run without a journal (a local tree) cannot resume: check the prefix
  for a manifest before deleting anything under it.
- **Report:** `output.remote` in the report ([run report](reference/run-report.md)): the commit's outcome, the
  uploads, retries by class, `upload_wait_ns` (the encoders' time waiting on uploads), the orphans deleted and,
  for S3, `aborts_unconfirmed` (multipart uploads the lifecycle rule must remove).
  Uploads happen inside the run, so the bucket's bandwidth is part of its wall time.

## Disk sizing

- **The dataset:** about 30 bytes per row at the default part size (the live 1.09-billion-object listing of
  run `cogslive1`, `bf5e6d52`, 256 workers, wrote 488 parts, 32 GB). Measure your own from a representative
  prefix: `output.output_bytes` over `output.rows_committed`.
- **Spill per row:** frames are prefix-coded and compressed (zstd) by default: 25 to 53 bytes per row on production
  keys with real ETags in the frame benchmark, and about 30 bytes per row on the live listing above (`cogslive1`:
  32 GB for 1.08 billion spilled rows). At the peak, a median 71 % of the rows were listed ahead of the output
  on lists of 20M keys or more (candidate 4, run `c4big`, plain frames), so live spill can come close to the
  dataset's own size.
- **Peak spill** ≈ 0.7 × rows × spill bytes per row. The default spill budget is half of (free space at the
  start − the 1 GiB reserve), so the free space needed is about the dataset plus twice the peak spill plus
  1 GiB.
- **A dataset in a bucket:** the work directory needs no room for the dataset. Size it as the spill budget
  plus metadata plus the 1 GiB reserve: set `--spill-budget` explicitly (the default, half the free space,
  follows whatever disk there is), and allow for the metadata, which grows with the parts (a claim receipt of
  under 1 KiB and a journal record each; the candidate manifest about 1 KiB a part, at most about 1.1 GiB at
  the 100,000-part limit). Memory grows instead by one upload buffer (an 8 MiB GCS chunk or S3 part) per
  encoder, admitted with its writer. A smaller spill budget trades disk for listing that waits on the output; measure a representative
  run's `resources.spill.peak_bytes` before shrinking the disk.
- **Example, 200 million objects:** a 6 GB dataset; peak spill 4.2 GB at 30 B per row or 7.4 GB at 53 B per row;
  so 15 to 22 GB free. Both inputs are assumptions until a representative prefix measures them. With less
  disk the run still completes, but frames are refused and listing slows to the output's pace (the first bad
  pattern above); set `--spill-budget` explicitly when you know the room.
