# Run large listings

How to run one large listing: the command, the output directory it needs, the outcome it leaves and the limits
that stop it. Running it unattended, on a schedule and under a supervisor, is
[Run on a schedule](production.md); what each exit code means is [Exit codes](reference/exit-codes.md).

```bash
swath list s3://bucket/prefix/ -o /data/inv-2026-11-10 \
  --seed-from /data/inv-2026-11-09 \
  --report /var/log/swath/inv-2026-11-10.attempt-1.json \
  --max-duration 6h
```

Each question a long run raises has its own page:

- <a id="sizing-a-run"></a><a id="disk"></a><a id="memory"></a><a id="cost"></a>[Size a run](size-a-run.md):
  disk, memory and the cost of requests.
- <a id="watching-a-run"></a><a id="throttling"></a>[Watch and tune a run](watch-a-run.md): the progress line,
  the progress file, the signals to read, throttling and the flags to change.
- <a id="stopping-and-resuming"></a><a id="overwrite-and-refresh"></a><a id="seeding-a-refresh-from-the-previous-dataset"></a>[Stop, resume and refresh](resume-and-refresh.md):
  signals, `swath resume`, replacing an inventory and seeding a refresh from the previous one.
- <a id="listing-every-version"></a>[List every version](all-versions.md): S3 versions and delete markers, GCS
  generations, Azure Blob versions, snapshots and soft-deleted blobs.
- <a id="reading-the-output"></a><a id="verify-a-dataset"></a>[Query an inventory](query.md): reading and
  verifying the dataset a run writes.
- <a id="known-limits"></a>[Known limits](troubleshooting.md#known-limits): platforms, power loss, live scans
  and the scale measured so far.

## Limits and the outcome of record

- **Output directory.** `-o DIR` must be empty or absent. A non-empty directory is refused (exit 2) unless
  `--overwrite`, which replaces only a previous dataset of this tool. For a local source the output may not be
  the listed root or contain it; a new or empty directory inside the root is accepted (the listing skips it),
  but `--overwrite` of an output inside the root is refused, and a non-empty one is refused saying so: write
  to a new directory outside the root.
- **The outcome of record** is the exit code plus the report written to `--report FILE` when the attempt ends,
  on failure too. Give each attempt a fresh path outside the output directory (or remove the old file first).
  The report's `outcome` holds `class`, `exit_code`, `resumable`, `cause`, `complete`, `stop_reason` and, when
  the outcome could not be written to `DIR/error.json`, `unrecorded`; `attempt.id` identifies the attempt.
  Its schema: [Run report and progress file](reference/run-report.md).
- **`--max-duration`** (default 0, no limit) stops the run with exit 124; an object-store run is resumable.
  The stall watchdog stops a service that answers only empty pages; `--max-duration` is the only stop for one
  that keeps returning rows or pages forever. Give every unattended attempt one.
- **Process deadline.** A stop is graceful; if the run has not stopped 15 s later the process writes its
  outcome and exits, at most 20 s after the stop. A supervisor's own kill deadline belongs above
  `--max-duration` plus 20 s. An attempt killed that way may have no report: treat it as unavailable, never as
  the outcome in an older `error.json`.
- **`--part-interval`** (default 30 s; 0: size only) closes an open part that holds rows once it is that old.
  It bounds how long rows wait in an open part, not the age of the durable recovery point (what a resume
  keeps): a closed part is still encoded, synced and published in order, and when no rows reach the output
  (a stalled frontier) nothing is open to close. `part_age_ms` in the progress file is the time since a part was
  last published; it says nothing about how old the rows in the newest part are, nor how much listed work
  a resume would repeat. With 0, part boundaries do not depend on timing.
  Once listing is done and the rest of a sorted run is written as parallel segments
  (`diagnostics.resources.drain.segments.reason` `engaged`), the interval closes no part: the tail is cut by size and row
  count only ([ordering](internals/ordering.md#segments-parts-written-in-parallel-after-listing)). The tail's
  workers are at most one per two processors (at least 8); with them the tail is CPU-bound on Parquet encoding
  and spill read-back (`diagnostics.resources.drain.segments.write_ns`, `read_ns`, `decode_ns`).
- **`--stall-timeout`** (default 10 min; 0: never) stops a run with no progress and no usable answer from the
  service for that long (exit 75). A warning is printed at half.
- **`--spill-wait-timeout`** (default 0: wait) stops a sorted run whose listing has waited that long on a
  full spill cap (exit 74, `cause` `spill_budget`). A dataset run of an object store (one with a journal)
  resumes with more disk or a larger `--spill-budget`; text output and a local tree run again.
  Without it the run goes on at the output's pace, and says so on stderr after 2 minutes
  ([three bad patterns](watch-a-run.md#three-bad-patterns)).
