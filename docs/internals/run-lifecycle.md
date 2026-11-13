# Run lifecycle

How the command decides a run's outcome and records it, when it declares a run stuck, how it stops, and
how it checks the paths it writes beside the output. The user's view is
[reference/exit-codes.md](../reference/exit-codes.md); the journal and resume are
[durability.md](durability.md).

## Outcome classification and recording

A supervisor's outcome of record is the exit code plus the report at the path it names with `--report`
(outside the output directory, a fresh path per attempt, or removed before the attempt starts): `outcome`
(`class`, `exit_code`, `resumable`, `cause`, `unrecorded`; `status`, `complete` and `stop_reason` follow the
class: a run that did not commit is `complete` false with its class as the `stop_reason`, and a committed
run's agree with the manifest's `coverage`) and `attempt` (`id`, random, and `started`). For a run that did
not complete, `DIR/error.json` holds the same outcome in the dataset (`class`, `exit_code`, `message`,
`resumable`, `cause`: the provider's failure class such as `throttle` or `access_denied`, or `no_space` for a
full disk; and the same `attempt`); it never needs stderr.

The command classifies the outcome once, before any account is written, and that classification is frozen:
`DIR/run.json` (a committed dataset), `error.json`, the report, the exit code and a forced exit's outcome agree
on `class`, `exit_code`, `resumable` and `cause`, also when a stop cause (the deadline, a signal) arrives after
the run settled (a dataset committed with partial coverage is `incomplete`, exit 1, in all of them). A
protocol violation (`cause` `protocol`, which the journal records, so `swath resume` refuses the run), the part
limit (`part_limit`) and an output layout swath did not make (a linked `data/`, workspace, journal or part,
`unsafe_layout`) outrank a stop cause; the refused paths are left as they were.

The report's `output` counts the parts that stay in the output on failure too: after a failure of a journaled
run, `parts_written`, `output_bytes` and `rows_durable` are the parts the journal acknowledged (the append
returned, a failed rename after it included: the resume renames them), the recovered ones included in
`output_bytes` and `rows_durable`.

`error.json` is a convenience: it is current only when its `attempt.id` is the report's. No program can
promise a new file on storage that refuses writes, so an attempt whose `error.json` write fails leaves the
previous one in place, possibly with the same class and exit code; its report names another attempt and says
why in `outcome.unrecorded`. With no report (it could not be written either, which fails a command that
succeeded with exit 1 and joins the error line otherwise; a usage error, exit 2, before the flags were read),
only the exit code is current.

`error.json` and the progress file are written only while the run holds the output lock, and `error.json` is
removed when a later run or resume completes. It is always the last attempt's outcome: a resume (or
`--overwrite`) that fails before it runs (credentials that do not load, a damaged journal, a failed
recovery) takes the lock and records its own outcome over the interrupted run's, if that run is still the one
it read (its journal and `error.json` unchanged) and the dataset still uncommitted once it holds the lock. An
attempt that cannot record it (another process owns the output, whose outcome stands; another process
completed or replaced the run first; a refusal that leaves the directory as it was, such as a directory
without a journal, where nothing is created, or an unusable output; a failed write) warns of it on stderr
with the reason (with `-v` only for a one-line failure, below); the JSON report carries the reason as
`outcome.unrecorded`, which is never empty when the outcome is not in `error.json`.

The outcome as one JSON line on stderr, `swath: outcome: {...}` before the error line, comes with `-v`; under
`--log-format json` it is the `outcome` event ([reference/log-events.md](../reference/log-events.md)).

A refusal or usage error (exit 2), and a failure before any listing request was answered or directory read
(a missing directory or bucket, credentials that do not load), print one line on stderr, `swath: MESSAGE`,
and nothing else: no statistics, no `INCOMPLETE`; a usage error adds its `See 'swath CMD --help'.` hint. A
stop (a signal, `--max-duration`, `--stall-timeout`, a full disk) and a failure after requests were answered
keep the statistics. `--stats` still prints them.

`swath du` writes its rows once the count completes, before the run's end is stamped and its outcome settled,
so the report's times include the writing: a run that fails or stops before prints none; a failed write fails
the run (`status` `failed`, a full disk `disk_full`), and so does a stop that comes while the rows are written
(a signal, `--max-duration`), since they may be cut short; a reader that closes stdout ends it with exit 0,
`status` `stopped`, `stop_reason` `downstream_closed`, as text output's.

## Liveness

The command alone decides that a run cannot go on: its watchdog watches the run's progress in key space (rows
listed, ordered, encoded, parts published, and the listing's pages: a page that held an entry or ended a
chain, probes included) and stops the run as stuck after `--stall-timeout` without a rise. So work queued
behind a low request limit is alive while the service answers pages; a source that hangs, or only fails or
throttles, is stuck. The `cause` names what rose meanwhile: `stuck_empty_chain` (empty continuing pages past
the first 1,024 of their run), `stuck_throttle` (throttle answers), `stuck_timeouts` (attempts past the
request timeout, no throttle) or `stuck_no_rows` (none of these: failing or hung requests, or a stopped
pipeline). A warning comes at half the timeout.

An empty continuing page (S3 and GCS may return them) counts only within the first 1,024 of its run
(`keyrange.EmptyChainLive`, the token window below): a service that answers forever with empty pages and fresh
tokens stops as stuck, `cause` `stuck_empty_chain`, about 1,024 pages plus `--stall-timeout` after the run of
them began, without `--max-duration`. A legitimate run of empty pages longer than that is still followed while
anything else progresses, and is stopped only if nothing does for the stall timeout. A source without the
range engine (a supplied client) is live by its answered requests instead. Nothing in the pipeline gives up on
a slow source (S3 requeues a page after its retries). With the watchdog off (`--stall-timeout 0`), only
`--max-duration` bounds such a service. A run of empty continuing pages must not repeat any of its last 1,024
tokens (an invalid page: exit 1, `cause` `protocol`, the run marked unresumable); a cycle longer than that
window is not detected, and is bounded as above.

A finite `--max-duration` belongs on every unattended attempt (the default, 0, has no limit): the watchdog
stops a service that answers forever with empty pages, but not one that keeps returning rows or pages forever,
nor anything with `--stall-timeout 0`.

## Stopping and the forced exit

A stop (a signal, `--max-duration`, `--stall-timeout`) is graceful: published parts and the journal stay, and
`error.json` and the report are written. Only the first signal counts. The process exits with its code 15 s
later if the run has not stopped by then (I/O that does not return, say), first writing (bounded at 2 s, as
the dump below) the same outcome the graceful stop would have to `error.json` and a minimal report without the
run's statistics, its `message` naming the forced exit. The exit does not wait for any of that beyond 5 s after
the grace (20 s after the stop in all): each line to stderr waits at most 250 ms and is dropped past it, so a
stderr pipe nobody drains cannot hold the process (warnings are written the same way), and the process exits at
that bound whatever still blocks. A supervisor's own kill deadline therefore belongs above `--max-duration`
plus 20 s; an attempt killed that way may have no report, which is treated as unavailable, never replaced by
an older `error.json`.

A stderr that does not drain never holds a run either: a line waits at most 250 ms, later ones queue (up to
256) without waiting while the write is stuck and are then dropped, counted in `requests.log_dropped`.
Warnings, errors and the statistics have a bound of their own (256, the oldest dropped past it, counted), and
progress and status records are disposable on every path. The final outcome and error lines go before the
queued request log and progress records, which are dropped (counted), after the queued warnings, within their
own 250 ms; past it the warnings left are dropped, counted, and the outcome gets 250 ms more.

**Forensics.** At a stall, before the run is canceled, and at a forced exit, every goroutine's stack and the
progress counts are written whole to `DIR/.swath/stacks-stuck.txt` or `stacks-forced-exit.txt` (replaced each
time) while the run owns the output, else to a new `swath-stacks-stuck-*.txt` or `swath-stacks-forced-exit-*.txt`
in the temporary directory (`$TMPDIR`), and stderr gets one warning naming the file. The stacks are forensics,
not output: they go to stderr as well only with `-v`, or instead when no file could be written within 2 s, cut
at 1 MiB. Under `--log-format json` a run without an output directory writes no file and keeps them in the
warning event's `stacks` field, where a supervisor collecting the log finds them (`TestStacksWholeAndBounded`).

**The persistent-failure warning.** Without `-v`, when requests have failed with one transport or service
cause (a connection refused, a 503) for 10 s with none answered, one warning names the provider, the endpoint
host and the cause and says how long retrying lasts (until the stall timeout, or for a connection that
nothing has ever answered at, the clients' 15 s fail-fast grace:
[clients](clients.md#what-swath-keeps)), and again at most once a minute while it lasts; a run that then ends stuck or at its
deadline names the cause in its final error line, the outcome's `message` and the report's `outcome.error`
(and so does one that ends so with no request ever answered, however short it was);
`requests.failure_warnings` counts the warnings.

## Side paths

`--report FILE` may not land on managed output (exit 2, checked before the source is opened, and by `swath
resume` before it settles anything): the text output file, the dataset directory's own entries
(`manifest.json`, `data/`, `run.json`, `error.json`, `.swath/` with the lock and journal) or anything below
them, or anything inside another dataset or run of this tool; an ordinary name such as `OUT/report.json` is
fine. FILE is resolved once and written where it was checked: its directory's symbolic links are resolved
before a `..` after them applies (as an open climbs from a link's target), its own name is checked as the entry
it is even while a symbolic link there points elsewhere, and so is where that link leads, a dangling one
included, and the output is found by file identity (on a case-insensitive filesystem `out/` is `OUT/`).
Another dataset or run is recognized by a heuristic: a `.swath/lock` or `.swath/journal`, or a
`manifest.json` with this tool's `format` and a `coverage` object; an earlier layout's marks (`_SUCCESS`
beside `data/`, a `schemaId` manifest) and other tools' manifests are not. The `--trace` file, the
`SWATH_CPUPROFILE` and `SWATH_MEMPROFILE` files, and the profiles' phase files beside them (`FILE.listing`,
`FILE.tail`, `FILE.exit`), are checked the same way and may not be symbolic links (a profile is opened, never
through a link; the report replaces its name by a rename, which replaces a link there rather than writing its
target). Nor may they be in the output directory, even as an ordinary name: they are opened before the run,
which would then find the directory it is to create or fill not empty and refuse it; only `--report`, written
after the run, may go there.

## The part limit

A dataset has at most 100,000 parts (`part-00000` to `part-99999`: ordinals have five digits,
`dataset.MaxParts`); at the defaults (64 MiB parts, about 30 bytes per row) it takes about 200 billion objects
or 35 days of uninterrupted 30-second time cuts; in practice a small `--part-size` or
`--part-rotation-max-rows` reaches it. A run that would open part 100,000 first publishes every earlier part,
then stops with exit 1, class `fatal`, `cause` `part_limit`, not resumable: the part size and row cap are frozen
in the resume identity, so a resume would stop at the same place. Start over (`--overwrite`) with a larger
`--part-size` or without `--part-rotation-max-rows`.

`--part-rotation-max-rows N`'s cut depends on the rows only, so it is part of the resume identity, a resume
reproduces the boundaries, and a row-cut part does not calibrate the size ratio (only a part closed by size
does); `resources.part_row_cuts` counts them. `--part-interval` is checked every eighth of the interval and
does not close a part whose boundary a resume fixed.

## Encoder sizing

The default `--encoders` is GOMAXPROCS lowered to what the resources carry: one writer's working set and one
state of the shared codec, priced from the provider's schema
([per-schema sizes](parquet-output.md#writer-buffers-and-admission)) plus a dataset in a bucket's upload
chunk, each from an encoder allowance of 1/16 of the memory found (at most 1 GiB, whatever `--sort-buffer`
says), apart from the rows, and the descriptor limit (after the spill readers, the workers'
connections and a reserve), never fewer than one. An explicit `--encoders N` is kept as given; it bypasses
default resource admission, with a warning when the resources cannot carry it. For a `.zst` text file the same
flag is the zstd jobs compressed at once, 16 MiB each from that allowance (no descriptor each); one job is a
synchronous encoder on the order stage ([running.md](../running.md#memory)). A job is a 4 MiB input section
(four 1 MiB windows: sorted listings match between neighboring keys, and a 1 MiB window measured the ratio and
speed of the 8 MiB default at a seventh of the memory), its output, the overlap the next job takes and its
encoder: measured at 5 to 13 MiB a job. A file's rows are formatted by `--encoders` goroutines
(`output.text.formatters`) into one ordered stream; their queued batches are rows in flight, charged to
`--sort-buffer`, and the chunks they format are about the same size ([ordering](ordering.md#the-waits)).

## The request limiter

`--workers` is the listing goroutines and the adaptive limiter's start; the limiter's window, growth rules,
timeout and latency brakes, and its counters are [clients.md](clients.md#request-admission). `--request-rate`
is refused when its spacing exceeds half `--stall-timeout`, which would read as a stall.
