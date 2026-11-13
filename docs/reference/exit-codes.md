# Exit codes

Every swath command exits with one of these codes. The run report (`--report FILE`) and, inside a dataset
directory, `error.json` carry the same code with its class and cause.

| Code | Class | Meaning | Resumable |
|---|---|---|---|
| 0 | `completed` | the dataset is committed, the text rows or totals are written, or the reader closed stdout early (`\| head`); also `swath resume` on a dataset that is already complete | – |
| 1 | `fatal` | a failure a resume will not fix: access denied, no such bucket, credentials that do not load, another swath process owning the output, an invalid page from the service (`cause` `protocol`), more than 100,000 parts (`part_limit`), an output layout swath did not make (`unsafe_layout`), more prefixes than `--max-prefixes` (`prefix_limit`); for a dataset in a bucket, another run's manifest created first (`lost_race`), a retained part changed in the bucket (`remote_altered`), a saved manifest an earlier attempt may still create naming other parts (`candidate_mismatch`) | no |
| 2 | `usage`, `refused` | `usage`: the command line does not parse, or names an unknown scheme. `refused`: something the caller must fix before anything runs: a non-empty `-o` directory without `--overwrite`, `--overwrite` on a directory that is not a swath dataset, a new run over an interrupted one, an output that is or contains the listed tree, `--overwrite` of an output inside it, a resume whose identity changed or that has nothing to resume, an S3 directory bucket, a bucket or container `-o` whose prefix already holds a dataset or overlaps the listed source | `refused` leaves the output as it was: an interrupted run there can still be resumed |
| 3 | `incomplete` | the run finished, but its output does not hold the whole scope: entries could not be read (permissions; the dataset's `coverage.unreadable` and the report's `outcome.unreadable` count them), or `--max-keys` stopped the listing (`stop_reason` `max_keys`). The rows, totals or dataset are written (a dataset is committed); the last line on stderr says why, and `-q` keeps it. `swath resume` on such a dataset exits 3 again | no: fix the cause and run again |
| 74 | `disk_full` | a part, a journal record or the manifest could not be written: the filesystem is full (`cause` `no_space`) | yes, once there is space |
| 75 | `stuck`, `unresolved` | `stuck`: no progress for `--stall-timeout` (10 minutes by default; a warning at half). `unresolved` (`cause` `commit_unresolved`): a dataset in a bucket whose manifest's creation could neither be confirmed nor refuted; every part is kept | yes |
| 124 | `deadline` | `--max-duration` reached before the output was finished | yes |
| 130 | `interrupted` | SIGINT | yes |
| 143 | `terminated` | SIGTERM | yes |

"Resumable" means a dataset run of an object store, whose journal is intact: `swath resume DIR` continues it.
A local tree, text output and `swath du` keep no journal and are run again.

After SIGINT or SIGTERM swath stops and records its outcome, then on Linux and macOS dies by that signal, as
any command a signal stops: a shell reports 130 or 143 and a shell loop around swath stops too, while a program
that waits for it sees the signal (Python's `returncode` is -2 or -15). The report and `error.json` say 130 or 143.
On Windows the process exits with the code. A forced exit after a signal (below) ends the same way.

The `cause` of exit 75 says what rose while nothing progressed:

| Cause | What it means |
|---|---|
| `stuck_throttle` | the service kept throttling |
| `stuck_timeouts` | requests kept running past `--request-timeout` |
| `stuck_empty_chain` | the service kept answering empty pages with a new continuation token |
| `stuck_no_rows` | none of these: requests failing or hanging, or a stopped pipeline |

The other causes are the provider's failure class (`auth`, `access_denied`, `not_found`, `region_redirect`, `throttle`, `timeout`,
`parse`, `unsupported`), `protocol`, `no_space`, the limits named above, and a dataset in a bucket's `commit_unresolved`,
`lost_race`, `remote_altered` and `candidate_mismatch`. A destination that keeps failing an upload past its
retries ends the run `fatal`; its journal is intact, and `swath resume DIR` continues it once the destination
answers.

## What a supervisor does with each code

| Exit | Do |
|---|---|
| 0 | done |
| 3 | the output is usable but partial: accept it, or fix the permissions and run again |
| 130, 143 | `swath resume DIR --report NEW-FILE` |
| 75 | resume once; a second 75 in a row on the same output needs a person (with `stuck_empty_chain`, the service no longer moves past a point in the key space). `unresolved`: resume, which creates the same manifest again; without a journal (a local tree written to a bucket), a person checks whether the prefix holds a manifest before deleting anything there |
| 74 | free space, or move the directory to a larger disk, then `swath resume DIR` |
| 124 | resume only within a separate budget of attempts or total time: a source that keeps a run busy without finishing reaches 124 on every attempt; past the budget, a person |
| 1, 2 | a person: a resume will not fix it |

Give every unattended attempt a `--max-duration`, and set the supervisor's own kill deadline above
`--max-duration` plus 20 seconds: after a stop swath waits up to 15 seconds for the run to stop, then records
its outcome and exits, at most 20 seconds after the stop. An attempt killed by the supervisor may have no
report; treat its outcome as unknown, never as the one in an older `error.json`.

## The outcome of record

The outcome of an attempt is its exit code plus the report it wrote to `--report FILE` (on failure too). Give
each attempt a fresh path outside the output directory. The report's `outcome` has `class`, `exit_code`,
`resumable`, `cause`, `complete`, `stop_reason`, and `attempt.id` names the attempt
([run-report.md](run-report.md)).

`DIR/error.json` holds the same outcome inside a dataset directory that did not complete:

```json
{
  "class": "deadline",
  "exit_code": 124,
  "message": "deadline: --max-duration reached",
  "resumable": true,
  "stop_source": "timebox",
  "attempt": {
    "id": "5c3c3d59be9ea1c21fa747cc8b647131",
    "started": "2026-11-10T12:35:34.390093792Z"
  }
}
```

It is current only when its `attempt.id` is the report's: an attempt that could not write it (another process
owned the output, the disk refused the write) leaves the previous one, and says why in the report's
`outcome.unrecorded`. `error.json` is removed when a later run or resume completes.

A usage error or refusal (exit 2), and a failure before any request was answered (a missing bucket or
directory, credentials that do not load), print one line on stderr and no statistics:

```text
swath: nosuchdir: no such file or directory
```

How swath classifies and records an outcome, and its liveness rules: [../internals/run-lifecycle.md](../internals/run-lifecycle.md).
