# Log events

With `--log-format json`, swath writes stderr as one JSON object per line and nothing else: no redrawn
progress line, no color and no statistics block (the `outcome` event and `--report` stand for it).

```bash
swath list s3://bucket/logs/ -o logs-inventory --log-format json -v 2> swath.log
```

`--log-format` is taken by every command, anywhere on the line before `--`, and again by `swath resume`. A
command line refused before the run starts (an unknown option, a bad value, a refused destination) still
reports in the format it names; for a line that does not parse, that is best effort.

Each object has `ts` (RFC 3339, UTC, six fraction digits, as every time swath writes in JSON), `level` (`debug`,
`info`, `warn`, `error`), `event`, `attempt_id` (the report's `attempt.id`, once a run has one), `run_id` (the
report's `run.id`, once the run has opened its output: the same for a run and its resumes), `msg` (the text line's
words), then the event's fields. Wherever an event has a `cause`, it is a label from a fixed vocabulary; the
free text is in `error`. Counts of HTTP attempts are `http_attempts`, as in the [run report](run-report.md).

| Event | Level | Fields |
|---|---|---|
| `start` | `info` (`-v`) | `source` (as the report's `invocation.source` and the manifest's `source.uri` spell it: a local path absolute), `mode` (what the run writes: `rows` on stdout, a text `file`, a `dataset`, `totals`, or a `count`), `destination` (the path, `-` for stdout, or absent for a count) |
| `progress` | `info` | the progress snapshot's fields, as `.swath/progress.json` has them (`state`, `rows_fetched`, `http_attempts`, `duration_ms`, ...: [Run report and progress file](run-report.md#progress-file)), but for its ids, which the envelope carries |
| `warning` | `warn` | `code` (below), then for some codes: a persistent failure's `host`, `cause` (as a `request` event's), `error` and `http_attempts`; unreadable entries' `unreadable` (their count) and `samples`; a goroutine dump's `stacks` (a run without an output directory, or with `-v`) |
| `request` | `debug` (`-vv` failed, `-vvv` all) | `op` (the method), `host`, `path` (the listing's own query parameters, such as `prefix`, as sent; every other parameter, name and value, shown as `[redacted]`), `status` (absent without an answer), `cause` (a failed exchange's: `http_STATUS` for an answer of 400 or more; without an answer `unreachable` (the connection refused, its host not found or unroutable), `timeout` or `connection`), `error` (without an answer, the transport's), `latency_us` |
| `retry` | `debug` (`-vv`) | `host` and `path` (the attempt's request, redacted as a `request` event's), `try` (the call's attempt, from 1), `status`, `code` (the provider's), `cause` (the failure's label, the vocabulary of the report's `outcome.cause`: `throttle`, `timeout`, `auth`, ..., `other`), `error`, `retry`, `wait_ms` |
| `dataset` | `info` (without `-v`; not with `-q`) | `swath du --from-dataset`: the dataset the totals are read from, as the JSONL rows' `dataset` member has it ([Output](../output.md#du-rows)): `path`, `source`, `schema_id`, `listing_started_at`, `listing_finished_at`, `complete`, `stop_reason`, `unreadable`, `filters`, `exclusions` |
| `read` | `info` (`-v`) | `swath du --from-dataset`: `rows` read under the prefix, `filtered` (dropped by the filters), `columns` read besides the key, and SeekParquet's `parts_opened`, `source_reads` and `source_bytes` |
| `spill_pressure` | `error` (not with `-qq`) | a sorted run's listing has waited on a full spill cap for 2 minutes, and every 30 minutes while it waits ([production](../production.md#three-bad-patterns)): `since` (when the wait began), `spill_bytes`, `spill_max_bytes`, `spill_reserve_bytes`, `output_rows_per_s` (rows handed to the output a second since the wait began or the previous event) |
| `outcome` | `info`, `error` on failure | `class`, `exit_code`, `resumable`, `cause`, `message` (the report's error; without `-v`, a credential refusal's one line: [troubleshooting](../troubleshooting.md#credentials-could-not-be-loaded)) |

Names and messages from the source (a local path, a key, a provider's error, a dataset's recorded source) are
untrusted, and a line is never a place to forge another. In text, a control character (C0, DEL, C1), U+2028,
U+2029, a bidirectional control and each byte that is not valid UTF-8 is written as a visible escape (`\n`,
`\x1b`, `\xff`, `\u2028`) and every other character stays as it is; a backslash is not doubled, so a Windows
path reads as it is. In JSON the string is exact, with those characters as `\u` escapes; bytes that are not
valid UTF-8 read back as U+FFFD (the rows' `_base64` members carry the exact bytes: [Output](../output.md#jsonl)).

A warning's `code` says what it warns of, whatever its words: `request_failures` (requests failing with one cause,
none answered, for 10 seconds), `unreadable` (entries that could not be read), `no_progress` (half the stall
timeout without progress), `stuck` (the stall timeout: the run stops), `stacks` (a goroutine dump),
`forced_exit` (still stopping after the shutdown grace), `outcome_unrecorded` (the outcome is not in
`error.json`), `trace_unwritten` (`--trace`), `profile_unwritten` (a development profile), `prefix_not_found` and
`coverage_unknown` (`swath du --from-dataset`: no key under the prefix; a manifest that does not say whether its
listing completed), `anonymous_credentials_ignored` (`--anonymous` with credentials in the environment),
`tls_unverified` (`--no-verify-ssl`), `namespace_unread` (Azure Blob: listed as flat), `directory_too_large` (a
local directory left out), `lock_unenforced`, `durable_spill_off`, `unresumable_not_marked`,
`seed_from_cuts_refused`, `seed_from_unavailable` (a resume without its `--seed-from` prior), `encoders_limited`,
`spill_not_removed`, `spill_in_memory` (spill on `tmpfs`), and for a committed dataset whose aftermath failed:
`run_report_unwritten`, `cuts_unwritten`, `commit_marker_unwritten`, `aftermath_unfinished`.

The verbosity options gate the events as they gate the text lines: `start` needs `-v`, `request` events `-vv`
or `-vvv`, and progress records `-v` or `--progress`; they come every `--progress-interval` (30 seconds by
default) whether or not stderr is a terminal. The `outcome` event ends every run and every failure unless
`-qq` is given.

A stderr that does not drain never holds the run. Warnings and errors are kept in a bounded queue of their own;
progress records and request events are dropped first. The final `outcome` event is written before the queued
request events. What was dropped is counted in the report's `requests.log_dropped`.

Each event once:

<!-- events:begin -->
```json
{"ts":"2026-11-10T12:00:00.000000Z","level":"info","event":"start","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"listing s3://bucket/logs/ to stdout","source":"s3://bucket/logs/","mode":"rows","destination":"-"}
{"ts":"2026-11-10T12:00:00.000000Z","level":"info","event":"progress","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"listing · 120,000 objects · 2,000 keys/s (avg 2,000)","progress_version":1,"state":"listing","rows_fetched":120000,"rows_emitted":0,"rows_encoded":0,"parts_written":0,"verified_bytes":0,"scanned_bytes":0,"advance":120000,"answered":121,"frontier":121,"empty_chain_pages":0,"throttles":0,"timeouts":0,"rows_retained":0,"http_attempts":122,"retries":1,"inflight":8,"limit":64,"mutex_wait_ns":0,"peak_mutex_hold_ns":0,"started_at":"2026-11-10T11:59:00.000000Z","as_of":"2026-11-10T12:00:00.000000Z","duration_ms":60000,"part_age_ms":60000}
{"ts":"2026-11-10T12:00:00.000000Z","level":"warn","event":"warning","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"S3 127.0.0.1:9: connection refused (retrying; 11 attempts; the run stops as stuck after 10m0s without progress)","code":"request_failures","host":"127.0.0.1:9","cause":"connection","error":"connection refused","http_attempts":11}
{"ts":"2026-11-10T12:00:00.000000Z","level":"debug","event":"request","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"request GET acct.blob.core.windows.net/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted]: 200 (42ms)","op":"GET","host":"acct.blob.core.windows.net","path":"/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted]","status":200,"latency_us":42000}
{"ts":"2026-11-10T12:00:00.000000Z","level":"debug","event":"retry","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"attempt 2 acct.blob.core.windows.net/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted] failed (throttle): 503 SlowDown: Please reduce your request rate.; retrying in 400ms","host":"acct.blob.core.windows.net","path":"/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted]","try":2,"status":503,"code":"SlowDown","cause":"throttle","error":"503 SlowDown: Please reduce your request rate.","retry":true,"wait_ms":400}
{"ts":"2026-11-10T12:00:00.000000Z","level":"info","event":"dataset","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"totals of the dataset logs-inventory, not a live listing: s3://bucket/logs/ as listed 2026-11-08T10:00:00Z (2 days ago); complete","path":"logs-inventory","source":"s3://bucket/logs/","schema_id":"swath/s3-objects/v1","listing_started_at":"2026-11-08T10:00:00.000000Z","listing_finished_at":"2026-11-08T10:00:00.000000Z","complete":true,"stop_reason":"completed","unreadable":0,"filters":[],"exclusions":[]}
{"ts":"2026-11-10T12:00:00.000000Z","level":"info","event":"read","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"read 120,000 rows of logs-inventory (0 dropped by the filters), the key and size, last_modified: 1,048,576 bytes in 9 reads","rows":120000,"filtered":0,"columns":["size","last_modified"],"parts_opened":2,"source_reads":9,"source_bytes":1048576}
{"ts":"2026-11-10T12:00:00.000000Z","level":"error","event":"spill_pressure","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"spill cap too small: listing has waited on the output for 2m0s: the spill cap cannot hold the rows listed ahead of it (spill 91.0 GiB, cap 91.0 GiB, free-space reserve 1.00 GiB); the output writes 9,800 rows/s; stop the run and resume it with more disk or a larger --spill-budget (swath resume inventory --spill-budget SIZE), or let it run: it finishes, slowly","since":"2026-11-10T11:58:00.000000Z","spill_bytes":97710505984,"spill_max_bytes":97710505984,"spill_reserve_bytes":1073741824,"output_rows_per_s":9800}
{"ts":"2026-11-10T12:00:00.000000Z","level":"error","event":"outcome","attempt_id":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","run_id":"0d4e8f6a2b1c4d3e9f8a7b6c5d4e3f2a","msg":"stuck: no progress within the stall timeout (stuck_no_rows)","class":"stuck","exit_code":75,"resumable":true,"cause":"stuck_no_rows","message":"stuck: no progress within the stall timeout (stuck_no_rows)"}
```
<!-- events:end -->
