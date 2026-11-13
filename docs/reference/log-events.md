# Log events

With `--log-format json`, swath writes stderr as one JSON object per line and nothing else: no redrawn
progress line, no color and no statistics block (the `outcome` event and `--report` stand for it).

```bash
swath list s3://bucket/logs/ -o logs-inventory --log-format json -v 2> swath.log
```

`--log-format` is taken by every command, anywhere on the line before `--`, and again by `swath resume`. A
command line refused before the run starts (an unknown option, a bad value, a refused destination) still
reports in the format it names; for a line that does not parse, that is best effort.

Each object has `ts` (RFC 3339, UTC, milliseconds), `level` (`debug`, `info`, `warn`, `error`), `event`,
`attempt` (the attempt id, once a run has one), `msg` (the text line's words), then the event's fields:

| Event | Level | Fields |
|---|---|---|
| `start` | `info` (`-v`) | `source`, `mode` (what the run writes: `rows` on stdout, a text `file`, a `dataset`, `totals`, or a `count`), `destination` (the path, `stdout`, or absent for a count) |
| `progress` | `info` | the progress snapshot's fields, as `.swath/progress.json` has them (`rows`, `requests`, `duration_ms`, ...: [Run report and progress file](run-report.md#progress-file)) |
| `warning` | `warn` | none, or: a persistent failure's `host`, `cause`, `attempts`; unreadable entries' `unreadable` (their count) and `samples`; a goroutine dump's `stacks` (a run without an output directory, or with `-v`) |
| `request` | `debug` (`-vv` failed, `-vvv` all) | `op` (the method), `host`, `path` (the listing's own query parameters, such as `prefix`, as sent; every other parameter, name and value, shown as `[redacted]`), `status` (absent without an answer), `cause` (without an answer), `latency_ms` |
| `retry` | `debug` (`-vv`) | `host` and `path` (the attempt's request, redacted as a `request` event's), `try` (the call's attempt, from 1), `class` (the diagnostic label), `status`, `code` (the provider's), `cause`, `retry`, `wait_ms` |
| `dataset` | `info` (without `-v`; not with `-q`) | `swath du --from-dataset`: the dataset the totals are read from, as the JSONL rows' `dataset` member has it ([Output](../output.md#du-rows)): `path`, `source`, `schema_id`, `listing_started_at`, `listing_finished_at`, `complete`, `stop_reason`, `unreadable`, `filters`, `exclusions` |
| `read` | `info` (`-v`) | `swath du --from-dataset`: `rows` read under the prefix, `filtered` (dropped by the filters), `columns` read besides the key, and SeekParquet's `parts_opened`, `source_reads` and `source_bytes` |
| `outcome` | `info`, `error` on failure | `class`, `exit_code`, `resumable`, `cause`, `message` (the report's error; without `-v`, a credential refusal's one line: [troubleshooting](../troubleshooting.md#credentials-could-not-be-loaded)) |

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
{"ts":"2026-11-10T12:00:00.000Z","level":"info","event":"start","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"listing s3://bucket/logs/ to stdout","source":"s3://bucket/logs/","mode":"rows","destination":"stdout"}
{"ts":"2026-11-10T12:00:00.000Z","level":"info","event":"progress","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"listing · 120,000 objects · 2,000 keys/s (avg 2,000)","rows":120000,"ordered":0,"encoded":0,"parts":0,"verified_bytes":0,"advance":120000,"answered":121,"frontier":121,"empty_chain_pages":0,"throttles":0,"timeouts":0,"rows_retained":0,"requests":122,"retries":1,"inflight":8,"limit":64,"mutex_wait_ms":0,"mutex_max_hold_ms":0,"started_at":"2026-11-10T11:59:00Z","as_of":"2026-11-10T12:00:00Z","duration_ms":60000,"part_age_ms":60000}
{"ts":"2026-11-10T12:00:00.000Z","level":"warn","event":"warning","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"S3 127.0.0.1:9: connection refused (retrying; 11 attempts; the run stops as stuck after 10m0s without progress)","host":"127.0.0.1:9","cause":"connection refused","attempts":11}
{"ts":"2026-11-10T12:00:00.000Z","level":"debug","event":"request","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"request GET acct.blob.core.windows.net/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted]: 200 (42ms)","op":"GET","host":"acct.blob.core.windows.net","path":"/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted]","status":200,"latency_ms":42}
{"ts":"2026-11-10T12:00:00.000Z","level":"debug","event":"retry","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"attempt 2 acct.blob.core.windows.net/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted] failed (throttle): 503 SlowDown: Please reduce your request rate.; retrying in 400ms","host":"acct.blob.core.windows.net","path":"/c?comp=list&[redacted]&prefix=logs%2F&restype=container&[redacted]&[redacted]","try":2,"class":"throttle","status":503,"code":"SlowDown","cause":"503 SlowDown: Please reduce your request rate.","retry":true,"wait_ms":400}
{"ts":"2026-11-10T12:00:00.000Z","level":"info","event":"dataset","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"totals of the dataset logs-inventory, not a live listing: s3://bucket/logs/ as listed 2026-11-08T10:00:00Z (2 days ago); complete","path":"logs-inventory","source":"s3://bucket/logs/","schema_id":"swath/s3-objects/v1","listing_started_at":"2026-11-08T10:00:00Z","listing_finished_at":"2026-11-08T10:00:00Z","complete":true,"stop_reason":"completed","unreadable":0,"filters":[],"exclusions":[]}
{"ts":"2026-11-10T12:00:00.000Z","level":"info","event":"read","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"read 120,000 rows of logs-inventory (0 dropped by the filters), the key and size, last_modified: 1,048,576 bytes in 9 reads","rows":120000,"filtered":0,"columns":["size","last_modified"],"parts_opened":2,"source_reads":9,"source_bytes":1048576}
{"ts":"2026-11-10T12:00:00.000Z","level":"error","event":"outcome","attempt":"5f0c2a9e4b1d4c3a8e7f6a5b4c3d2e1f","msg":"stuck: no progress within the stall timeout (stuck_no_rows)","class":"stuck","exit_code":75,"resumable":true,"cause":"stuck_no_rows","message":"stuck: no progress within the stall timeout (stuck_no_rows)"}
```
<!-- events:end -->
