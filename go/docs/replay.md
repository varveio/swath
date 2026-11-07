# swath-replay (Go)

`swath-replay` serves a recorded listing back through the S3 `ListObjectsV2` API, so a lister can be tested
and benchmarked against a real bucket's shape without the bucket. It is a contributor tool, built from the same
module as `swath`, a separate binary (`cmd/swath-replay`), and a drop-in for the Java `swath-replay serve`
([`../../docs/swath-replay.md`](../../docs/swath-replay.md)): the same flags, the same startup line, the same
metrics endpoint and the same bytes on the wire for the S3 route ([internals/replay.md](internals/replay.md)
says what "the same" covers and how it is checked).

It is not an object store: no object data, no writes, no authentication, path-style addressing only, current
objects only.

## Build and run

```bash
cd go
./build.sh -o swath-replay ./cmd/swath-replay        # CGO_ENABLED=0, revision stamped
./swath-replay serve --fixture /data/rec/noaa-normals-pds/dataset/data --bucket noaa-normals-pds \
  --host 127.0.0.1 --port 19090 --metrics-port 19192 &
until curl -sf http://127.0.0.1:19192/healthz >/dev/null; do sleep 0.2; done
curl 'http://127.0.0.1:19090/noaa-normals-pds?list-type=2&max-keys=10&encoding-type=url'
./swath list s3://noaa-normals-pds -endpoint http://127.0.0.1:19090 -path-style -anonymous -o /tmp/ds
kill %1
```

`--fixture` is a sorted listing: a dataset's `data/` directory or a format-2 dataset directory, a directory of
sorted parts written by `swath --sort` or `swath-replay sort-fixture`, or one such part. A fixture that cannot
be served sorted is refused when it opens, with the reason. A file ending
in `.tsv` is instead a key list loaded into memory (one object per line: key, size, last-modified microseconds,
ETag, storage class, owner id, owner display name, checksum algorithm, checksum type; fields percent-escaped,
`\N` for null), for tests and for listings that have no Parquet form; `bench/replaydiff -dump-tsv` writes one.

`swath-replay --version` prints the build.

`swath-replay sort-fixture --capture PATH --output DIR [--key-limit N]` turns an unsorted capture (one Parquet
part in the 13-column canonical schema, or a directory of `*.parquet` parts) into a sorted format-2 dataset that
`serve` can open. `--output` must be new or empty; like the lister, the command takes the output's lock and
leaves `.swath/` with the lock file in it. Only `OBJECT` rows are written; a duplicate key, a versioned row, an
unknown `row_type` or a key over the cap (1,024 bytes; `--key-limit` raises it up to 4,096) is refused before
anything is written. On success it prints one line on stdout:

```
sort_fixture rows=10000 objects=10000 skipped=0 parts=1 bytes=230500 schema=swath-replay/objects/v1 wall_ms=40 output=/data/sorted
```

A refusal is one `sort-fixture: ...` line on stderr and exit 1; a bad command line is exit 2. The capture is
held in memory while it is sorted (about 300 bytes per row), so it suits captures of a few million rows.

## Flags

`serve` takes the Java launcher's flags, in either `--flag value` or `--flag=value` form, with or without the
`serve` command word.

| Flag | Default | Meaning |
|---|---|---|
| `--fixture PATH` | required | the sorted fixture (above) |
| `--bucket NAME` | required | the bucket name the S3 route answers for; any other is `NoSuchBucket` |
| `--host`, `--port` | `127.0.0.1`, `0` | bind address; port `0` binds a free port, reported in the startup line |
| `--metrics-port` | `-1` | the metrics port (below); negative disables it, `0` binds a free one |
| `--max-concurrent-requests` | `512` | requests served at once (at least 8); more wait for a slot. An injected delay holds its slot, so set this above the widest client fan-out or the queue shows up as latency. Writing a response does not hold a slot: up to twice this many responses can be outstanding, and past that a request is refused (`response_count_exhausted`). The serving listener holds at most four times this many connections; one more is closed as soon as it is accepted |
| `--parquet-connections` | `0` | concurrent fixture page decodes; `0` is `max(8, min(32, GOMAXPROCS))` |
| `--inject-latency SPEC` | off | target latency by request shape (below) |
| `--latency-jitter F` | `0` | jitter fraction in [0, 1), drawn from the request so a repeated request gets the same delay |
| `--latency-scale N` | `1` | divide every injected delay by N (compressed time); needs `--inject-latency` |
| `--response-buffer-budget` | 256 MiB | response memory held at once, charged before it is allocated; a request that would exceed it is refused (below) |
| `--max-response-bytes` | 64 MiB | the largest encoded response; a larger page is a 500 |
| `--stop-timeout` | `10s` | shutdown deadline |
| `--idle-timeout` | `30s` | idle connection timeout |
| `--write-timeout` | `30s` | response write timeout, counted from the first byte written (an injected delay does not count) |
| `--slow-request-log-ms` | `$SWATH_REPLAY_SLOW_REQUEST_LOG_MS`, else `-1` | log requests whose own cost reaches this many milliseconds; negative disables |
| `--serving-mode` | `sorted` | `sorted` only; `duckdb` is refused with the reason |
| `--protocols` | `s3` | any of `s3`, `gcs` and `azure`, comma-separated (below). With `gcs` the bucket must be a valid GCS bucket name, and `storage` is refused with both `s3` and `gcs`; with `azure` it must be a valid container name |
| `--azure-account` | `replay` | the Azure route's account path segment (3 to 24 lower-case letters and digits); refused without `azure` |
| `--advertised-host` | `--host` | the host the Azure `ServiceEndpoint` and `native_endpoints` name; required for a wildcard bind with `azure` |

The durations take `500ms`, `10s` or ISO-8601 (`PT10S`). A bad value is a usage error (exit 2) naming the
flag; a fixture that cannot be opened exits 1.

When both ports are bound the server prints one line to stderr, in the Java launcher's `key=value` shape:

```text
swath_replay endpoint=http://127.0.0.1:19090 bucket=noaa-normals-pds fixture=/data/... serving_mode=SORTED
  parquet_connections=8 inject_latency=off latency_scale=1.0 metrics_endpoint=http://127.0.0.1:19192/metrics
  max_concurrent_requests=512 protocols=s3 response_buffer_budget=268435456 max_response_bytes=67108864
  max_responses=1024 output_chunk_bytes=262144 fixture_identity=... ordering_profile=unsigned-utf8-byte-order
  metadata_policy=fixture-plus-synthetic profiles={s3=list-objects-v2} native_endpoints=s3:http://...?list-type=2
```

(one line on the terminal). SIGINT or SIGTERM stops it: new requests and requests waiting for a slot are answered
`503`, requests sleeping in an injected delay are answered `503` at once, the fixture is closed when the last
read returns, and the process exits 130 or 143. Everything, closing the fixture included, is bounded by
`--stop-timeout`: past it the server logs `shutdown_incomplete` and exits, leaving a read that ignores
cancellation to finish on its own. A failure to close the fixture is logged (`replay store cleanup failed`).

## The S3 route

`GET /{bucket}?list-type=2` with `prefix`, `delimiter`, `start-after`, `continuation-token`, `max-keys`,
`encoding-type=url` and `fetch-owner`. The bucket is the first segment of the path as sent (later segments are
ignored; an escaped spelling is another bucket). Keys are bytes in unsigned byte order.

- `max-keys` is echoed as asked; a page holds at most 1,000 entries, objects and common prefixes together.
  Missing or blank is 1,000; negative or not an integer is `400 InvalidArgument`. An integer is an optional sign
  and decimal digits from any script of the Basic Multilingual Plane (`١٢` is 12); a digit outside it (such as
  U+1D7D9) is not an integer.
- A `continuation-token` takes precedence over `start-after`: the page resumes at the token and the response
  omits the ignored `StartAfter`. A blank token is absent. A malformed token is `400 InvalidArgument`
  even when a `start-after` could have served. Replay tokens (`v2:...`) and S3's are not interchangeable.
- `encoding-type=url` percent-encodes every byte but `A-Z a-z 0-9 - _ . /` with upper-case hex, as S3 does.
  Without it, key bytes are rendered as UTF-8 text with malformed sequences replaced.
- `LastModified` has millisecond precision, with the year of era (`0001` for year 0, `+10000` past year 9999);
  `Owner` appears only with `fetch-owner=true`.
- Errors are S3's XML error document with `Code`, `Message`, `Resource` (the path as sent) and `RequestId`:
  `404 NoSuchBucket`, `405 MethodNotAllowed` (anything but GET), `400 InvalidArgument`. Every answer carries
  `x-amz-request-id` and `x-amz-id-2` (`S3LISTINGREPLAY`).
- The replay's own refusals carry `x-swath-replay-error` with the reason: `503 ServiceUnavailable` for
  `shutdown`, `response_count_exhausted` and `response_budget_exhausted`, `500 InternalError` for
  `response_too_large`; `500 InternalError` with `fixture_disordered` when a row group's keys turn out not to
  ascend (the message names the row group and the part's file name, never its directory); and
  `500 InternalError` (`internal`, no detail) when the fixture read fails otherwise.
- A request head (request line, headers, blank line) may be 8 KiB with the S3 route alone and 64 KiB with a
  native route enabled, the Java server's limits: a request line longer than that is `414 URI Too Long`, a
  longer head `431 Request Header Fields Too Large`, both with the HTML refusal page below.
- The listing routes take no request body. A request that declares one (`Content-Length` or chunked) is answered
  as it would be without it, and its connection is then closed instead of drained, as on the Java server; a
  body that never arrives holds nothing.
- Some request targets are refused before the route, with `400`, an HTML page and
  `Cache-Control: must-revalidate,no-cache,no-store`, as the Java server's HTTP layer refuses them: a fragment
  (`#`), an empty path segment other than the last (`//bucket`, `/;/bucket`), an encoded `/` or `%`, an encoded
  dot segment (`%2e%2e`) or a dot segment with a parameter (`..;`), a raw `\` or a raw byte outside the path
  characters (`"`, `<`, `|`, non-ASCII, ...), an encoded control character or `\`, invalid UTF-8 once decoded,
  a malformed escape or an encoded NUL, a path that climbs above the root (`/../bucket`), and user information
  in an absolute target. The page is this server's own.

Response memory (the same on every route). A request reserves room for a full page (on S3 320 bytes an entry,
at least 4 KiB; on GCS `1024 + 384` an entry; on Azure `2048 + 512` an entry; at most `--max-response-bytes`)
before it reads the fixture, and is refused (`response_budget_exhausted`) if the budget
has no room for that. The response is then rendered into chunks sized from the page it actually got, each
charged before it is allocated; rendering stops at the first byte past `--max-response-bytes`
(`response_too_large`) or past the budget. The unused reservation goes back before any injected delay, and the
chunks are held until the response is written.

## The Azure route

```bash
./swath-replay serve --fixture /data/sorted --bucket replay-fixture --protocols s3,azure --azure-account replay \
  --host 127.0.0.1 --port 19090 &
curl -H 'x-ms-version: 2026-06-06' 'http://127.0.0.1:19090/replay/replay-fixture?restype=container&comp=list&maxresults=2'
```

`--protocols s3,azure` serves Azure Blob's flat `List Blobs` beside S3 from the same fixture, store, admission
and meters: `GET /{account}/{container}?restype=container&comp=list`, where `--bucket` is the container (it must
be a valid container name) and `--azure-account` the account. A request whose path is `/{account}/{one segment}`
and whose query has `restype` or `comp` belongs to this route before its method is checked; one that also has
`list-type` is a replay `400 {"error":"replay_ambiguous_request"}`; every other request stays on the S3 route,
or, with S3 off, is a replay `404 {"error":"replay_not_found"}`.

- `x-ms-version` must be `2026-06-06` or `2026-10-06` and is echoed; anything else is `400 InvalidHeaderValue`.
  `x-ms-client-request-id` (at most 1,024 bytes, no CR or LF) is echoed. `Accept` must allow `application/xml`.
- Parameters: `prefix`, `delimiter` (any string), `marker`, `maxresults` (1 to 2,147,483,647 ASCII digits; a page
  holds at most 5,000, the default; the value is echoed as sent), `startFrom` (inclusive), `timeout` (1 to
  3,600), and an empty `include`. A repeated parameter, an unknown one, `endBefore`, a non-empty `include`, a
  malformed `%` escape, a value that decodes to invalid UTF-8, a field over 4,096 bytes (8,192 for `marker`),
  and a `prefix` or `delimiter` holding a control character other than TAB and LF are refused with the code the
  matrix in [`../../docs/replay-provider-conformance.md`](../../docs/replay-provider-conformance.md) records.
  `+` is a literal plus. Raw non-ASCII bytes in the query are read as UTF-8.
- Names are listed in UTF-16 code-unit order, as live Azure lists them; the fixture stays byte-sorted
  ([internals/replay.md](internals/replay.md#the-azure-route)). With a delimiter, `Blob` and `BlobPrefix` entries
  interleave in that order, and a `startFrom` inside a rolled prefix keeps the prefix only when one of its names
  is at or after `startFrom`.
- `NextMarker` is `az1.` and base64url of the resume point, bound to the listing scope (account, container,
  version, prefix, delimiter, `startFrom`, fixture identity): a marker used with another scope is
  `400 InvalidQueryParameterValue`. Markers are the Java server's bytes for the same fixture identity.
- Every body, errors included, starts with a UTF-8 byte order mark before the XML declaration. `Blob` carries
  `Name` and synthetic `Properties`: `Last-Modified` (second precision), `Etag` `0x000000000000001`,
  `Content-Length`, `Content-Type` `application/octet-stream`, `BlockBlob`, `Hot` (inferred), `unlocked`,
  `available`. A name holding U+FFFE or U+FFFF is sent `Encoded="true"`, percent-encoded. A fixture row the
  profile cannot render (a control character in a blob name, invalid UTF-8, over 1,024 UTF-16 units, a negative
  size, a time outside years 1 to 9999) is `500 InternalError` with the reason in `x-swath-replay-error`.
- Errors are `<Error><Code>X</Code><Message>X</Message></Error>` with `x-ms-error-code`; `HEAD` is refused
  (`400`, no body) and other methods are `405 UnsupportedHttpVerb` with `Allow: GET`. Overload is
  `503 ServerBusy`. Answers carry `x-ms-request-id` (`replay-` and a counter) and `Date`.
- The injected latency profile applies alike: a delimiter request is a structure probe, `maxresults=1` a pivot
  probe.

Unlike the Java server, a name of more than 254 `/`-separated segments is listed: native Azure lists one (the
evidence archive's capture `azure-12a`), where the Java server answers `500 too_many_segments`.

## Injected latency

```bash
swath-replay serve ... --inject-latency 'worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp' \
  --latency-jitter 0.15
swath-replay serve ... --inject-latency prod-commoncrawl --latency-scale 50
```

A request with a delimiter is a `structure_probe`, a delimiter-free request with `max-keys` at most 1 a
`pivot_probe`, anything else a `worker_page` (aliases `structure`, `pivot`, `page`). A delay is `223ms`, `6.9s`
or a bare millisecond count; `+55ms/cp` (structure probes only) adds a term per common prefix returned. An
unlisted shape gets none. The number before `ms` or `s` is read as a floating-point number in the Java
grammar (`1.5e3ms`, `2dms`, `0x1p0s`); a delay too long to represent saturates at about 292 years rather than
wrapping to none. `prod-commoncrawl` is the built-in reference profile (223 ms pages, 121 ms pivots,
223 ms + 55 ms per prefix for structure probes).

The delay is the request's total target time, not a surcharge: the server sleeps only for what its own cost
left, so the client sees `max(cost, profile)`. A request whose own cost exceeds its profile is counted in
`swath.replay.inject.overrun{shape}`, its excess in `swath.replay.inject.overrun.ms{shape}`; a run with many
overruns measured the server, not the profile. `--latency-scale` divides every term, which keeps the profile's
shape but not absolute time (server and client CPU do not scale).

## The metrics port

`--metrics-port` serves three paths on a second listener, which never waits behind the request ceiling, never
reads the fixture and never moves a listing meter. Its connections are bounded by time instead: an idle
keep-alive connection, reading a request and reading its body take `--idle-timeout`, writing an answer
`--write-timeout`, the serving listener's values:

- `GET /healthz`: `ok` once the server is accepting. Poll it before starting a client.
- `GET /metrics`: JSON, `{schema_version: 2, serving_mode, serving: {...}, uptime_ms, sampled_at_epoch_ms,
  meters: [...]}`. `serving` holds the enabled protocols, the bucket and Azure account, the fixture identity, the
  ordering and metadata profiles, the request and response bounds (`max_concurrent_requests`, `max_responses`,
  `response_buffer_budget`, `max_response_bytes`, `output_chunk_bytes`), the charged and peak response bytes,
  the active responses, the three timeouts, whether latency is injected and the route profiles. Each meter has
  `name`, `type` and `tags`; counters a `count`, timers `count`, `sum_ms`, `mean_ms`, `max_ms`, `p50_ms`,
  `p99_ms`, distributions `count`, `total`, `mean`, `max`, gauges a `value`. Meters are in a fixed order (by
  name, then tags) so two scrapes diff cleanly; `uptime_ms` bounds an interval on the server's clock.
- `GET /runtime-attestation`: `runtime-attestation-v1`, the cgroup v2 `cpuset.cpus.effective`, `memory.max` and
  `memory.swap.max` of the process's own cgroup and `/proc/self/status` `Cpus_allowed_list`, each a
  `{value, error}` pair: a missing, unreadable or invalid source is an explicit error.

A timer's `count` and `sum_ms` cover the server's life; its `max_ms`, `p50_ms` and `p99_ms` cover recent
samples, the window a Java server's timers publish: every sample goes into three histograms, the oldest is read,
and every 40 seconds the oldest is cleared, so a scrape covers the last 80 to 120 seconds. The histograms are
log-linear (32 buckets per power of two), so a percentile is within 3.1% above the sample it stands for and never
above `max_ms`. Read a measured window's mean as the difference of two scrapes' `sum_ms` over the difference
of their `count`. `index.load.latency` and `sortfixture.build.latency` publish no percentiles (`null`), as on
the Java server.

### The meters

All names begin `swath.replay.`; the Java server's are in [`../../docs/swath-replay.md`](../../docs/swath-replay.md#metrics-and-tuning).
A scrape after the same requests lists the same meters with the same types and tags on either server, apart from
the rows marked absent or renamed below (`server` `TestMeterSetMatchesTheContract` compares the set with a Java
scrape, `testdata/meters.txt`). Meters marked "from startup" are there, at zero, before the first request; the
others appear on first use.

| Meter | Go server | Notes |
|---|---|---|
| `http.requests`, `http.errors`, `http.request.latency` | present, from startup | every answer on the serving port; `errors` the 5xx. The latency runs from arrival to the start of the write. The bench scripts sum `http.requests` `count` |
| `request.latency{shape}` | present, from startup | the S3 route's own cost per shape (`worker_page`, `pivot_probe`, `structure_probe`), injected delay excluded |
| `request.stage.latency{protocol,stage}` | present | `page`, `render`, `delay`, `write` of a page; an error answer has no write stage |
| `fixture.list.latency`, `serving.path{mode}` | present, from startup | one S3 pager operation |
| `serving.fallback{reason}` | absent | there is no other serving mode to fall back to: a fixture sorted serving refuses ends startup with the reason (the Java server's `--serving-mode sorted` does the same and records nothing) |
| `serving.refused{reason}` | present | `row_group_disorder`, counted for every read that finds it, a range read or a rollup (the Java server counts the rollup's only) |
| `index.load.latency{source=derived}`, `index.entries` | present, from startup | the fixture open (footers, routing keys); one routing entry per non-empty row group |
| `page.read.latency` | present, from startup | how long a store read holds its reader slot, slot wait excluded |
| `parquet.query.latency`, `parquet.query.rows`, `parquet.query.errors` | present, from startup | one store call, slot wait included; its rows or rollup entries. The Java server's rows include its prefetch window fills, so its totals are larger |
| `parquet.queries.in_flight`, `parquet.queries.peak` | present, from startup | reads holding one of the `--parquet-connections` slots, now and at most. One pool serves every read, so the peak is at most the flag; the Java server has a pool per file and per read kind, so its peak can exceed the flag |
| `delimiter.path{path}` | present | `rollup` or `walk` for an S3 delimiter page |
| `delimiter.skipscan.row_group_opens`, `.whole_group_shortcuts`, `.decoded_key_rows`, `.page_reseeks` | present, from startup | the `/` skip-scan's key cursors opened, groups emitted from routing keys alone, and per rollup the key values decoded and the page-index reseeks. A key cursor stays open on its group for the whole rollup and decodes whole key pages, so `row_group_opens` is lower and `decoded_key_rows` higher than the Java server's for the same rollup |
| `delimiter.reader_pool.open.latency`, `delimiter.reader_pool.readers_opened` | renamed: `handle_cache.*` | the Java server opens a row-group reader pool per file on its first rollup; the Go reader has no reader pools. Every part is read through one descriptor cache instead: `handle_cache.opens` (first opens and reopens after eviction), `.evictions`, `.waits` (reads that waited for a descriptor) as counters, and `.open`, `.peak`, `.limit` (half the soft descriptor limit, at most 4,096) as gauges, from startup |
| `sortfixture.build.latency`, `sortfixture.output.bytes` | present, from startup, zero | recorded by `sort-fixture`, never by a server |
| `sort.progress`, `sort.steal_reason{outcome,reason}` | absent | the Java sorter's own scheduling; `sort-fixture` here sorts in memory |
| `prefetch.*` (the window cache's fills, hits, misses, ramp, live windows, rows and anchors, selection, joins, evictions) | absent | the reader has no window cache: every page is decoded from the fixture |
| `response.admission.refused{protocol,reason}`, `response.bytes.live`, `response.bytes.peak`, `response.active` | present (gauges from startup) | refusals by `shutdown`, `response_count_exhausted`, `response_budget_exhausted`, `response_too_large` |
| `response.chunk.allocation{reason}`, `response.chunk.capacity.bytes{reason}` | present, from startup | chunks by `initial`, `initial_cap_partial`, `continuation`, `cap_partial` |
| `response.percent.encoding.path{protocol,reason}`, `response.write.deadline{protocol,reason}` | present | |
| `protocol.requests.active{protocol}` | present, from startup | per enabled route, until the answer is written |
| `protocol.http.requests{protocol,status_class}`, `protocol.objects`, `protocol.prefixes`, `protocol.encoded.bytes{protocol,shape}`, `provider.path{protocol,path,reason}` | present | |
| `inject.overrun{shape}`, `inject.overrun.ms{shape}` | present | absent while nothing overran; zero is the healthy state |
| `jvm.*`, `process.*`, `system.*` | absent | the Java runtime's and host meters; read the process from `/proc` and the limits from `/runtime-attestation` |

### Diagnostics

- **Slow requests.** `--slow-request-log-ms N` (or `SWATH_REPLAY_SLOW_REQUEST_LOG_MS`; the Java server's
  `swath.replay.slow-request-log-ms` system property) logs every S3 page whose own cost reaches N ms, with the
  Java server's message and fields, without its logger's time and level prefix (`TestSlowRequestLogLine`):

  ```text
  replay slow request shape=STRUCTURE_PROBE ms=0.690114 prefix=logs/ delimiter=/ start_after=logs/a continuation=none max_keys=7 entries=4 truncated=false
  ```

- **The startup line** has the Java launcher's keys in its order (`TestServeEndToEnd`) and the same values for the
  same flags and fixture; `fixture_identity` is also in the payload's `serving` object.
- **Refusals** are typed: `serving.refused{reason}` for the fixture, `response.admission.refused{protocol,reason}`
  for admission, and the `x-swath-replay-error` header on the answer.
- **Not here:** the Java `bench` command (an in-process token-walk benchmark; `bench/replayload` drives a running
  server instead, below), the DuckDB serving mode, and the Java server's
  `swath_replay_metrics` summary line at shutdown (logged below the Java launcher's default log level; poll the
  endpoint instead).
- A client that disconnects while its page is being read does not cancel the read: the page is read, rendered and
  counted as served (`2xx`, nothing in `http.errors`, no log line), and only its write fails, as on the Java
  server (`TestClientGoneFinishesThePage`). Shutdown still interrupts it. A client that leaves while its request
  sleeps in an injected delay is not noticed either: the delay runs out and the write fails without a log line
  (`bench/resume_storm.sh`, which kills a listing with 64 requests in flight, leaves the server log clean).

## With the bench scripts

`bench/fault_replay.sh`, `bench/policy_replay.sh` and `bench/defaults_replay.sh` start `swath-replay` from
`PATH` after sourcing `$REPLAY_ENV`. To run them on the Go server, point `REPLAY_ENV` at a file that puts it
first:

```bash
cat > /tmp/go-replay.env <<EOF2
export PATH=$PWD:\$PATH            # the directory holding the Go swath-replay
EOF2
REPLAY_ENV=/tmp/go-replay.env RECORDINGS=/data/rec FAULTPROXY=/tmp/faultproxy \
  bench/fault_replay.sh arms.tsv noaa-normals-pds 95 19090 1 /tmp/out
```

The scripts' flags (`--serving-mode sorted --parquet-connections 8 --max-concurrent-requests 1024
--inject-latency ...`) and their reading of `/healthz` and `swath.replay.http.requests` are the same on
either server.

`bench/replayload` drives a running server (either one) with concurrent S3 token walks and prints one line: walks,
seconds, requests, objects, rows per second, the request latency's p50 and p99, retried `503`s, errors, and with
`-metrics` the server's request count, `inject.overrun` total and `parquet.queries.peak`. With `-fixture` each
walk starts at its own row group's routing key; a walk that reaches the end starts over:

```bash
go build -o /tmp/replayload ./bench/replayload
/tmp/replayload -endpoint http://127.0.0.1:19090 -bucket synth -fixture /data/synthetic-1m/sorted-rolled \
  -walks 64 -max-keys 1000 -warmup 10s -duration 50s -metrics http://127.0.0.1:19192/metrics
```

The server's load tests (`TestLoadRequestCeilingUnderFaultProxy`, `TestLoadReadersBoundPageDecodes`,
`TestLoadNoOverrunAtSixtyFourWalks`) run with `SWATH_REPLAY_LOAD=1`, a few seconds each; with
`SWATH_REPLAY_FIXTURES=ROOT` they serve `ROOT/fixtures/synthetic-1m/sorted-rolled`, otherwise a generated fixture of
the same key shape.

## Comparing with the Java server

`bench/replaydiff` starts both servers on one fixture and compares every answer byte for byte (see
[internals/replay.md](internals/replay.md#the-differential-driver)):

```bash
go build -o /tmp/replaydiff ./bench/replaydiff
/tmp/replaydiff -java $JAVA_ROOT/swath-replay/build/install/swath-replay/bin/swath-replay -go ./swath-replay \
  -fixture /data/sorted -bucket bkt -report /tmp/diff.json     # exit 1 and FAIL lines on a difference
/tmp/replaydiff -protocol azure -walk-limit 300 ...                   # the Azure route, both servers on s3,azure
```

## Measurements

Configuration, unless a table says otherwise. Host: a 4 vCPU session container (Intel Xeon @ 2.10 GHz) shared
with other jobs, 15 GiB RAM, Linux 6.18; **4 vCPU shared host, not the corridor's eight isolated cores**, and the
client runs on the same four vCPUs over loopback, so these numbers are the pair's, not the server's alone. Go
server: this module at the commit that added this section, `./build.sh`, GOMAXPROCS 4. Java server: swath `main`
at `01dd55b` (0.3.3-SNAPSHOT) on Temurin 25.0.5+8 with the launcher's heap defaults and `JAVA_TOOL_OPTIONS` as the
host sets it (trust store and proxy properties only), as `bench/fault_replay.sh` leaves it. One server at a time,
both with `--serving-mode sorted --parquet-connections 8 --max-concurrent-requests 1024`. Fixtures, page cache
warm: `synthetic-1m/sorted-rolled` (synthetic, 1,000,000 keys, 6 parts, 110 row groups, 22.7 MB, written by the
Java `sort-fixture`) and the `noaa-gk2a-pds` recording (a public bucket's listing, 18,472,015 keys, 2 parts, 55
row groups, 461 MB; not in the repository). Measured on 2026-11-07; runs vary by about 15% from one to the next
on this host.

### Throughput and memory

Each run: `swath-replay serve --fixture F --bucket B --port P --metrics-port M --serving-mode sorted
--parquet-connections 8 --max-concurrent-requests 1024`, then `replayload -endpoint http://127.0.0.1:P -bucket B
-fixture F -walks W -max-keys 1000 -warmup 10s -duration D -metrics http://127.0.0.1:M/metrics` (D 30 s for 16
walks, 50 s for 64), with the server's `VmRSS` read from `/proc/PID/status` every second from the client's start:
the 64-walk rows are one minute of samples. RSS is the measured window's mean and maximum.

| Server | Fixture | Walks | Object rows/s | Request p50 / p99 ms | RSS mean / max MB | Reader peak |
|---|---|---|---|---|---|---|
| Go | synthetic-1m rolled | 16 | 581,534 | 23.4 / 78.0 | 39 / 45 | 8 |
| Go | synthetic-1m rolled | 64 | 667,485 | 92.5 / 153.7 | 45 / 50 | 8 |
| Java | synthetic-1m rolled | 16 | 1,466,799 | 8.9 / 27.0 | 1,217 / 1,219 | 16 |
| Java | synthetic-1m rolled | 64 | 1,918,334 | 28.8 / 91.6 | 1,600 / 1,601 | 46 |
| Go | noaa-gk2a-pds | 16 | 636,777 | 21.4 / 65.1 | 64 / 72 | 8 |
| Go | noaa-gk2a-pds | 64 | 708,869 | 85.5 / 165.3 | 79 / 84 | 8 |
| Java | noaa-gk2a-pds | 16 | 758,565 | 15.8 / 84.8 | 1,937 / 2,206 | 10 |
| Java | noaa-gk2a-pds | 64 | 1,062,748 | 49.2 / 184.0 | 2,118 / 2,474 | 16 |

Idle, the Go server holds 13 MB and answers `/healthz` 0.22 s after it starts on either fixture; the Java server
holds about 180 MB and answers after 2.2 s. The Go server's memory does not follow the fixture's object count
(18.5 million keys cost 30 MB more than one million); it is the in-flight pages and responses
(`response.bytes.peak` 20.5 MB at 64 walks, 82 MB at 256). The Java server serves 1.2 to 2.9 times as many rows a
second here: its prefetch window cache decodes a run of rows once for the pages a walk reads next, where the Go
reader decodes every page's rows from the fixture (`page.read.latency`, the reader slot's hold time with 8 slots
sharing 4 vCPUs, averaged 9.8 ms a 1,000-key page at 64 walks).
With 256 walks the Go server served 708,891 rows/s with `parquet.queries.peak` 8, page reads 8.5 ms and the reader
slot wait in `parquet.query.latency` (347 ms).

### Overruns at 64 walks

`--inject-latency worker_page=50ms,pivot_probe=50ms,structure_probe=50ms`, 64 walks, 30 s, synthetic-1m rolled,
`replayload ... -walks 64 -max-keys K -duration 30s`. 64 walks at 50 ms ask for 1,280 pages a second.

| Server | max-keys | Object rows/s | Request p50 / p99 ms | Requests | `inject.overrun` |
|---|---|---|---|---|---|
| Go | 1000 | 672,848 | 91.8 / 150.9 | 20,252 | 20,057 |
| Go | 100 | 123,950 | 51.1 / 58.0 | 37,194 | 0 |
| Java | 1000 | 1,011,384 | 54.6 / 163.9 | 30,382 | 312 |

At 1,000-key pages the profile asks for 1.28 million rows a second, about twice what the Go server serves on this
host with the client beside it, so nearly every page overruns: such a run measures the server, not the profile.
At 100-key pages (128,000 rows a second) it holds the profile with no overrun (`TestLoadNoOverrunAtSixtyFourWalks`
pins this). Check `inject.overrun` against the request count before trusting a run with a profile.

### Request ceiling, reader slots and descriptors

From the load tests (`SWATH_REPLAY_LOAD=1 SWATH_REPLAY_FIXTURES=ROOT go test -run TestLoad -v
./internal/replay/server/`) and `TestServeWithinADescriptorLimit`:

- **Request ceiling under the fault proxy.** 32 walks of 20,000 keys through `faulttest` (a throttle window,
  0.5 s to 2 s, on 30% of requests; 20 ms added to half) against `--max-concurrent-requests 8` and a 50 ms profile:
  4.1 s; the proxy saw 745 requests (103 throttled, 289 delayed, 640 forwarded) and held 32 at once; the server held
  at most 8 execution slots (9 requests active counting those writing, bound 16) and answered exactly the 640
  forwarded. With 16 clients stalled on their reads (twice the ceiling) a request through the proxy was answered
  `503 response_count_exhausted` and counted once; when they left, walks completed again.
- **Reader slots.** 256 walks of 8 pages over synthetic-1m rolled with 8 slots: 2,044,568 rows in 2.7 s, in-flight
  reads sampled every millisecond at most 8, `parquet.queries.peak` 8. Under the race detector (2 pages a walk)
  the same bound holds. The 256-walk binary run above also peaked at 8.
- **Descriptors.** A 120-part fixture served from a process whose soft descriptor limit is 64, to 12 concurrent
  full walks of 250-key pages: every key listed, at most 53 descriptors open, `handle_cache.peak` 32 of 32, 360
  opens and 328 evictions, no errors.

### The bench scripts

`bench/fault_replay.sh ARMS noaa-normals-pds LAT 29300 1 OUT`, REPS 1, the Go `swath diag list` with 64 workers,
two arms: `baseline` (no proxy fault) and `storm` (`-window throttle:5s-15s:0.5`). Wall seconds and the proxy's
requests (throttled):

| Injected latency | Server | baseline | storm |
|---|---|---|---|
| 95 ms | Go | 1.38 s, 251 | 1.34 s, 249 (0) |
| 95 ms | Java | 1.98 s, 253 | 1.40 s, 251 (0) |
| 1,000 ms | Go | 11.26 s, 248 | 35.30 s, 266 (15) |
| 1,000 ms | Java | 11.38 s, 244 | 35.37 s, 268 (15) |

At 95 ms the listing ends before the throttle window opens at 5 s; 1,000 ms puts the window inside the run.
`KILL_AT=5 PHASE=25 bench/resume_storm.sh noaa-normals-pds 1000 29300 1 OUT 64` (the list SIGKILLed at 5 s,
resumed at once through a proxy throttling 20% of requests until 25 s), list then resume seconds, requests
(throttled):

| Server | shipped | slowstart | off |
|---|---|---|---|
| Go | 5.1 + 45.4 s, 301 (19) | 5.1 + 45.3 s, 271 (10) | 5.1 + 11.6 s, 398 (80) |
| Java | 5.1 + 45.4 s, 303 (19) | 5.1 + 46.3 s, 274 (10) | 5.1 + 11.6 s, 399 (80) |

Every run exited 0. The scripts behave the same on either server; nothing in the Go server needed a change for them.

## Limits

- Three routes (S3 `ListObjectsV2`, path-style, the GCS JSON `objects.list` and the Azure flat `List Blobs`
  subset below) and one serving mode (sorted). Version listing, object data and
  authentication are not served.
- Three request-target cases are not byte-identical with the Java server, by decision; the differential driver
  compares them on the status and the headers the Java server sets, and says which in each case's name:
  - The error page of a target refused before routing (above). Status `400`, `Content-Type:
    text/html;charset=iso-8859-1` and `Cache-Control: must-revalidate,no-cache,no-store` match; the HTML body is
    this server's own page naming the problem, not a copy of the Java server's HTTP layer's page.
  - A request line Go's HTTP parser refuses before any handler runs: whitespace or a control byte in the target,
    a malformed `%` escape (`%zz`, a lone `%`, `%uXXXX`) in the path, a target with neither a leading `/` nor a
    scheme, and a request head over about 132 KiB (`431` from both servers). Both servers answer `400`; this one with Go's `text/plain; charset=utf-8` page and no
    `Cache-Control`.
  - An absolute target whose authority differs from the `Host` header (`GET http://other.invalid/bucket?...`
    with `Host: localhost`). The Java server refuses it with `400`; this server serves it, because Go's HTTP
    layer drops the `Host` header for an absolute target, as RFC 9112 section 3.2.2 has an origin server
    ignore it, so the disagreement cannot be seen. The driver does not send this case.
- Timer percentiles are histogram estimates on both servers (within 3.1% here; the Java registry's default
  precision is coarser), so the two servers' `p50_ms` and `p99_ms` agree in window, not in digits.
- One process does not reproduce distributed service capacity or network faults; put `bench/faultproxy` in
  front for those.

## The GCS route

With `--protocols gcs` (alone or with `s3`) the server answers the Cloud Storage JSON API's `objects.list`:
`GET /storage/v1/b/{bucket}/o`. Point a GCS client at it with its endpoint override (for example
`http://127.0.0.1:19090`) and anonymous credentials.

```bash
./swath-replay serve --fixture /data/rec/noaa-normals-pds/dataset/data --bucket noaa-normals-pds --protocols s3,gcs &
curl 'http://127.0.0.1:19090/storage/v1/b/noaa-normals-pds/o?prefix=normals-hourly%2F&delimiter=%2F&maxResults=5'
```

With several routes a request is classified before anything else: a path under `/storage/v1/b/` belongs to the
GCS route (with an S3 `list-type` parameter it is ambiguous: `400 {"error":"replay_ambiguous_request"}`); any
other request is the S3 route's, or, when S3 is not enabled, `404 {"error":"replay_not_found"}`. Any other
path under `/storage/v1/b/` (another bucket, an escaped spelling, a trailing slash or segment) is `404`
`notFound`.

| Parameter | Served |
|---|---|
| `prefix`, `delimiter` | any UTF-8 text up to 1,024 bytes; a delimiter of any length; empty is absent |
| `startOffset` | inclusive lower bound; a prefix rolled up from a name at or after it is listed even when the offset falls inside that prefix |
| `endOffset` | exclusive upper bound |
| `maxResults` | 1 to 1,000 entries (objects and prefixes together) per page; larger asks are capped at 1,000; absent is 1,000; `0`, a sign, a leading zero or a non-integer is `invalid_max_results` |
| `pageToken` | the replay's own `gcs1.` token, bound to the fixture and the request's prefix, delimiter and offsets (not to `maxResults`); anything else is `invalid_page_token` |
| `projection` | `noAcl` (default) or `full` (each item carries an empty `acl`) |
| `prettyPrint` | `true` or `false`, accepted and ignored: the body is always compact |
| `alt` | `json` only (`alt_format` otherwise) |
| `versions`, `softDeleted`, `includeFoldersAsPrefixes`, `includeTrailingDelimiter` | `false` only (`feature_NAME` otherwise) |
| `matchGlob`, `filter`, `fields` | empty only (`feature_NAME` otherwise) |
| anything else, or a repeated parameter | `unknown_or_duplicate_parameter` |

A query is read as UTF-8 (a raw ill-formed sequence becomes U+FFFD); `%XX` escapes must be complete and decode
to valid UTF-8 (`invalid_percent_escape`, `invalid_utf8`); a raw `+` stays `+`. A value over 1,024 bytes
(2,048 for `pageToken`) is `field_too_long_NAME`. A request with several faults reports the first in this
order: unknown or repeated names and bad escapes (left to right), the four boolean features (`versions`,
`softDeleted`, `includeFoldersAsPrefixes`, `includeTrailingDelimiter`), `matchGlob`, `filter`, `fields`,
`alt`, `prettyPrint`, `projection`, `maxResults`, the field lengths, then the token. The Java server checks the
two feature groups in the same order, but within a group it takes its set's iteration order, which is
randomized each time the JVM starts: with two refused features of one group (say `versions=true&softDeleted=true`
or `fields=x&filter=x`) its `x-swath-replay-error` names either, while this server's order is fixed as listed.
The status and the envelope are the same either way.

The answer is `{"kind":"storage#objects"}` plus `items` and `prefixes` when non-empty and `nextPageToken` when
there is more. Errors are the JSON API envelope `{"error":{"code":N,"message":...,"errors":[{"message":...,
"domain":"global","reason":...}]}}` with the replay's reason in `x-swath-replay-error`: `400 invalid` for a
refused request, `404 notFound`, `405 methodNotAllowed` (with `Allow: GET`; anything but `GET`, `HEAD` included),
`503 backendError` for `shutdown`, `response_count_exhausted` and `response_budget_exhausted`, and
`500 internalError` for `response_too_large`, a fixture row the API cannot represent (`name_length`,
`invalid_utf8_name`, `name_newline`, `reserved_name`, `negative_size`, `timestamp_out_of_range`),
`fixture_disordered` and `internal`. Every answer is `Content-Type: application/json; charset=utf-8`.

The route shares the request ceiling, the response count and byte budget, the injected latency (a delimiter
request is a structure probe, `maxResults=1` a pivot probe, anything else a worker page, with the jitter keyed
by the request's prefix, delimiter, offsets, page size and token) and shutdown with the S3 route, and counts
under the same meters tagged `protocol=gcs`, plus `swath.replay.provider.path{protocol,path,reason}` (the path
each page took: `flat`, `delimiter_indexed`, `delimiter_walk`, `empty_intersection`, `page_limit_clamped`,
`timestamp_render`, and the refusals).

### Item metadata by fixture schema

Each item carries `kind`, `bucket`, `name`, `size` (a decimal string), `updated` (RFC 3339 UTC, six fractional
digits: the fixture's microseconds exactly), then the metadata fields. Which values they hold depends on the
fixture's declared schema id:

| Field | `swath/gcs-objects/v1` (the lister's GCS datasets) | every other schema (S3, canonical, sort-fixture output, local) |
|---|---|---|
| `name`, `size`, `updated` | exact | exact (a null size is `0`, a null time the epoch) |
| `etag` | exact (`swath-gcs-synthetic-v1` when null) | synthetic: `swath-gcs-synthetic-v1` (an S3 ETag is not a GCS etag) |
| `generation`, `metageneration` | exact (`1` when null) | synthetic: `1` |
| `storageClass` | exact (`STANDARD` when null) | synthetic: `STANDARD` |
| `contentType` | exact (`application/octet-stream` when null) | synthetic: `application/octet-stream` |
| `md5Hash`, `crc32c` | exact, absent when null | absent |
| `acl` | `[]` with `projection=full` | `[]` with `projection=full` |
| `id`, `selfLink`, `mediaLink`, `timeCreated`, `owner`, `metadata` and the rest | absent (not stored) | absent |

The synthetic values are the wire contract's, so the Java server and this one answer a non-GCS fixture
identically; the Java server does not read `swath/gcs-objects/v1` datasets.

### The GCS evidence replay

`go/internal/replay/evidence` replays the provider evidence archive (`docs/replay-provider-evidence.tar.gz`)
against this server. With `SWATH_REPLAY_FIXTURES=ROOT` it extracts the archive into a temporary directory,
serves the capture corpus (from `ROOT/fixtures/evidence-gcs/sorted` when present, and always from the same
corpus written in the test and sorted by sort-fixture) under the bucket `replay-bucket`, and replays every
`gcs/captures/*.json`: an exchange as captured with the bucket placeholder mapped and a native page token
replaced by the replay's token for the same position (or its tampered, flipped or truncated form), and a walk
re-walked with the replay's own tokens. An SDK run is executed by an official client library, the Go one
(`google.golang.org/api/storage/v1`; the captures were made with the Java one): pointed at the server with its
endpoint override, anonymous, auto-paginating, one attempt per call. Its requests must carry the captured
scope as decoded parameters (its own `alt=json` and `prettyPrint=false` aside) and identify the Go client,
its answers are compared as a walk, and its own decoded inventory must equal the provider's. Each answer is compared with the provider's under the conformance field policy: status, classified
headers, then the page (items field by field, prefixes, terminal pagination; walks as merged inventories with
unbroken token chains) or the error envelope's first reason. Each outcome must be its pin in
`gcs/replay-expectations.json` (with `docs/replay-provider-expectations.json` applied) or better: a pinned
mismatch must be reproduced as the whole message (only a replay token's body after a pin ending at the token's
prefix is open), and a pinned mismatch that now matches is logged as an improvement. `SWATH_PROVIDER_REPORT=DIR` writes the per-capture
outcomes to `DIR/<fixture>/gcs-outcomes.json`, in the shape of the Java suite's `-PproviderReport` file.

```bash
SWATH_REPLAY_FIXTURES=/data/replay SWATH_PROVIDER_REPORT=/tmp/report go test -v ./internal/replay/evidence
```

Today 77 of the 110 GCS captures match, the 77 pinned `match`; the other 33 meet their pinned mismatches.

### Comparing the GCS route with the Java server

```bash
/tmp/replaydiff -protocol gcs -java $JAVA_ROOT/swath-replay/build/install/swath-replay/bin/swath-replay \
  -go ./swath-replay -fixture /data/sorted -bucket bkt -report /tmp/gcs-diff.json
```

starts both servers with `--protocols gcs` and compares the GCS request matrix, token walks and a seeded random
walk ([internals/replay.md](internals/replay.md#the-gcs-route)).
## Container image

`Dockerfile.replay-go` (at the repository root) builds an image that holds the static binary and nothing else:
`/opt/swath-replay/swath-replay` is the entrypoint, the process runs as `10001:10001`, and the runtime stage is
`scratch`, so there is no shell, no package manager and no OS package in it. Fixtures are never baked in: mount
one read-only and pass its container path to `--fixture`.

```bash
docker build -f Dockerfile.replay-go --build-arg SWATH_REVISION="$(git rev-parse HEAD)" -t swath-replay-go:dev .
docker run --rm -p 127.0.0.1:19090:19090 -p 127.0.0.1:19192:19192 -v /data/sorted:/fixtures:ro \
  swath-replay-go:dev serve --fixture /fixtures --bucket bkt --host 0.0.0.0 --port 19090 --metrics-port 19192
```

The server must bind `0.0.0.0` for a published port to reach it; it has no authentication, so bind the host
side to loopback as shown unless an isolated container network is the boundary. The build context is `go/`
alone (`Dockerfile.replay-go.dockerignore`, which also leaves out local build output). The build stage
cross-compiles for the target platform, so `--platform linux/arm64` builds on an amd64 host without emulation.
Build arguments: `SWATH_REVISION` is the commit `--version` prints (the context carries no `.git`, so without
it the binary names no revision), `GOFLAGS` passes through to the Go toolchain (`-p=2` bounds the compile on a
shared host), and `GOPROXY` names a module mirror.

`sort-fixture` writes its output, so it needs a writable mount; running it as the invoking user leaves the
output removable by that user:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v /data/capture:/capture:ro -v /data/out:/out \
  swath-replay-go:dev sort-fixture --capture /capture --output /out/sorted
```

`scripts/ci/smoke-replay-container.sh IMAGE FIXTURE sorted` smokes the image: it serves `FIXTURE` (a directory
of sorted parts, or a dataset directory) as bucket `smoke` with a metrics port, waits for `/healthz` to answer
`ok`, requires a `ListBucketResult` naming the bucket and holding a key, `/metrics` with `schema_version` 2,
and the container's user `10001:10001`. When the server refuses the fixture as not servable sorted (a capture,
or a `swath list` dataset written without `--sort` by the Java lister), the smoke sorts it with the image's
own `sort-fixture` into a temporary directory and serves that. Without the third argument, or with `duckdb`,
the script smokes the Java replay image as before.

## Continuous integration

Two jobs of `.github/workflows/ci.yml` cover the Go port. Each always runs and does its work only when its
inputs changed (or on a manual run); `ci-gate` needs both.

- `go` (inputs: `go/`, the workflow, `Dockerfile.replay-go` and its ignore file, the replay smoke script):
  `go vet`, `go test -race`, the static builds of `swath` and `swath-replay` for linux/amd64, linux/arm64,
  darwin/arm64 and windows/amd64 (each linux/amd64 binary's `--version` must name the commit), then the image:
  built for linux/amd64 and linux/arm64 without pushing, the native one loaded as `swath-replay-go:ci` and
  smoked twice on fixtures written in the job, so neither needs network, Java or a recording: the Go
  lister's dataset of a `bench/s3fake` bucket of 2,000 keys (sorted, served as it is), and an unsorted
  capture from `bench/replay/capture` (2,000 objects in descending key order, no stamps), which the server
  refuses, so the smoke sorts it with the image's `sort-fixture` first; the step requires that it did.
- `go-fixtures` (inputs: `go/`, the workflow, the Java `swath-replay`, `swath-core` and `swath-model` modules,
  the shared Gradle build and wrapper (`build.gradle.kts`, `settings.gradle.kts`, `gradle.properties`,
  `gradle/`, `build-logic/`, `gradlew`), `docs/replay-provider-evidence.tar.gz`,
  `docs/replay-provider-expectations.json`, the replay smoke script): JDK 25,
  `./gradlew :swath-replay:installDist`, `bench/replay/make-fixtures.sh` into the runner's temporary
  directory, then the gated tests named in the step's `FIXTURE_PACKAGES` and `FIXTURE_TESTS` with
  `SWATH_REPLAY_FIXTURES` set. Every named test must run and pass: one that skips, or a name that matches no
  test, fails the step. A new gated test is added to those two lists.
