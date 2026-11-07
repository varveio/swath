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

`swath-replay --version` prints the build. `sort-fixture` is not built into the Go binary; sort a capture with
the Java `swath-replay sort-fixture`.

## Flags

`serve` takes the Java launcher's flags, in either `--flag value` or `--flag=value` form, with or without the
`serve` command word.

| Flag | Default | Meaning |
|---|---|---|
| `--fixture PATH` | required | the sorted fixture (above) |
| `--bucket NAME` | required | the bucket name the S3 route answers for; any other is `NoSuchBucket` |
| `--host`, `--port` | `127.0.0.1`, `0` | bind address; port `0` binds a free port, reported in the startup line |
| `--metrics-port` | `-1` | the metrics port (below); negative disables it, `0` binds a free one |
| `--max-concurrent-requests` | `512` | requests served at once (at least 8); more wait for a slot. An injected delay holds its slot, so set this above the widest client fan-out or the queue shows up as latency |
| `--parquet-connections` | `0` | concurrent fixture page decodes; `0` is `max(8, min(32, GOMAXPROCS))` |
| `--inject-latency SPEC` | off | target latency by request shape (below) |
| `--latency-jitter F` | `0` | jitter fraction in [0, 1), drawn from the request so a repeated request gets the same delay |
| `--latency-scale N` | `1` | divide every injected delay by N (compressed time); needs `--inject-latency` |
| `--response-buffer-budget` | 256 MiB | encoded response bytes held at once; a request that would exceed it is refused |
| `--max-response-bytes` | 64 MiB | the largest encoded response; a larger page is a 500 |
| `--stop-timeout` | `10s` | shutdown deadline |
| `--idle-timeout` | `30s` | idle connection timeout |
| `--write-timeout` | `30s` | response write timeout, counted from the first byte written (an injected delay does not count) |
| `--slow-request-log-ms` | `$SWATH_REPLAY_SLOW_REQUEST_LOG_MS`, else `-1` | log requests whose own cost reaches this many milliseconds; negative disables |
| `--serving-mode` | `sorted` | `sorted` only; `duckdb` is refused with the reason |
| `--protocols` | `s3` | `s3`, `gcs`, or both (`s3,gcs`); `azure` is a reserved name, refused. With `gcs` the bucket must be a valid GCS bucket name, and `storage` is refused with both |
| `--azure-account`, `--advertised-host` | | reserved for the Azure route; `--azure-account` is refused without it |

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
read returns, and the process exits 130 or 143. A stop that outlives `--stop-timeout` logs
`shutdown_incomplete`.

## The S3 route

`GET /{bucket}?list-type=2` with `prefix`, `delimiter`, `start-after`, `continuation-token`, `max-keys`,
`encoding-type=url` and `fetch-owner`. The bucket is the first segment of the path as sent (later segments are
ignored; an escaped spelling is another bucket). Keys are bytes in unsigned byte order.

- `max-keys` is echoed as asked; a page holds at most 1,000 entries, objects and common prefixes together.
  Missing or blank is 1,000; negative or not an integer is `400 InvalidArgument`.
- A `continuation-token` takes precedence over `start-after`: the page resumes at the token and the response
  omits the ignored `StartAfter`. A blank token is absent. A malformed token is `400 InvalidArgument`
  even when a `start-after` could have served. Replay tokens (`v2:...`) and S3's are not interchangeable.
- `encoding-type=url` percent-encodes every byte but `A-Z a-z 0-9 - _ . /` with upper-case hex, as S3 does.
  Without it, key bytes are rendered as UTF-8 text with malformed sequences replaced.
- `LastModified` has millisecond precision; `Owner` appears only with `fetch-owner=true`.
- Errors are S3's XML error document with `Code`, `Message`, `Resource` (the path as sent) and `RequestId`:
  `404 NoSuchBucket`, `405 MethodNotAllowed` (anything but GET), `400 InvalidArgument`. Every answer carries
  `x-amz-request-id` and `x-amz-id-2` (`S3LISTINGREPLAY`).
- The replay's own refusals carry `x-swath-replay-error` with the reason: `503 ServiceUnavailable` for
  `shutdown`, `response_count_exhausted` and `response_budget_exhausted`, `500 InternalError` for
  `response_too_large`, and `500 InternalError` (`internal`, no detail) when the fixture read fails.

## Injected latency

```bash
swath-replay serve ... --inject-latency 'worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp' \
  --latency-jitter 0.15
swath-replay serve ... --inject-latency prod-commoncrawl --latency-scale 50
```

A request with a delimiter is a `structure_probe`, a delimiter-free request with `max-keys` at most 1 a
`pivot_probe`, anything else a `worker_page` (aliases `structure`, `pivot`, `page`). A delay is `223ms`, `6.9s`
or a bare millisecond count; `+55ms/cp` (structure probes only) adds a term per common prefix returned. An
unlisted shape gets none. `prod-commoncrawl` is the built-in reference profile (223 ms pages, 121 ms pivots,
223 ms + 55 ms per prefix for structure probes).

The delay is the request's total target time, not a surcharge: the server sleeps only for what its own cost
left, so the client sees `max(cost, profile)`. A request whose own cost exceeds its profile is counted in
`swath.replay.inject.overrun{shape}`, its excess in `swath.replay.inject.overrun.ms{shape}`; a run with many
overruns measured the server, not the profile. `--latency-scale` divides every term, which keeps the profile's
shape but not absolute time (server and client CPU do not scale).

## The metrics port

`--metrics-port` serves three paths on a second listener, which never waits behind the request ceiling, never
reads the fixture and never moves a listing meter:

- `GET /healthz`: `ok` once the server is accepting. Poll it before starting a client.
- `GET /metrics`: JSON, `{schema_version: 2, serving_mode, serving: {...}, uptime_ms, sampled_at_epoch_ms,
  meters: [...]}`. `serving` holds the configuration (bucket, fixture identity, ordering profile, request and
  response bounds, charged and peak response bytes, active responses, timeouts, whether latency is injected).
  Each meter has `name`, `type` and `tags`; counters a `count`, timers `count`, `sum_ms`, `mean_ms`, `max_ms`,
  `p50_ms`, `p99_ms` (percentiles over the last 4,096 samples), distributions `count`, `total`, `mean`, `max`,
  gauges a `value`. Meters are in a fixed order so two scrapes diff cleanly; `uptime_ms` bounds an interval on
  the server's clock.
- `GET /runtime-attestation`: `runtime-attestation-v1`, the cgroup v2 `cpuset.cpus.effective`, `memory.max` and
  `memory.swap.max` of the process's own cgroup and `/proc/self/status` `Cpus_allowed_list`, each a
  `{value, error}` pair: a missing, unreadable or invalid source is an explicit error.

The meters: `swath.replay.http.requests` (every answer; the bench scripts sum its `count`),
`http.errors` (5xx), `http.request.latency`, `request.latency{shape}` (the server's own cost per shape, injected
delay excluded: read it against the profile for that shape), `request.stage.latency{protocol,stage}` (page,
render, delay, write), `fixture.list.latency`, `serving.path{mode}`, `delimiter.path{path}` (`rollup` when the
fixture answered a `/` listing in one pass, `walk` for the range walk), `inject.overrun{shape}`,
`inject.overrun.ms{shape}`, `protocol.http.requests{protocol,status_class}`,
`protocol.objects|prefixes|encoded.bytes{protocol,shape}` (shape `page`, `seek`, `delimiter`),
`protocol.requests.active{protocol}`, `response.admission.refused{protocol,reason}`,
`response.bytes.live|peak`, `response.active` and `response.write.deadline{protocol,reason}`, all under
`swath.replay.`.

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

## Comparing with the Java server

`bench/replaydiff` starts both servers on one fixture and compares every answer byte for byte (see
[internals/replay.md](internals/replay.md#the-differential-driver)):

```bash
go build -o /tmp/replaydiff ./bench/replaydiff
/tmp/replaydiff -java $JAVA_ROOT/swath-replay/build/install/swath-replay/bin/swath-replay -go ./swath-replay \
  -fixture /data/sorted -bucket bkt -report /tmp/diff.json     # exit 1 and FAIL lines on a difference
```

## Limits

- Two routes (S3 `ListObjectsV2`, path-style, and the GCS JSON `objects.list` below) and one serving mode (sorted). Version listing, object data and
  authentication are not served.
- The Java server's HTTP layer rejects some request targets before the S3 route sees them (an empty path
  segment such as `//bucket`, an encoded `/` in the path, a raw non-ASCII byte in the path, a `#` in the
  target) with an HTML `400`; this server answers them as S3 requests (`NoSuchBucket` or a listing).
- Timer `max_ms` is the all-time maximum and percentiles cover the last 4,096 samples; the Java registry decays
  both over a time window, so the two servers' timer fields agree on counts and sums, not on those.
- `max-keys` accepts decimal digits from every script the Go toolchain's Unicode tables know; digits added in
  later Unicode versions are not integers here.
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
order: unknown or repeated names and bad escapes (left to right), the four boolean features (in the table's
order), `matchGlob`, `filter`, `fields`, `alt`, `prettyPrint`, `projection`, `maxResults`, the field lengths,
then the token.

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
replaced by the replay's token for the same position (or its tampered, flipped or truncated form), a walk and
an SDK run re-walked with the replay's own tokens (an SDK run with the run's captured query scope and client
headers). Each answer is compared with the provider's under the conformance field policy: status, classified
headers, then the page (items field by field, prefixes, terminal pagination; walks as merged inventories with
unbroken token chains) or the error envelope's first reason. Each outcome must be its pin in
`gcs/replay-expectations.json` (with `docs/replay-provider-expectations.json` applied) or better: a pinned
mismatch that now matches is logged as an improvement. `SWATH_PROVIDER_REPORT=DIR` writes the per-capture
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
