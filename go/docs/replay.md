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
| `--max-concurrent-requests` | `512` | requests served at once (at least 8); more wait for a slot. An injected delay holds its slot, so set this above the widest client fan-out or the queue shows up as latency. Writing a response does not hold a slot: up to twice this many responses can be outstanding, and past that a request is refused (`response_count_exhausted`) |
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
`response.bytes.live|peak`, `response.active`, `response.chunk.allocation{reason}` and
`response.chunk.capacity.bytes{reason}` (response chunks by `initial`, `initial_cap_partial`, `continuation`,
`cap_partial`), `response.percent.encoding.path{protocol,reason}` and `response.write.deadline{protocol,reason}`,
all under `swath.replay.`.

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
/tmp/replaydiff -protocol azure -walk-limit 300 ...                   # the Azure route, both servers on s3,azure
```

## Limits

- Three routes (S3 `ListObjectsV2`, path-style, the GCS JSON `objects.list` and the Azure flat `List Blobs`
  subset below) and one serving mode (sorted). Version listing, object data and
  authentication are not served.
- Two request-target cases differ from the Java server: a request line Go's HTTP parser refuses itself
  (whitespace or a control byte in the target, a malformed `%` escape in the path, a target with neither a
  leading `/` nor a scheme) gets its `400` with a `text/plain` page; and an absolute target whose host differs
  from the `Host` header, which the Java server refuses, is served, because Go's HTTP layer ignores the `Host`
  header for an absolute target as RFC 9112 section 3.2.2 has an origin server do.
- Timer `max_ms` is the all-time maximum and percentiles cover the last 4,096 samples; the Java registry decays
  both over a time window, so the two servers' timer fields agree on counts and sums, not on those.
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
  smoked. The smoke's fixture is written in the job: `bench/s3fake` serves 2,000 keys and the Go lister's
  dataset of that bucket is the fixture, so the smoke needs no network, Java or recording.
- `go-fixtures` (inputs: `go/`, the workflow, the Java `swath-replay`, `swath-core` and `swath-model` modules,
  the shared Gradle build and wrapper (`build.gradle.kts`, `settings.gradle.kts`, `gradle.properties`,
  `gradle/`, `build-logic/`, `gradlew`), `docs/replay-provider-evidence.tar.gz`,
  `docs/replay-provider-expectations.json`, the replay smoke script): JDK 25,
  `./gradlew :swath-replay:installDist`, `bench/replay/make-fixtures.sh` into the runner's temporary
  directory, then the gated tests named in the step's `FIXTURE_PACKAGES` and `FIXTURE_TESTS` with
  `SWATH_REPLAY_FIXTURES` set. Every named test must run and pass: one that skips, or a name that matches no
  test, fails the step. A new gated test is added to those two lists.
