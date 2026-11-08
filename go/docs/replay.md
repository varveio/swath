# swath-replay (Go)

`swath-replay` serves a recorded listing back through the S3 `ListObjectsV2` API, so a lister can be tested
and benchmarked against a real bucket's shape without the bucket; with `--protocols` it also serves the Cloud
Storage JSON API's `objects.list` and Azure Blob's flat `List Blobs` from the same recording. It is a
contributor tool, built from the same module as `swath`, a separate binary (`cmd/swath-replay`).
[internals/replay.md](internals/replay.md) says how it is built and which tests hold its answers;
[internals/replay-java.md](internals/replay-java.md) compares it with the Java `swath-replay serve` it was
ported from.

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

`--fixture` is a sorted listing: a dataset directory this tool wrote (SeekParquet, read through the seekparquet
module `github.com/varveio/seekparquet/go`; one whose keys repeat, an `--all-versions` listing, is refused until
the versions routes exist), a format-2 dataset directory of an
earlier build, a dataset's `data/` directory, a directory of sorted parts written by Java `swath --sort`, or one
such part. A fixture that cannot
be served sorted is refused when it opens, with the reason. A file ending
in `.tsv` is instead a key list loaded into memory (one object per line: key, size, last-modified microseconds,
ETag, storage class, owner id, owner display name, checksum algorithm, checksum type; fields percent-escaped,
`\N` for null), for tests and for listings that have no Parquet form; `bench/replaydiff -dump-tsv` writes one.

`swath-replay --version` prints the build.

`swath-replay sort-fixture --capture PATH --output DIR [--key-limit N]` turns an unsorted capture (one Parquet
part in the 13-column canonical schema, or a directory of `*.parquet` parts) into a sorted SeekParquet dataset
that `serve` can open. `--output` must be new or empty; like the lister, the command takes the output's lock and
leaves `.swath/` with the lock file in it. Only `OBJECT` rows are written; a duplicate key, a versioned row, an
unknown `row_type` or a key over the cap (1,024 bytes; `--key-limit` raises it up to 4,096) is refused before
anything is written. On success it prints one line on stdout:

```
sort_fixture rows=10000 objects=10000 skipped=0 parts=1 bytes=230500 schema=swath-replay/objects/v1 wall_ms=40 output=/data/sorted
```

A refusal is one `sort-fixture: ...` line on stderr and exit 1; a bad command line is exit 2. The capture is
held in memory while it is sorted (about 300 bytes per row), so it suits captures of a few million rows.

## Flags

`serve` takes its flags in either `--flag value` or `--flag=value` form, with or without the `serve` command
word.

| Flag | Default | Meaning |
|---|---|---|
| `--fixture PATH` | required | the sorted fixture (above) |
| `--bucket NAME` | required | the bucket name the S3 route answers for; any other is `NoSuchBucket` |
| `--host`, `--port` | `127.0.0.1`, `0` | bind address; port `0` binds a free port, reported in the startup line |
| `--metrics-port` | `-1` | the metrics port (below); negative disables it, `0` binds a free one |
| `--max-concurrent-requests` | `512` | requests served at once (at least 8); more wait for a slot. An injected delay holds its slot, so set this above the widest client fan-out or the queue shows up as latency. Writing a response does not hold a slot: up to twice this many responses can be outstanding, and past that a request is refused (`response_count_exhausted`). The serving listener holds at most four times this many connections; one more is closed as soon as it is accepted |
| `--parquet-connections` | `0` | concurrent fixture page decodes; `0` is `max(8, min(32, GOMAXPROCS))` |
| `--prefetch-window-rows` | `12500` | the rows a walk's window cache fill ramps to (below); `0` turns the cache off |
| `--prefetch-max-windows` | `192` | the windows the cache holds, at most this many times `--prefetch-window-rows` rows; `0` turns it off |
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

When both ports are bound the server prints one line to stderr, `key=value` pairs in a fixed order:

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

- The query is read as a form, as live S3 reads it: `+` is a space, `%XX` a byte, and a parameter whose name or
  value holds a malformed escape (`%ZZ`, a trailing `%`) is dropped, so `prefix=a%ZZ` lists from the bucket's
  first key with an empty `Prefix`. Of a repeated parameter the first value counts. `prefix`, `delimiter` and
  `start-after` are read as UTF-8, each malformed sequence one U+FFFD (`start-after=%FF` bounds and echoes as
  U+FFFD).
- `max-keys` is echoed as asked; a page holds at most 1,000 entries, objects and common prefixes together.
  Missing or blank is 1,000; not an integer (or outside int32) is `400 InvalidArgument` "Provided max-keys not an
  integer or within integer range", negative is "Argument maxKeys must be an integer between 0 and 2147483647"
  (`max-keys=+2` is " 2", not an integer). An integer is an optional sign and decimal digits from any script of
  the Basic Multilingual Plane (`١٢` is 12); a digit outside it (such as U+1D7D9) is not an integer.
- Refusals, each `400 InvalidArgument` with live S3's message, checked in S3's order after `max-keys`: a `marker`
  (ListObjects V1's, even empty); an `encoding-type` other than `url` in any case (the empty one included); a
  `continuation-token` that is empty, blank, not in this server's token form or whose checksum fails; a NUL in `prefix`, or
  in a `start-after` that is used (a token wins over it).
- A `continuation-token` takes precedence over `start-after`: the page resumes at the token and the response
  omits the ignored `StartAfter`. A token is unpadded base64url of the resume boundary and a CRC-32, so a
  flipped or dropped character is refused as S3 refuses it (S3 serves some alterations of its own decoded
  bytes; this server refuses every one). A dropped or appended character is accepted only when the shorter or
  longer payload is by a 1-in-2^32 coincidence the token of another boundary. The checksum covers only the flag and the boundary: it shows a token was
  not altered, not where it came from. No bucket, fixture, process or secret is bound in, so a token survives a
  restart, is accepted by a server on another fixture, and one a client builds for any boundary is accepted
  (one past the fixture's keys gives an empty page), as a `start-after` would be (`TestTokenIsNotBound`). A flat page's token sent with a delimiter resumes
  at its key and rolls up what follows, the common prefix that key falls in included; a `start-after` inside a
  common prefix passes it. Replay tokens and S3's are not interchangeable.
- An empty `start-after` bounds nothing and is echoed as `<StartAfter></StartAfter>`.
- `encoding-type=url` writes letters, digits, `-`, `_`, `.`, `/` and `*` as they are, a space as `+`, and every
  other byte as `%XX` with upper-case hex; `EncodingType` echoes the value as sent (`URL`). Without it, key
  bytes are rendered as UTF-8 text with malformed sequences replaced, the five XML metacharacters as entities,
  and a C0 control character other than TAB, LF and CR as a lower-case hexadecimal character reference
  (`&#x0;`, `&#x1;`, `&#xb;`, `&#x1f;`); TAB, LF, CR, DEL and every other character are written as they are.
  This holds in `Key`, `Prefix`, `StartAfter`, `Delimiter` and `CommonPrefixes/Prefix`.
- A listing and an error document both start with `<?xml version="1.0" encoding="UTF-8"?>` and a newline (LF),
  then the root element, the bytes live S3 wrote on 2026-11-07 (`TestLiveXMLDeclarationNewline`).
- The rules in the four items above, the form reading and the refusals answer as live S3 does (probed on
  2026-11-07 against a public `us-east-1` bucket, anonymous; the `TestLive*` tests of `s3` pin each).
  ListObjects V1 (no `list-type`) and `list-type=3` are refused (`400 InvalidArgument`), where S3 serves them;
  `fetch-owner=true` renders `Owner` only where the fixture holds owner columns.
- `fetch-owner` asks for owners only when its value, read as above, is exactly `true` (`%74rue` too): `TRUE`,
  `True`, `yes`, `1`, an empty or bare `fetch-owner`, `true%20` and `+true` list without `Owner`, and of a
  repeated `fetch-owner` the first value decides (`TestLiveFetchOwnerIsExactlyTrue`).
- `LastModified` has millisecond precision, with the year of era (`0001` for year 0, `+10000` past year 9999);
  `Owner` appears only with `fetch-owner=true`, after `Size` and before `StorageClass`, where live S3 writes it
  (`TestLiveOwnerBeforeStorageClass`; S3 wrote `ID` alone on 2026-11-07, and a recording that holds a display
  name renders it after the `ID`). Which fields a `Contents` element carries, and from what, depends
  on the recording's schema ([Item fields by route and recording schema](#item-fields-by-route-and-recording-schema)):
  a `swath/gcs-objects/v1` recording is listed as Cloud Storage's own S3-compatible XML API lists it.
- Errors are S3's XML error document, as live S3 wrote each: `Code`, `Message`, then the elements of that
  error, then `RequestId` and `HostId` (the `x-amz-request-id` and `x-amz-id-2` values; S3's change with every
  request, the replay's are `S3LISTINGREPLAY`). `400 InvalidArgument` carries `ArgumentName` and, except for a
  token or a `marker`, `ArgumentValue` (the value as read: `+` a space, malformed UTF-8 replaced); S3 names a
  negative `max-keys` `maxKeys` and a `start-after` `key-marker`. `404 NoSuchBucket` carries `BucketName` (the
  path's first segment, percent-decoded; none for the root). `405 MethodNotAllowed` (anything but GET) carries
  `Method` and `ResourceType` `BUCKET`, with S3's message "The specified method is not allowed against this
  resource."; live S3 itself answers an anonymous POST, PUT or DELETE with other codes. Error text is escaped
  as S3 escapes it: `&`, `<` and `>` as entities, quotation marks and apostrophes as they are, a C0 control
  character other than TAB, LF and CR as a decimal reference (`&#0;`, `&#1;`). The `list-type` refusal is the
  replay's own (S3 serves ListObjects V1) in that form: `ArgumentName` `list-type` and the value when present.
  The replay's own refusals below carry no element between `Message` and `RequestId`.
- The replay's own refusals carry `x-swath-replay-error` with the reason: `503 ServiceUnavailable` for
  `shutdown`, `response_count_exhausted` and `response_budget_exhausted`, `500 InternalError` for
  `response_too_large`; `500 InternalError` with `fixture_disordered` when a row group's keys turn out not to
  ascend (the message names the row group and the part's file name, never its directory); and
  `500 InternalError` (`internal`, no detail) when the fixture read fails otherwise.
- A request head (request line, headers, blank line) may be 8 KiB with the S3 route alone and 64 KiB with a
  native route enabled, the replay wire contract's limits (`server` `TestRequestHeadLimits`): a request line
  longer than that is `414 URI Too Long`, a longer head `431 Request Header Fields Too Large`, both with the HTML
  refusal page below.
- The listing routes take no request body. A request that declares one (`Content-Length` or chunked) is answered
  as it would be without it, and its connection is then closed instead of drained; a body that never arrives
  holds nothing (`server` `TestUnsentBodyHoldsNothing`).
- Some request targets are refused before the route, with `400`, `Content-Type: text/html;charset=iso-8859-1`
  and `Cache-Control: must-revalidate,no-cache,no-store`, by the replay wire contract's rules
  (`server` `TestRejectsAmbiguousTargets`): a fragment
  (`#`), an empty path segment other than the last (`//bucket`, `/;/bucket`), an encoded `/` or `%`, an encoded
  dot segment (`%2e%2e`) or a dot segment with a parameter (`..;`), a raw `\` or a raw byte outside the path
  characters (`"`, `<`, `|`, non-ASCII, ...), an encoded control character or `\`, invalid UTF-8 once decoded,
  a malformed escape or an encoded NUL, a path that climbs above the root (`/../bucket`), and user information
  in an absolute target. The body is a short HTML page naming the problem. Some malformed request lines are
  answered by the HTTP layer itself (Limits).

Response memory (the same on every route). A request reserves room for a full page (on S3 320 bytes an entry,
at least 4 KiB; on GCS `1024 + 384` an entry; on Azure `2048 + 512` an entry; at most `--max-response-bytes`)
before it reads the fixture, and is refused (`response_budget_exhausted`) if the budget
has no room for that. The response is then rendered into chunks sized from the page it actually got, each
charged before it is allocated; rendering stops at the first byte past `--max-response-bytes`
(`response_too_large`) or past the budget. The unused reservation goes back before any injected delay, and the
chunks are held until the response is written.

## The window cache

A paginating client reads the fixture in order, a page at a time. The window cache in front of the store's
range reads lets one fixture read serve the pages that follow it: when a read's lower bound is the tail of a
page the server answered before (an S3 continuation token or `start-after`, the GCS and Azure tokens), its fill
reads four times the rows the previous fill read, up to `--prefetch-window-rows` rounded up to a multiple of the
page size, and keeps them as a window; the next pages are sliced from the window. Once a walk's page leaves less
than a quarter of its window unread, the next window is read ahead in the background (at most half as many at
once as there are processors, each holding a reader slot while it reads), and a walk that reaches the end of its
window while that read is in flight waits for it: walks paced alike, such as a fleet under a latency profile,
otherwise reach their windows' ends together and queue for the reader slots at once
(`TestWindowCacheReadsAhead`; `server` `TestLoadNoOverrunAtSixtyFourWalks`). Any other read (a first page, a
probe, a seek) reads only what it asks for, and keeps it only when the fixture had more. A read is answered
from the windows only when they hold every row its bounds select, from one window or two adjacent ones;
anything less reads the fixture, so an answer is byte for byte the one a fixture read gives
(`store` `TestWindowCacheMatchesTheFixture`). A read answered from a window takes no reader slot: the
`--parquet-connections` slots bound fixture reads only (`TestWindowCacheHitsTakeNoReaderSlot`). Delimiter
rollups, and the GCS route's reads of a `swath/gcs-objects/v1` dataset (which project its own columns), read
the fixture.

Windows are evicted least recently used first, past `--prefetch-max-windows` windows or past that many times
`--prefetch-window-rows` rows (a walk holds two while it reads the last quarter of one, so the default of 192
windows serves 96 walks); a window's rows are held packed, the keys and each text column in one array
apiece (about 130 bytes a row for the canonical schema with 44-byte keys). With the defaults the cache holds
up to 2.4 million rows (the server's resident memory under load is in Measurements below). `0` for either flag
turns the cache off: every read then decodes its pages from the fixture. The cache's meters are `prefetch.*`
(below); a read-ahead is counted as a fill (`prefetch.window.fill`, `prefetch.fill.rows`), not as a miss.

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
  `400 InvalidQueryParameterValue`. A marker's bytes depend only on its scope and resume point, so a restart on
  the same fixture identity issues the same markers (`azure` `TestMarkerGolden`).
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

A name of more than 254 `/`-separated segments is listed, as native Azure lists it (the evidence archive's
capture `azure-12a`; `azure` `TestRenderRefusals`).

## Item fields by route and recording schema

The rule: a recording served on its own provider's API answers exactly as stored; on any other API, each field
is either mapped to the equivalent value, where a real equivalent exists, or a fixed placeholder. The S3-shaped
schemas are S3's own (`swath/canonical` is the format-1 schema, `swath/s3-objects/v1` the Go lister's S3
datasets, `swath-replay/objects/v1` what `sort-fixture` writes from a capture; all three hold the same nine
columns); `swath/gcs-objects/v1` is GCS's own; `swath-local/v1` (a local tree's size and modification time) has
no provider API, so every route maps or fills it.

| Route: field | S3-shaped: `swath/canonical`, `swath/s3-objects/v1`, `swath-replay/objects/v1` | `swath/gcs-objects/v1` | `swath-local/v1` |
|---|---|---|---|
| S3: `Key` | exact | mapped from the object name (the same bytes) | mapped from the relative path |
| S3: `LastModified` | exact, milliseconds (the epoch when null) | mapped from `time_created` (GCS `timeCreated`, which the XML API lists), milliseconds; from `last_modified` (GCS `updated`) where `time_created` is null or the part lacks the column | mapped from the modification time, milliseconds |
| S3: `ETag` | exact, absent when null | mapped: the MD5 in lower-case hex; without an MD5, `-` and the etag; neither: placeholder `swath-s3-synthetic-v1` | placeholder `swath-s3-synthetic-v1` (S3 lists every object with an ETag) |
| S3: `Size` | exact (`0` when null) | mapped from `size` (the same value) | mapped from the file size |
| S3: `StorageClass` | exact, absent when null | absent (the XML API sends none, whatever the class) | placeholder `STANDARD` (S3 lists every object with a class) |
| S3: `ChecksumAlgorithm`, `ChecksumType` | exact, absent when null | absent (the XML API sends none) | absent (S3 sends none for an object stored without a checksum) |
| S3: `Owner` (`fetch-owner=true`) | exact, absent when both columns are null | absent (not stored; the XML API sent none) | absent, no real equivalent: an S3 owner `ID` names an account, nothing in a local tree does (its uid names a user of one host), and a placeholder would invent one. Live S3 drops an owner field it does not return rather than filling it (2026-11-07: `Owner` with `ID` alone, no `DisplayName`); the route answers `fetch-owner=true` without `Owner` for every recording that holds none |
| GCS: `name`, `size` | mapped from the key and size (`0` when null) | exact | mapped from the path and file size |
| GCS: `updated` | mapped from `last_modified` (six digits; the epoch when null) | exact | mapped from the modification time |
| GCS: `timeCreated` | absent (not stored) | exact (six digits), just before `updated`; absent when `time_created` is null or the part lacks the column | absent |
| GCS: `etag` | placeholder `swath-gcs-synthetic-v1` (an S3 ETag is not a GCS etag) | exact (`swath-gcs-synthetic-v1` when null) | placeholder `swath-gcs-synthetic-v1` |
| GCS: `generation`, `metageneration` | placeholder `1` | exact (`1` when null) | placeholder `1` |
| GCS: `storageClass` | placeholder `STANDARD` | exact (`STANDARD` when null) | placeholder `STANDARD` |
| GCS: `contentType` | placeholder `application/octet-stream` (S3 lists no content type) | exact, absent when null | placeholder `application/octet-stream` |
| GCS: `md5Hash`, `crc32c` | absent (an S3 ETag is an MD5 only for some uploads, and the listing cannot tell which) | exact, absent when null | absent |
| GCS: `acl` (`projection=full`) | `[]` | `[]` | `[]` |
| Azure: `Name`, `Content-Length` | mapped from the key and size | mapped from the name and size | mapped from the path and file size |
| Azure: `Last-Modified` | mapped, seconds (Azure's precision) | mapped from `last_modified` (`updated`, which moves with metadata changes as Azure's does), seconds | mapped, seconds |
| Azure: `Etag` | placeholder `0x000000000000001` | placeholder `0x000000000000001` | placeholder `0x000000000000001` |
| Azure: `Content-Type` | placeholder `application/octet-stream` | mapped from `content_type`; placeholder `application/octet-stream` when null or a value Azure cannot hold (a control character, not UTF-8) | placeholder `application/octet-stream` |
| Azure: `Content-MD5` | absent | mapped from `md5` (GCS lists it as the standard base64 of 16 bytes, the form `Content-MD5` takes), after `Content-Type` and before `BlobType`, where native listings place it; absent when null or in any other form | absent |
| Azure: `BlobType`, `AccessTier`, `AccessTierInferred`, `LeaseStatus`, `LeaseState` | placeholders `BlockBlob`, `Hot`, `true`, `unlocked`, `available` | the same placeholders | the same placeholders |

The S3 view of a `swath/gcs-objects/v1` recording is Cloud Storage's own S3-compatible XML API's answer for the
same objects, probed on 2026-11-07 (`storage.googleapis.com`, anonymous `list-type=2` on public buckets, with and
without `fetch-owner=true`): `ETag` is the object's MD5 in lower-case hex, quoted; a composite object, which has
no MD5, has `-` and its JSON API `etag`, quoted (the etag already carries the metageneration); no `StorageClass`,
`Owner`, `ChecksumAlgorithm` or `ChecksumType` element appears for a STANDARD, NEARLINE or MULTI_REGIONAL object
(no public COLDLINE or ARCHIVE object was found to probe); `LastModified` is the object's `timeCreated`, not its
`updated` (they differ once its metadata changed, metageneration above 1), so the route maps it from
`time_created`. A recording's parts written before the lister listed `timeCreated` hold no `time_created` column;
their rows keep `updated`. An `md5` value that is not the standard base64 of 16 bytes is not taken for an MD5. The
placeholder `swath-s3-synthetic-v1` is neither 32 hexadecimal digits with an optional `-N` part count (an S3 ETag)
nor `-` and base64 (the XML API's composite form), so a client cannot take it for a real ETag; Cloud Storage lists
every object with an etag, so only a recording made some other way lacks both. The route writes no `Generation` or
`MetaGeneration` element, which the XML API adds and S3 does not have, and writes the ETag's quotation marks as
`&quot;`, as live S3 does (`server` `TestLiveGCSXMLAPIContents` pins the probed elements,
`TestGCSRecordingOnTheS3Route` the edge rows, `TestResumedGCSRecordingMixesParts` a recording with parts of both
kinds).

The S3-shaped schemas' answers on every route are the replay wire contract's
(`server` `TestEmptyAndNullStringsEndToEnd`).

## Injected latency

```bash
swath-replay serve ... --inject-latency 'worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp' \
  --latency-jitter 0.15
swath-replay serve ... --inject-latency prod-commoncrawl --latency-scale 50
```

A request with a delimiter is a `structure_probe`, a delimiter-free request with `max-keys` at most 1 a
`pivot_probe`, anything else a `worker_page` (aliases `structure`, `pivot`, `page`). A delay is `223ms`, `6.9s`
or a bare millisecond count; `+55ms/cp` (structure probes only) adds a term per common prefix returned. An
unlisted shape gets none. The number before `ms` or `s` is a decimal or hexadecimal floating-point number with
an optional `d` or `f` suffix (`1.5e3ms`, `2dms`, `0x1p0s`), the replay wire contract's grammar (`latency`
`TestNumberGrammar`); a delay too long to represent saturates at about 292 years rather than
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

A timer's `count` and `sum_ms`, and a distribution's `count` and `total`, cover the server's life. A timer's
`max_ms`, `p50_ms` and `p99_ms` and a distribution's `max` cover recent samples, with a one-minute expiry and
three buffers (the replay wire contract's windows), in two different windows, both counted from the meter's
creation and rotated when a sample is recorded or the meter is read:

- **The maximum** is kept in three slots, each sample raising all three. Each minute the slot read is cleared and
  the next becomes the one read; after three minutes without a rotation all three are cleared. A sample stays
  the maximum for two to three minutes.
- **The percentiles** come from three histograms, each sample recorded in all three. Every 20 seconds the one read
  is cleared and the next becomes the one read, so they describe the last 40 to 60 seconds. One rotation clears
  at most three histograms and moves the rotation clock 20 seconds per histogram, so after a long idle spell the
  next few scrapes keep clearing until the clock catches up.

For example, after 900 ms at 0 s and 10 ms at 79 s, a scrape at 79 s reads `p50_ms` 10 and `max_ms` 900, at 121 s
`p50_ms` 0 and `max_ms` 900, at 181 s `max_ms` 10 (`metrics` `TestWindowsFollowTheJavaRegistry`). The histograms
are log-linear (32 buckets per power of two), so a percentile is within 3.1% above the sample it stands for.
Read a measured window's mean as the difference of two scrapes' `sum_ms` over the difference
of their `count`. `index.load.latency` and `sortfixture.build.latency` publish no percentiles (`null`).

### The meters

All names begin `swath.replay.`. Meters marked "from startup" are there, at zero, before the first request; the
others appear on first use. `server` `TestMeterSetMatchesTheContract` pins the names, types and tags a scrape
lists after a request mix on all three routes (`testdata/meters.txt`). The server publishes no process or host
meters: read the process from `/proc` and its limits from `/runtime-attestation`.

| Meter | Published | Meaning |
|---|---|---|
| `http.requests`, `http.errors`, `http.request.latency` | from startup | every answer on the serving port; `errors` the 5xx. The latency runs from arrival to the start of the write. The bench scripts sum `http.requests` `count` |
| `request.latency{shape}` | from startup | the S3 route's own cost per shape (`worker_page`, `pivot_probe`, `structure_probe`), injected delay excluded |
| `request.stage.latency{protocol,stage}` | on first use | `page`, `render`, `delay`, `write` of a page; an error answer has no write stage |
| `fixture.list.latency`, `serving.path{mode}` | from startup | one S3 pager operation |
| `serving.refused{reason}` | on first use | `row_group_disorder` (a Java-era or format-2 fixture) or `key_order` (a SeekParquet dataset), counted for every read that finds it, a range read or a rollup |
| `index.load.latency{source=derived}`, `index.entries` | from startup | the fixture open (footers, routing keys); one routing entry per non-empty row group |
| `page.read.latency` | from startup | how long a store read holds its reader slot, slot wait excluded |
| `parquet.query.latency`, `parquet.query.rows`, `parquet.query.errors` | from startup | one fixture read, slot wait included; its rows or rollup entries. A window cache fill is one such read; a read answered from a window is none |
| `parquet.queries.in_flight`, `parquet.queries.peak` | from startup | reads holding one of the `--parquet-connections` slots, now and at most. One pool serves every read, so the peak is at most the flag |
| `delimiter.path{path}` | on first use | `rollup` or `walk` for an S3 delimiter page |
| `delimiter.skipscan.row_group_opens`, `.whole_group_shortcuts`, `.decoded_key_rows`, `.page_reseeks` | from startup; absent over a SeekParquet dataset (the seekparquet module reports no page work: unavailable, not zero) | the `/` skip-scan's key cursors opened, groups emitted from routing keys alone, and per rollup the key values decoded and the page-index reseeks. A key cursor stays open on its group for the whole rollup and decodes whole key pages |
| `handle_cache.opens`, `.evictions`, `.waits`, `.open`, `.peak`, `.limit` | from startup | every part is read through one descriptor cache: first opens and reopens after eviction, evictions, and reads that waited for a descriptor as counters; descriptors open, at most, and the limit (half the soft descriptor limit, at most 4,096) as gauges |
| `sortfixture.build.latency`, `sortfixture.output.bytes` | from startup, zero | recorded by `sort-fixture`, never by a server |
| `prefetch.window.hit`, `.window.miss{reason}`, `.window.fill`, `.fill.rows`, `.anchor{event}`, `.window.ramp_ceiling_rows`, `.window.eviction`, `.window.row_budget_eviction`, `.window.selection{reason}`, `.window.join{reason}`, `.window.uncached{reason}`, `.windows.live`, `.anchors.live`, `.rows.live` | while the window cache is on (the counters without a reason tag, the anchors' three events and the gauges from startup) | the window cache (above): reads answered from windows; misses by `continuation` or `cold`; each fill's time and the rows it asked for; continuation anchors `registered`, `claimed`, `evicted_before_claim` and a claimed one's ramp ceiling; windows evicted, and those the row bound forced; how a read was served (`single_exact_bound`, `single_contained_bound`, `two_window_join`, `insufficient`, `no_cover`); a declined join's reason; `request_exceeds_window`, `rounded_exceeds_row_budget`, `window_exceeds_row_budget`; and the windows, anchors and rows held |
| `response.admission.refused{protocol,reason}`, `response.bytes.live`, `response.bytes.peak`, `response.active` | on first use (gauges from startup) | refusals by `shutdown`, `response_count_exhausted`, `response_budget_exhausted`, `response_too_large` |
| `response.chunk.allocation{reason}`, `response.chunk.capacity.bytes{reason}` | from startup | chunks by `initial`, `initial_cap_partial`, `continuation`, `cap_partial` |
| `response.percent.encoding.path{protocol,reason}`, `response.write.deadline{protocol,reason}` | on first use | |
| `protocol.requests.active{protocol}` | from startup | per enabled route, until the answer is written |
| `protocol.http.requests{protocol,status_class}`, `protocol.objects`, `protocol.prefixes`, `protocol.encoded.bytes{protocol,shape}`, `provider.path{protocol,path,reason}` | on first use | |
| `inject.overrun{shape}`, `inject.overrun.ms{shape}` | on first use | absent while nothing overran; zero is the healthy state |

### Diagnostics

- **Slow requests.** `--slow-request-log-ms N` (or `SWATH_REPLAY_SLOW_REQUEST_LOG_MS`) logs every S3 page whose
  own cost reaches N ms, as one line with no time or level prefix (`TestSlowRequestLogLine`):

  ```text
  replay slow request shape=STRUCTURE_PROBE ms=0.690114 prefix=logs/ delimiter=/ start_after=logs/a continuation=none max_keys=7 entries=4 truncated=false
  ```

- **The startup line** has its keys in a fixed order (`TestServeEndToEnd`); `fixture_identity` is also in the
  payload's `serving` object.
- **Refusals** are typed: `serving.refused{reason}` for the fixture, `response.admission.refused{protocol,reason}`
  for admission, and the `x-swath-replay-error` header on the answer.
- **At shutdown** the server logs no summary of its meters; scrape `/metrics` before stopping it to keep them.
- **Profiles.** `SWATH_CPUPROFILE=FILE` and `SWATH_MEMPROFILE=FILE` (the lister's variables) write a CPU and an
  allocation profile for `go tool pprof`, covering the server's life from the startup line; both are written when
  SIGINT or SIGTERM stops it.
- A client that disconnects while its page is being read does not cancel the read: the page is read, rendered and
  counted as served (`2xx`, nothing in `http.errors`, no log line), and only its write fails
  (`TestClientGoneFinishesThePage`). Shutdown still interrupts it. A client that leaves while its request
  sleeps in an injected delay is not noticed either: the delay runs out and the write fails without a log line
  (`bench/resume_storm.sh`, which kills a listing with 64 requests in flight, leaves the server log clean).

## With the bench scripts

`bench/fault_replay.sh`, `bench/policy_replay.sh` and `bench/defaults_replay.sh` start `swath-replay` from
`PATH` after sourcing `$REPLAY_ENV`. To run them on this server, point `REPLAY_ENV` at a file that puts its
directory first:

```bash
cat > /tmp/go-replay.env <<EOF2
export PATH=$PWD:\$PATH            # the directory holding this swath-replay
EOF2
REPLAY_ENV=/tmp/go-replay.env RECORDINGS=/data/rec FAULTPROXY=/tmp/faultproxy \
  bench/fault_replay.sh arms.tsv noaa-normals-pds 95 19090 1 /tmp/out
```

The server takes the scripts' flags (`--serving-mode sorted --parquet-connections 8 --max-concurrent-requests
1024 --inject-latency ...`) and answers their reading of `/healthz` and `swath.replay.http.requests`.

`bench/replayload` drives a running server with concurrent S3 token walks and prints one line: walks,
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

## Live conformance

The Go server is held to the provider by recordings: a client's requests and the provider's answers, captured
once by the recording proxy and replayed offline as often as needed (Live capture, below). The recording that
carries the edge cases is the live matrix described here, sent once through the proxy (The recorded live
matrix, below); a live run is how such a recording is made, and a replay of it judges every answer by the same
comparison as the live run did.

`bench/replaydiff -live-endpoint` compares the Go server with the provider itself: anonymous requests to live S3
or GCS, and the same requests to the Go server serving a recording of the same bucket. A live bucket changes, so
`-live-prefix` keeps every request inside one scope (a request of the matrix that names no prefix is given the
scope as its prefix) and `-live-stable-before` refuses to run unless every recorded name there was last modified
before the given time (the recording's start).

```bash
go build -o /tmp/replaydiff ./bench/replaydiff
/tmp/replaydiff -live-endpoint https://s3.us-east-1.amazonaws.com -protocol s3 -go ./swath-replay \
  -fixture /data/rec/noaa-normals-pds/dataset/data -bucket noaa-normals-pds -report /tmp/live.json
/tmp/replaydiff -live-endpoint https://s3.eu-central-1.amazonaws.com -protocol s3 -go ./swath-replay \
  -fixture /data/rec/gleif/dataset/data -bucket gleif -live-prefix data/csv/lei/date=2025- \
  -live-stable-before 2026-11-06T22:00:33Z -report /tmp/live-gleif.json
/tmp/replaydiff -live-endpoint https://storage.googleapis.com -protocol gcs -go ./swath-replay \
  -fixture /data/rec/gcs/BUCKET/dataset/data -bucket BUCKET -live-prefix PREFIX -report /tmp/live-gcs.json
```

The S3 endpoint is path-style (`https://s3.REGION.amazonaws.com`) or virtual-host
(`https://BUCKET.s3.REGION.amazonaws.com`). Outbound requests honour `HTTPS_PROXY`; no `Accept-Encoding` is sent.

**Requests.** The provider conformance matrix of `scripts/provider-conformance` (97 S3 and 110 GCS cases, the
cases the native evidence was captured with), each bound to names under the scope that keep the corpus name's
part in the question, then token walks of the scope and of each of its directories (up to `-max-top`), flat and
with `/`, at each `-walks` page size (default 7 and 1,000) whose walk takes at most `-live-walk-pages` pages
(300). Each side follows its own tokens; a token case takes each side's own token from its source walk and
alters it as the campaign did. `-live-scripted` adds the driver's own scripted matrix (the requests that mean the
same against the provider). A public bucket cannot answer every case: 25 S3 and 24 GCS cases need an owned
bucket's corpus (names across the surrogate block and the supplementary planes, NFC/NFD and case pairs;
reserved and escape-like characters whose presence the spelling probe decides; control, XML-invalid and
dot-segment names; names at the length limits) or an official client library, and some need a shape the
recording lacks (a directory of exactly 1,000 names, of six objects, a name that is a byte prefix of another).
The report lists every case with its query or the reason it was not sent.

**Comparison.** The conformance suite's field policy (its exchange comparator with the S3 and GCS profile
comparators, and the campaign's `evidence.py` normalizations): the same status; on each side only the
classified headers (S3: `Content-Type`, `Date`, `x-amz-request-id`, `x-amz-id-2`, `x-amz-bucket-region`,
`Content-Length`, `Server`, `Transfer-Encoding`, `Connection`, `x-swath-replay-error`; GCS: `Content-Type`,
`Date`, `x-goog-request-id`, `Content-Length`, `Server`, `Cache-Control`, `Vary`, `Transfer-Encoding`,
`x-swath-replay-error`, `x-guploader-uploadid`, `x-goog-gcs-base-ts`, `Expires`, `Alt-Svc`, `Pragma`), the media
type, a `Content-Length` equal to the body and a padded GMT `Date`, their values not compared; then for a
listing every element or field in order (S3: the top-level element sequence, `Prefix`, `Delimiter`,
`StartAfter`, `EncodingType`, `KeyCount`, `MaxKeys`, `IsTruncated`, `Marker`, each `Contents` field with
`LastModified` as an instant, `CommonPrefixes`; GCS: each item's name, size, `updated` as an instant, then the
metadata below, the prefixes, whether the page is the last) and for an error the S3 `Code` or the GCS error's
first `reason`. Set aside, and only these: request ids, dates, the tokens' values (a token is compared by its
presence and by where it leads; an S3 page's `ContinuationToken` must echo its own side's), and the S3 owner's
values (its element sequence is compared; the replay's are synthetic). GCS item metadata is compared exactly
(`etag`, `generation`, `metageneration`, `storageClass`, `contentType`, `md5Hash`, `crc32c`) when the fixture is
a `swath/gcs-objects/v1` dataset, and held to the synthetic profile otherwise (`-gcs-metadata`, default auto);
`timeCreated` is compared as an instant where the replay lists it (a recording that stored it); `id`,
`selfLink` and the rest are not stored, so not compared. A header outside the classified
set fails the comparison, as it fails the suite. Two S3 checks go beyond the suite's policy: an S3 error is
compared as its whole document, every element's name and text as written in order, `RequestId` and `HostId` by
presence only; and the key-carrying elements of a page (`Key`, `Prefix`, `StartAfter`, `Delimiter`) are
compared as written as well as decoded, so a character reference and the raw character differ.

**Documented differences.** A difference the replay is known to keep is listed apart, with its reason, under
`documented_residuals` (`DOCUMENTED RESIDUAL` in the text), and is neither passed nor failed. Each rule matches
the request's shape and both answers' shapes, never the case name alone, so the same request answered any other
way still fails (`TestDocumentedDifferences`): a mismatch the provider evidence pins for its case, message for
message (`-live-pins`; on a walk, the page that differs ends the walk), for a request of the shape the evidence
captured for that case, answered with the captured provider status and, where the provider refused, its reason
(GCS) or code (S3), and carrying the token the case sends as the replay formed it: for a case that sends an issued
token, one the replay mapped from an earlier answer (a provider token sent unmapped documents nothing,
`TestSourcedPinNeedsAnIssuedToken`). The shape is the case's query template (its literal parameters exactly, each name any name),
and the pins file is read with the evidence beside it (`index.json` and the captures): a pin whose case is not in
the matrix, whose capture is missing, or whose template does not render the captured query over the evidence's run
prefix is an error, so no pin documents by its case label alone (`TestReadPins`,
`TestPinsBindTheRecordedRequest`); S3 ListObjects V1 (`list-type` absent or
not 2) listed by the provider and refused by the replay (V2 only, see Limits); S3 `fetch-owner=true` where the
provider writes `Owner` and the replay writes none (a recording made without owners), the pages otherwise equal;
S3, an issued token with a decoded byte flipped (`s3-07x`), served by the provider as if unaltered and refused by
the replay; GCS `softDeleted=true` (`gcs-11h`) listed by a bucket that keeps soft-deleted objects and refused by
the replay. The live run and the replay of its recording apply the same rules.

### Live S3, 2026-11-07

The recordings of 2026-11-06 (`swath list ... --no-sign-request --sort`, recorded 21:58 to 22:13 UTC), the Go
server at this commit, anonymous path-style requests through the session's proxy, `-parallel 4`, default walk
sizes 7 and 1,000.

| Bucket, region | Scope (newest recorded name) | Matrix cases sent | Matrix cases passed / differing | Walks passed / differing | HTTP requests |
|---|---|---|---|---|---|
| `noaa-normals-pds`, us-east-1 | whole bucket (2026-11-04T13:05Z) | 72 of 97 | 54 / 17 (+1 skipped) | 10 / 0 | 1,906 (with `-live-scripted -max-prefixes 40`: 312 / 25) |
| `giab`, us-east-1 | whole bucket (2026-11-01T19:06Z) | 72 of 97 | 55 / 16 (+1 skipped) | 30 / 2 | 736 |
| `gleif`, eu-central-1 | `data/csv/lei/date=2025-` (2025-12-31) | 44 of 97 | 37 / 7 | 204 / 0 | 3,830 |
| `gleif`, eu-central-1 | `data/xml/rr/date=2024-` (2024-12-31) | 44 of 97 | 37 / 7 | 204 / 0 | 3,836 |

`gleif` gains date partitions every day, so its runs are scoped to closed years. Its partitions hold three
objects each, so the cases that need a directory of six objects or a name that is a byte prefix of another are
not sent there. `s3-07x` (a decoded byte of an issued token flipped) is skipped where either side's token is
not base64 (live S3's own token is skipped on `giab`).

Every difference the first runs found, by matrix case (the same on each bucket where the case was sent). The S3
route now answers twelve of them as live S3 does (The S3 route, above). The S3 rows of
`docs/replay-provider-conformance.md` are `UNMEASURED`.

| Case | Request | Live S3 | Replay at the first runs | Replay now |
|---|---|---|---|---|
| `s3-01c`, `s3-01g` | `start-after=` (empty) | echoes an empty `<StartAfter></StartAfter>` | omits `StartAfter` | as live S3 |
| `s3-05e`, `s3-05f` | `prefix=...a%ZZ`, `prefix=...pct%` (malformed escape) | drops the prefix: lists the bucket from its first key, `<Prefix></Prefix>` | lists the literal prefix (empty) | as live S3 |
| `s3-05i` | `prefix` twice with different values | the first | the last | as live S3 |
| `s3-06m` | `max-keys=1&max-keys=2` | the first | the last | as live S3 |
| `s3-06k` | `max-keys=+2` (raw `+`) | `400 InvalidArgument` (the `+` read as a space) | accepts the sign: 2 | as live S3 |
| `s3-07w` | `continuation-token=` (empty) | `400 InvalidArgument` ("The continuation token provided is incorrect") | blank is absent: the first page | as live S3 |
| `s3-07r`, `s3-07s` | an issued token with its last character changed, or cut by four | `400 InvalidArgument` | the altered `v2:` token still decodes to a position: a page | as live S3 |
| `s3-07k` | a token from a flat walk sent with `delimiter=/` | resumes at the token's key and lists the common prefix that holds it again | resumes after that whole prefix | as live S3 |
| `s3-08d` | `encoding-type=bogus` | `400 InvalidArgument` | ignored | as live S3 |
| `s3-12e` | `marker` with `list-type=2` | `400 InvalidArgument` ("Marker unsupported ... in list-type=2") | ignored | as live S3 |
| `s3-12a`, `s3-12b`, `s3-12c` | ListObjects V1 (no `list-type`, or `list-type=3`) | a V1 listing | `400` | `400` (V2 only, see Limits) |
| walks of `giab` | `encoding-type=url` over a key with spaces | a space is `+` (`Karyotype+Report+-+...`) | `%20` | as live S3 |
| scripted, 22 requests | `start-after` holding U+0000 | `400 InvalidArgument` ("cannot include Null") | a listing | as live S3 |
| scripted, 2 requests | `start-after` with a byte that is not UTF-8 (`%FF`) | read as U+FFFD (echoed `%EF%BF%BD`) | the byte as sent | as live S3 |
| `s3-11a`, scripted `fetch-owner=true` | `fetch-owner=true` | `Owner` with `ID`, before `StorageClass` | no `Owner` | no `Owner` over a recording made without owners; over one made with them, as live S3 (below) |
| scripted `fetch-owner=TRUE delimiter` | `fetch-owner=TRUE` | no `Owner` (only `true` in lower case asks for it) | `Owner`, over a recording made with owners | as live S3 (below) |

Rerun after the change (same recordings and flags): `noaa-normals-pds` with `-live-scripted -max-prefixes 40`,
provider cases 67 passed and 5 differing, scripted 336 / 1, walks 10 / 0 (1,908 requests); `giab` 67 / 4, walks
32 / 0 (766 requests). The differences left are ListObjects V1 (`s3-12a` to `s3-12c`), `fetch-owner` over a
recording without owners (`s3-11a` and its scripted twin), and `s3-07x`, now sent because the replay's token is
base64 as a whole: live S3 serves its own token with the last decoded byte flipped as if unaltered, while the
replay refuses every alteration (The S3 route).

Control characters and the error document (2026-11-07): live S3 wrote a control character in listing text as
a character reference and an error as its own document (The S3 route); the replay wrote the raw byte and an
error document with `Resource`. The comparison above was extended to see both, and the scripted matrix gained
nine requests that show them (`control characters` in `start-after`, `prefix` and `delimiter`; `error document`
for `max-keys`, `encoding-type`, a NUL in `prefix` and in `start-after`, `marker` and a token). Against the
previous server, `noaa-normals-pds` with `-live-scripted -max-prefixes 40` differed on 58 of 428 (provider 17,
scripted 41, every S3 error and the control characters); after the change, provider cases 67 passed and 5
differing, scripted 345 / 1, walks 10 / 0 (1,926 requests), the same six differences as above.

Owner's place (2026-11-07): over a recording of `noaa-normals-pds` made with `--fetch-owner` the same day, the
replay wrote `Owner` after `StorageClass`, and `s3-11a` and the scripted `fetch-owner=true` differed on the
`Contents` field sequence; it now writes it before, as live S3 does (The S3 route), and both pass. With
`-live-scripted -max-prefixes 40`: provider cases 68 passed and 4 differing, scripted 345 / 1, walks 10 / 0
(1,926 requests). The scripted `fetch-owner=TRUE` differed as well: live S3 writes `Owner` only for the value
`true` in lower case, where the route took `true` in any case; it now reads the value as live S3 does (The S3
route). Rerun: provider cases 68 passed and 4 differing, scripted 346 / 0, walks 10 / 0 (1,926 requests); the
differences left are ListObjects V1 (`s3-12a` to `s3-12c`) and `s3-07x`.

### Live GCS, 2026-11-07

Four public buckets recorded with the Go lister the same day (native `swath/gcs-objects/v1` datasets, anonymous
`--gcs-anonymous`, through `faultproxy -record`; below), each served by the Go server and compared with live
`storage.googleapis.com` inside the recorded prefix, `-live-pins` set to the evidence archive's
`gcs/replay-expectations.json`, item metadata compared exactly (native).

| Bucket and scope (recorded) | Objects | Matrix cases sent | Matrix passed / differing (of them pinned) | Walks passed / differing | HTTP requests |
|---|---|---|---|---|---|
| `cloud-samples-data`, whole bucket (13:09Z) | 164,869 | 87 of 110 | 56 / 30 (29) | 166 / 0 | 2,024 |
| `gcp-public-data-sentinel-2`, `L2/tiles/32/T/NS/` (13:09Z) | 284,292 | 86 of 110 | 54 / 31 (29) | 202 / 0 | 7,274 |
| `gcp-public-data-landsat`, `LC08/01/001/` (13:10Z) | 314,390 | 86 of 110 | 54 / 31 (30) | 124 / 0 | 1,942 |
| `gcp-public-data-nexrad-l2`, `1991/06/` (13:11Z) | 12 | 83 of 110 (r/ on a reduced shape) | 54 / 28 (27) | 36 / 0 | 278 |

A fifth bucket, recorded at 22:25Z by a lister that lists `timeCreated` (the dataset holds `time_created`;
`swath list gs://gatk-best-practices --gcs-anonymous`, without the proxy), compared the same way with the walks
comparing `timeCreated` as an instant: `gatk-best-practices`, whole bucket, 1,954 objects, 86 of 110 cases sent,
54 / 31 (30), walks 48 / 0, 1,478 requests; scope `cnn-h38/` (6 objects), 26 / 24 (23), walks 4 / 0. 120 of its
objects have metageneration 2 and a `timeCreated` before `updated`. Walking the whole bucket on the replay's S3
route (`list-type=2`, `max-keys=1000`, `fetch-owner=true`, flat and with `/`) against Cloud Storage's XML API
for the same bucket, every `Contents` field agreed on all 1,954 objects (`LastModified` included; the server
before `time_created` was read sent `updated` and differed on those 120), and the GCS route's `timeCreated`
and `updated` agreed with the JSON API's on every object.

`gcs-07v` (a decoded byte of an issued token flipped) is skipped on every run: the replay's `gcs1.` token is not
base64 as a whole, the pinned "no analogue" of the evidence. On `nexrad-l2` no directory holds six objects, so a
directory of three stands in for `r/`; those cases are listed as run on a reduced shape (`REDUCED SHAPE`).

Pinned differences (counted as differing in the table above; the comparison now lists them as documented):
every difference but the ones below reproduces, message for message, the mismatch the
owned-bucket evidence pins for its case (`docs/replay-provider-conformance.md`): `gcs-03p` (`includeTrailingDelimiter`
refused), `gcs-05e` to `gcs-05i` (malformed escapes, invalid UTF-8 and duplicate parameters refused; live GCS
serves them), `gcs-06a`, `06b`, `06l`, `06m` (`maxResults` 0, leading zeros and duplicates refused), `gcs-06e`
(live GCS returns 1,001 items for `maxResults=1001`; the replay caps at 1,000), `gcs-06f`, `06g`, `06i`, `06j`, `06k`,
`06n`, `11c` (error reason `invalid` where live says `invalidParameter`), `gcs-07i`, `07k`, `07l`, `07o`, `07t`, `07u`
(a token sent with another scope: refused by the replay, honoured by GCS), `gcs-07r`, `07s` (an altered token: GCS
refuses it, the replay decodes it), `gcs-11e`, `11f`, `11g`, `11i` (`fields`, `versions`, `matchGlob`,
`includeFoldersAsPrefixes` refused). The differences the evidence does not pin:

| Case | Bucket | Request | Live GCS | Replay |
|---|---|---|---|---|
| `gcs-11h` | all five | `softDeleted=true` | `200`, a listing (these buckets keep soft-deleted objects; the evidence bucket had soft delete disabled and answered `400`) | `400 invalid`, `feature_softDeleted` |
| `gcs-07r` | sentinel-2 | an issued token with its last character changed | accepted (the changed character only touched unused bits) and resumes at the token's position | the altered `gcs1.` token decodes to another position |

### Live capture

`bench/faultproxy -record FILE` writes every exchange it forwards as a JSON line (the campaign's raw-exchange
shape: method, URL, raw path and query, request headers with credentials redacted, status, response headers, body,
timestamps). Each exchange is written before its answer is released to the client, so the answer that issued a
page token precedes, in the file and in sequence numbers, every request that carries that token
(`TestRecordPersistsBeforeRelease`). The client is sent the provider's body with its Content-Type set and never
sniffed (`X-Content-Type-Options: nosniff`; `application/octet-stream` where the provider named none, `text/plain`
for an HTML type), while the file keeps the provider's headers as sent (`TestRecordReleasesWithAContentType`). On SIGINT or SIGTERM the proxy stops accepting connections and waits up to
`-drain` (30 s) for the requests in flight to be answered and recorded, a reset included, before it closes the
file (`TestRecordDrainsOnShutdown`); an exchange still in flight then is reported and the proxy exits 1. Running the lister through it against the provider records the listing and the client's whole
request stream in one go; `replaydiff -capture FILE` then replays every captured request, in order, against the Go
server serving the dataset that same run wrote, maps each captured token to the token the Go server issued for the
same request, and compares each answer with the captured one under the policy above (a gzip-coded captured body is
decoded first):

```bash
go build -o /tmp/faultproxy ./bench/faultproxy
/tmp/faultproxy -target https://s3.us-east-1.amazonaws.com -listen 127.0.0.1:19401 -record /tmp/capture.jsonl &
./swath list s3://noaa-normals-pds/ -o /tmp/rec --anonymous --endpoint http://127.0.0.1:19401 --region us-east-1 --workers 16
kill -INT %1
/tmp/replaydiff -capture /tmp/capture.jsonl -go ./swath-replay -fixture /tmp/rec -bucket noaa-normals-pds
# GCS: -target https://storage.googleapis.com and swath list gs://B/P --gcs-anonymous --gcs-endpoint http://127.0.0.1:19401/storage/v1/
```

The capture is read and replayed one exchange at a time (`TestCaptureStreams`): one exchange, the token map and
the report's details stay in memory. The report counts every exchange exactly but details at most
`-capture-max-details` failing exchanges and as many skipped ones, each quoting at most 1 KiB of its request path and
query; the passing ones are only counted, and the report says how many failures and skips it does not show
(`failures_not_shown`, `skips_not_shown`, and `N more failures not shown` in the text;
`TestCaptureReportIsBounded`). Because the recorder writes an issuing answer before any request that uses its token,
a token no earlier exchange issued fails its request (the capture is out of order or starts mid-walk), and the rest
of that walk is skipped naming it, as it is after a page whose replay failed (`TestCaptureReplaysAClientStream`,
`TestCaptureFailedIssuerSkipsItsWalk`). Every size is bounded, and a bound that is reached is a failure of that
exchange, never a cut body (`TestRecordBodyLimit`, `TestCaptureLimits`):

| Flag | Default | Bounds | When exceeded |
|---|---|---|---|
| `faultproxy -record-max-body` | 16 MiB | the answer body the proxy buffers and records (the proxy reads no further than one byte past it) | the client is answered `502` with `X-Faulttest-Body-Limit`, the exchange is recorded with `limit_exceeded` and no body, logged, and the proxy exits 1 at shutdown with the count |
| `replaydiff -capture-max-body` | 16 MiB | a captured body as recorded (a `body_base64` body by its decoded length); a capture line may be six times this plus 1 MiB (JSON escaping) | the exchange (or the line, read to its end) fails and the replay goes on |
| `replaydiff -capture-max-decoded` | 64 MiB | a body once its content coding (gzip) is undone | the exchange fails |
| `replaydiff -capture-max-tokens` | 1,048,576 | the page tokens remembered for the walks still to be replayed (one entry per issuing request) | the replay fails at that exchange and stops |
| `replaydiff -capture-max-details` | 1,000 | the failing exchanges, and apart from them the skipped ones, the report details | the rest are counted, not detailed, and the report says how many |

An exchange the recorder marked `limit_exceeded` fails in the replay as well.

On 2026-11-07 (the Go lister's own requests, 16 workers, ranges split by the lister):

| Capture | Exchanges | Passed | Differing |
|---|---|---|---|
| `s3://noaa-normals-pds` (whole bucket) | 233 | 233 | 0 |
| `gs://cloud-samples-data` (whole bucket) | 274 | 274 | 0 |
| `gs://gcp-public-data-sentinel-2/L2/tiles/32/T/NS/` | 417 | 417 | 0 |
| `gs://gcp-public-data-landsat/LC08/01/001/` | 509 | 509 | 0 |
| `gs://gcp-public-data-nexrad-l2/1991/06/` | 1 | 1 | 0 |

The first replay of `cloud-samples-data` found the one difference this check has found: an object stored without a
content type is listed by GCS without `contentType`, which the native rendering filled with
`application/octet-stream`; the native rendering now omits it (`TestNativeNullContentTypeIsOmitted`).

### The recorded live matrix

A client's own recording holds only ordinary requests. The edge cases (an empty `start-after=`, malformed
escapes, duplicated parameters, control characters, altered and invented tokens, the error documents) are the
live matrix's, so the live matrix is recorded too: the live mode is pointed at the recording proxy with
`-live-record`, and the recording is replayed with `-capture`:

```bash
/tmp/faultproxy -target https://s3.us-east-1.amazonaws.com -listen 127.0.0.1:19401 -record /tmp/live-matrix.jsonl &
/tmp/replaydiff -live-endpoint http://127.0.0.1:19401 -live-record -protocol s3 -go ./swath-replay \
  -fixture /data/rec/noaa-normals-pds/dataset/data -bucket noaa-normals-pds -live-scripted -max-prefixes 40
kill -INT %1
/tmp/replaydiff -capture /tmp/live-matrix.jsonl -go ./swath-replay -fixture /data/rec/noaa-normals-pds/dataset/data \
  -bucket noaa-normals-pds -report /tmp/replay.json
# GCS: -target https://storage.googleapis.com, -protocol gcs, -live-prefix P and -live-pins on both runs
```

With `-live-record` each request to the provider carries annotations as `X-Record-*` headers, which the proxy
writes to the exchange (`annotations`) and removes before forwarding, so the provider is asked exactly what it
would be asked directly (`TestRecordAnnotations`): `X-Record-Case`, the matrix case; `X-Record-Token`, how a token
the driver formed itself was made: `verbatim` (the tampered constant, a scripted invented token: issued by no
answer, sent to the replay as it was) or the alteration (`flip`, `truncate`, `flip-decoded`) and the issued token
it was applied to; and `X-Record-Token-From`, the walk a token case takes its token from. An annotation is a claim
the replay holds to the captured request, never acted on unchecked: the alteration applied to the named issued
token must give exactly the token the request carried, and a `verbatim` token must be one no earlier captured answer
issued; otherwise the exchange fails (`annotation does not match the captured token`,
`TestCaptureMutationAnnotationIsVerified`). The replay sends an altered token as the Go server's token for the same
issued one, altered the same way, and applies `-live-pins` to the annotated case only where the request and answers
are the pinned ones (above). The driver also drops the `X-Content-Type-Options` the proxy adds to every answer it releases
(the file keeps the provider's headers as sent).

A provider may issue the same token from requests of different scope (a GCS token names the object its page ends
at, whatever the query asked), while the Go server's tokens carry their scope and are refused with another. The
replay therefore keeps one Go token per issuing request. A request annotated with its source walk sends that walk's
Go token, or fails when that walk issued no such token, whatever its own query was issued; only a request without a
source continues the token an earlier request of its own query was issued, else one of the same query but its page
size (`maxResults`, `max-keys`: a client may change it mid-walk, and both token contracts accept the same scope with
another page size; `TestCaptureWalkChangesPageSize`), its own case's where several were.
Where the candidates would send different Go tokens the request fails as ambiguous; none is chosen by recency
(`TestCaptureTokenIssuedByTwoScopes`, `TestCaptureTokenFromNamedWalk`, `TestTokenMapLookup`). A live run sent through a recorder replays with the
live run's verdicts: no failure the live run did not have, every documented difference documented again, and a
second replay gives the same report (`TestLiveMatrixRecordedReplaysOffline`,
`TestPinnedDifferencesAreDocumentedInBoth`).

On 2026-11-07 (fixtures recorded the same day: `noaa-normals-pds` with the Java lister, `--no-sign-request
--sort`, no owners; the `gcp-public-data-landsat` prefix with the Go lister, a native dataset; the Go server at
this commit, `-parallel 4`, default walk sizes 7 and 1,000, anonymous requests through the session's proxy; each
recording replayed twice, the two reports identical apart from their timing):

| Recording | Exchanges | Passed | Documented | Failed |
|---|---|---|---|---|
| `s3://noaa-normals-pds`, live matrix with `-live-scripted -max-prefixes 40` | 963 | 957 | 6 | 0 |
| `gs://gcp-public-data-landsat/LC08/01/001/`, live matrix with `-live-pins` | 971 | 940 | 31 | 0 |

The six S3 documented differences are ListObjects V1 (`s3-12a`, `s3-12b`, `s3-12c`), `fetch-owner=true` over a
recording made without owners (`s3-11a` and its scripted twin) and `s3-07x`; the 31 GCS ones are the 30 cases whose
mismatch the evidence pins (`gcs-06b` among them, a walk) and `gcs-11h`. The live runs that made the recordings
gave the same verdicts (S3 422 compared, 422 passed; GCS 178 compared, 178 passed, `gcs-07v` not sent: the replay's
token is not base64 as a whole). On the GCS route the driver's own scripted matrix (`-live-scripted`) is not part
of the live check: it was written against the Java server, and 48 of its requests differ from live GCS, the same
48 in the live run and in the replay of its recording (parameters live GCS accepts and the replay refuses, the
error reason `invalid` where GCS writes `invalidParameter` or `required`, and root listings outside the recorded
scope).

Recordings of real buckets are never committed: they hold a third party's listing. They are made locally (a
live run through the proxy) and replayed locally against the dataset of the same run. The recorded flows CI runs
use inputs of the repository alone: the fake providers and in-test recorders of `bench/replaydiff` and
`bench/faultproxy` (the tests named above), and the owned-bucket provider evidence committed under `docs/`
(The GCS evidence replay, below; the Azure evidence replay).

## Measurements

Configuration, unless a table says otherwise. Host: a 4 vCPU session container (Intel Xeon @ 2.10 GHz) that
other jobs share, 15 GiB RAM, Linux 6.18, quiet during the runs; **4 vCPU shared host, not the corridor's eight
isolated cores**, and the
client runs on the same four vCPUs over loopback, so these numbers are the pair's, not the server's alone. The
server: this module at `56abc8d` (the serving code this page describes), `./build.sh`, GOMAXPROCS 4, the window
cache's defaults, `--serving-mode sorted --parquet-connections 8 --max-concurrent-requests 1024`, nothing else
running. Three rounds on 2026-11-07, 19:17 to 19:52 UTC, the Java server (see
[the comparison](internals/replay-java.md#measurements-both-servers)) measured back to back with this one for each
configuration; every figure below is the median of the three runs, with the range of the three where a cell gives
one. Fixtures, page cache warm: `synthetic-1m/sorted-rolled` (synthetic, 1,000,000 keys, 6 parts, 110 row groups,
22.7 MB, written by `bench/replay/make-fixtures.sh`) and the `noaa-gk2a-pds` recording (a public bucket's listing,
18,483,603 keys, 2 parts, recorded 2026-11-07 18:30 to 18:33 UTC; not in the repository). Run-to-run spread on
this host: the three throughputs of a configuration spread by 0.6% to 25% (range over median), the widest for
16 walks over the recording (25%) and 64 walks over the synthetic fixture (20%, one slower first round), so
differences of that size between single runs mean nothing.

### Throughput and memory

Each run: `swath-replay serve --fixture F --bucket B --port P --metrics-port M --serving-mode sorted
--parquet-connections 8 --max-concurrent-requests 1024`, then `replayload -endpoint http://127.0.0.1:P -bucket B
-fixture F -walks W -max-keys 1000 -warmup 10s -duration D -metrics http://127.0.0.1:M/metrics` (D 30 s for 16
walks, 50 s for 64), with the server's `VmRSS` read from `/proc/PID/status` every second from the client's start.
RSS mean is the median over the three runs of each run's mean; RSS max is the highest sample of the three runs.

| Fixture | Walks | Object rows/s, median (range) | Request p50 / p99 ms | RSS mean / max MB | Reader peak |
|---|---|---|---|---|---|
| synthetic-1m rolled | 16 | 2,407,056 (2,261,596 to 2,591,308) | 5.3 / 17.2 | 426 / 442 | 8 |
| synthetic-1m rolled | 64 | 3,530,206 (2,852,386 to 3,543,225) | 16.1 / 50.1 | 704 / 719 | 8 |
| noaa-gk2a-pds | 16 | 1,450,953 (1,227,179 to 1,592,280) | 8.9 / 29.2 | 883 / 973 | 8 |
| noaa-gk2a-pds | 64 | 2,144,642 (1,931,133 to 2,234,807) | 25.7 / 82.6 | 918 / 954 | 8 |

Under load the server's resident memory averaged 426 to 918 MB (at most 973 MB over any sample) and the reader
stayed at its 8 slots in every run. It grew with the walks over the synthetic fixture (426 MB at 16 walks, 704 MB
at 64) and hardly at all over the recording (883 MB, 918 MB), whose 18.5 million keys the server does not hold.

The remaining measurements were made on 2026-11-07 between 12:50 and 12:58 UTC, with the server as recorded in
`b96582d` and the recording as it was then (18,472,015 keys), and were not repeated in the later runs above, so
they carry that date and commit. Idle, the server holds 14 MB and answers `/healthz` 0.22 s after it starts on
either fixture. It serves a walk's pages from its window cache (above): at 64 walks over the recording it read
the fixture 7,617 times (12,821 rows a fill on average, read-aheads included) for 108,248 pages answered from
windows. Its memory is the cache (2.39 million rows held over the recording, 2.29 million over the synthetic
fixture, whose walks overlap) and the in-flight responses (`response.bytes.peak` 20 to 24 MB at 64 walks), not
the fixture's object count. With the cache off (`--prefetch-window-rows 0`) the same 64-walk synthetic run
served 756,214 rows/s in 45 MB, every page decoded from the fixture. With 256 walks the server served
3,582,394 rows/s (25 s, 680 MB, `parquet.queries.peak` 8).

Where the server's CPU goes at 64 walks over the synthetic fixture (same date and commit as the paragraph above;
`SWATH_CPUPROFILE`, 40 s, 2.4 million rows a second): rendering the S3 document 40% (XML escaping 15%), slicing
pages out of windows 29% (unpacking rows into objects 11%), the response write 13% (mostly the `write` system
call), fixture reads 5%, the collector under 1%.

### Overruns at 64 walks

`--inject-latency worker_page=50ms,pivot_probe=50ms,structure_probe=50ms`, 64 walks, 30 s, synthetic-1m rolled,
`replayload ... -walks 64 -max-keys 1000 -duration 30s`, medians of the three rounds of 2026-11-07 above. 64 walks at
50 ms ask for 1,280 pages a second.

| max-keys | Object rows/s, median (range) | Request p50 / p99 ms | Requests | `inject.overrun`, median (range) |
|---|---|---|---|---|
| 1000 | 1,228,090 (1,224,054 to 1,231,044) | 51.5 / 58.1 | 36,859 | 29 (12 to 43) |

At 1,000-key pages the profile asks for 1.28 million rows a second; with the client on the same four vCPUs the
server falls a little short, overrunning 0.08% of its pages (29 of 36,859; 0.03% to 0.12% over the three runs).
The 100-key run was measured on 2026-11-07 (12:50 to 12:58 UTC, `b96582d`) and not repeated: 124,415 rows/s,
request p50 / p99 51.1 / 56.2 ms, 37,337 requests, no overrun. At 100-key pages (128,000 rows a second) the
server holds the profile (`TestLoadNoOverrunAtSixtyFourWalks` pins this). Check `inject.overrun` against the
request count before trusting a run with a profile.

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

| Injected latency | baseline | storm |
|---|---|---|
| 95 ms | 1.38 s, 251 | 1.34 s, 249 (0) |
| 1,000 ms | 11.26 s, 248 | 35.30 s, 266 (15) |

At 95 ms the listing ends before the throttle window opens at 5 s; 1,000 ms puts the window inside the run.
`KILL_AT=5 PHASE=25 bench/resume_storm.sh noaa-normals-pds 1000 29300 1 OUT 64` (the list SIGKILLed at 5 s,
resumed at once through a proxy throttling 20% of requests until 25 s), list then resume seconds, requests
(throttled):

| shipped | slowstart | off |
|---|---|---|
| 5.1 + 45.4 s, 301 (19) | 5.1 + 45.3 s, 271 (10) | 5.1 + 11.6 s, 398 (80) |

Every run exited 0.

## Limits

- Three routes (S3 `ListObjectsV2`, path-style, the GCS JSON `objects.list` and the Azure flat `List Blobs`
  subset below) and one serving mode (sorted). Version listing, object data and
  authentication are not served.
- Request framing is decided by Go's HTTP layer before any handler runs:
  - A request line it refuses (whitespace or a control byte in the target, a malformed `%` escape (`%zz`, a lone
    `%`, `%uXXXX`) in the path, a target with neither a leading `/` nor a scheme) is answered `400`, and a request
    head over about 132 KiB `431`, with the layer's own `text/plain; charset=utf-8` page and no `Cache-Control`.
  - An absolute target whose authority differs from the `Host` header (`GET http://other.invalid/bucket?...`
    with `Host: localhost`) is served: the layer drops the `Host` header for an absolute target, as RFC 9112
    section 3.2.2 has an origin server ignore it.
  - Up to four CR or LF bytes between a request without a body (a `POST` with `Content-Length: 0`) and the next
    request line on the same connection are skipped and not counted toward the next request's head; with more,
    what follows the fourth is answered as a malformed request line with `400` and the connection is closed (a
    client still sending may see the connection reset).
  - A CRLF before the request line with no request in front of it on the connection is an empty request line:
    `400`, and the connection is closed.
- Timer percentiles are histogram estimates, within 3.1% above the sample they stand for.
- `sort-fixture` holds the capture in memory while it sorts it, about 300 bytes a row, so it suits captures of a
  few million rows.
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
`alt`, `prettyPrint`, `projection`, `maxResults`, the field lengths, then the token.

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

Each item carries `kind`, `bucket`, `name`, `size` (a decimal string), `timeCreated` where stored, `updated`
(RFC 3339 UTC, six fractional digits: the fixture's microseconds exactly), then the metadata fields. Which values they hold depends on the
fixture's declared schema id:

| Field | `swath/gcs-objects/v1` (the lister's GCS datasets) | every other schema (S3, canonical, sort-fixture output, local) |
|---|---|---|
| `name`, `size`, `updated` | exact | mapped: the same values (a null size is `0`, a null time the epoch) |
| `etag` | exact (`swath-gcs-synthetic-v1` when null) | synthetic: `swath-gcs-synthetic-v1` (an S3 ETag is not a GCS etag) |
| `generation`, `metageneration` | exact (`1` when null) | synthetic: `1` |
| `storageClass` | exact (`STANDARD` when null) | synthetic: `STANDARD` |
| `contentType` | exact, absent when null (as live GCS lists an object stored without one) | synthetic: `application/octet-stream` |
| `md5Hash`, `crc32c` | exact, absent when null | absent |
| `timeCreated` | exact (six fractional digits, as `updated`), written just before `updated`; absent when null or in a part without the optional `time_created` column (one written before the lister listed it) | absent |
| `acl` | `[]` with `projection=full` | `[]` with `projection=full` |
| `id`, `selfLink`, `mediaLink`, `owner`, `metadata` and the rest | absent (not stored) | absent |

The synthetic values are the replay wire contract's (`gcs` `TestRenderGolden`). The table in
[Item fields by route and recording schema](#item-fields-by-route-and-recording-schema) holds every route's
fields for every schema.

### The GCS evidence replay

`go/internal/replay/evidence` replays the provider evidence archive (`docs/replay-provider-evidence.tar.gz`)
against this server. With `SWATH_REPLAY_FIXTURES=ROOT` it extracts the archive into a temporary directory,
serves the capture corpus (from `ROOT/fixtures/evidence-gcs/sorted` when present, and always from the same
corpus written in the test and sorted by sort-fixture) under the bucket `replay-bucket`, and replays every
`gcs/captures/*.json`: an exchange as captured with the bucket placeholder mapped and a native page token
replaced by the replay's token for the same position (or its tampered, flipped or truncated form), and a walk
re-walked with the replay's own tokens. An SDK run is executed by an official client library, the Go one
(`google.golang.org/api/storage/v1`): pointed at the server with its
endpoint override, anonymous, auto-paginating, one attempt per call. Its requests must carry the captured
scope as decoded parameters (its own `alt=json` and `prettyPrint=false` aside) and identify the Go client,
its answers are compared as a walk, and its own decoded inventory must equal the provider's. Each answer is compared with the provider's under the conformance field policy: status, classified
headers, then the page (items field by field, prefixes, terminal pagination; walks as merged inventories with
unbroken token chains) or the error envelope's first reason. Each outcome must be its pin in
`gcs/replay-expectations.json` (with `docs/replay-provider-expectations.json` applied) or better: a pinned
mismatch must be reproduced as the whole message (only a replay token's body after a pin ending at the token's
prefix is open), and a pinned mismatch that now matches is logged as an improvement. `SWATH_PROVIDER_REPORT=DIR` writes the per-capture
outcomes to `DIR/<fixture>/gcs-outcomes.json`.
`scripts/provider-conformance/evidence.py go-receipts` runs this replay and the Azure one at a clean commit and
writes a `producer: go` receipt for each capture they match (see
[Go receipts](../../docs/replay-provider-conformance.md#go-receipts)).

```bash
SWATH_REPLAY_FIXTURES=/data/replay SWATH_PROVIDER_REPORT=/tmp/report go test -v ./internal/replay/evidence
```

Today 77 of the 110 GCS captures match, the 77 pinned `match`; the other 33 meet their pinned mismatches.

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
or a format-1 dataset written unsorted), the smoke sorts it with the image's own `sort-fixture` into a temporary
directory and serves that.

## Continuous integration

The Go jobs of `.github/workflows/ci.yml` each always run and do their work only when their inputs changed
(or on a manual run); `ci-gate` needs all of them. `go-lint`, `go-race` and `go-nonrace` are the tests
([testing.md](testing.md)); two concern the replay:

- `go-build` (inputs: `go/`, the workflow, `Dockerfile.replay-go` and its ignore file, the replay smoke
  script, the race shard table): the static builds of `swath` and `swath-replay` for linux/amd64, linux/arm64,
  darwin/arm64 and windows/amd64 (each linux/amd64 binary's `--version` must name the commit), then the image:
  built for linux/amd64 and linux/arm64 without pushing, the native one loaded as `swath-replay-go:ci` and
  smoked twice on fixtures written in the job, so neither needs network or a recording: the Go
  lister's dataset of a `bench/s3fake` bucket of 2,000 keys (sorted, served as it is), and an unsorted
  capture from `bench/replay/capture` (2,000 objects in descending key order, no stamps), which the server
  refuses, so the smoke sorts it with the image's `sort-fixture` first; the step requires that it did.
- `go-fixtures` (inputs: `go/`, the workflow, the `swath-replay`, `swath-core` and `swath-model` Gradle modules,
  the shared Gradle build and wrapper (`build.gradle.kts`, `settings.gradle.kts`, `gradle.properties`,
  `gradle/`, `build-logic/`, `gradlew`), `docs/replay-provider-evidence.tar.gz`,
  `docs/replay-provider-expectations.json`, the replay smoke script): JDK 25,
  `./gradlew :swath-replay:installDist`, `bench/replay/make-fixtures.sh` into the runner's temporary
  directory, then the gated tests named in the step's `FIXTURE_PACKAGES` and `FIXTURE_TESTS` with
  `SWATH_REPLAY_FIXTURES` set: the store's fixture tests (`internal/replay/store`), the GCS
  evidence replay (`internal/replay/evidence`) and the Azure evidence replay (`internal/replay/azure`
  `TestAzureEvidenceReplay`, on the `evidence-azure/capture` it sorts itself; serving the
  manifest from memory instead fails the step). Every named test must run and pass: one that skips, or a
  name that matches no test, fails the step. A new gated test is added to those two lists.
