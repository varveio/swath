# The Go replay server and the Java one

This page holds every comparison between the Go `swath-replay` ([the user page](../replay.md)) and the Java
`swath-replay serve` it was ported from ([`../../../docs/swath-replay.md`](../../../docs/swath-replay.md)): what
is the same, what the Java server has that this one does not, where the two answer differently and why, how the
differential driver holds the Go server to the Java one, and the Java side of the measurements. The user page
describes the Go server in its own terms; this page goes with the Java server, and when the Java server is
deleted it is deleted whole, with the pointers to it in [the user page](../replay.md),
[the internals page](replay.md) and [../testing.md](../testing.md).

## What is the same

The Go server is a drop-in for the Java `swath-replay serve`: the same flags, the same startup line, the same
metrics endpoint and the same bytes on the wire for the S3 route, apart from the divergences below.
[The internals page](replay.md) says what "the same" covers and how it is checked.

- **Flags.** `serve` takes the Java launcher's flag names and defaults, in either `--flag value` or
  `--flag=value` form, with or without the `serve` command word (`cmd/swath-replay` `TestFlagSpellings`).
  `--serving-mode duckdb` is accepted by name and refused with the reason; the Java server serves it.
- **System properties.** The Java server reads some settings from system properties (through
  `SWATH_REPLAY_OPTS` or `JAVA_TOOL_OPTIONS`) that the Go server takes as flags or environment:

  | Java system property (default) | Go equivalent (default) |
  |---|---|
  | `swath.replay.prefetch.window-rows` (`12500`) | `--prefetch-window-rows` (`12500`) |
  | `swath.replay.prefetch.max-windows` (`96`) | `--prefetch-max-windows` (`192`) |
  | `swath.replay.prefetch.enabled` (`true`) | `0` for either prefetch flag turns the cache off |
  | `swath.replay.slow-request-log-ms` | `--slow-request-log-ms`, or `SWATH_REPLAY_SLOW_REQUEST_LOG_MS` |

  The two window caches use the same window size and publish the same `prefetch.*` meters; the Java one does not
  read ahead (a Go read-ahead is counted as a fill, not as a miss).
- **The startup line** is the Java launcher's `key=value` line with its keys in its order, and the same values
  for the same flags and fixture (`cmd/swath-replay` `TestServeEndToEnd`).
- **The metrics port** serves the same three paths (`/healthz`, `/metrics`, `/runtime-attestation`) and the same
  schema-2 envelope, `{schema_version, serving_mode, serving, uptime_ms, sampled_at_epoch_ms, meters}`, with the
  meters sorted by name and then by the tag list, the order the Java server's snapshot uses. A timer's maximum
  and percentiles and a distribution's maximum follow the Java server's registry, Micrometer's
  `SimpleMeterRegistry` in cumulative mode (a one-minute expiry, three buffers; `metrics`
  `TestWindowsFollowTheJavaRegistry`), and `index.load.latency` and `sortfixture.build.latency` publish no
  percentiles (`null`) on both. The percentiles are histogram estimates on both servers, within 3.1% here and
  coarser at the Java registry's default precision, so the two servers' `p50_ms` and `p99_ms` agree in window,
  not in digits.
- **The slow-request log line** has the Java server's message and fields, without its logger's time and level
  prefix (`server` `TestSlowRequestLogLine`).
- **Request handling the Java server's HTTP layer decides**: the request-head limits (8 KiB with the S3 route
  alone, 64 KiB with a native route; `server` `TestRequestHeadLimits`, each boundary measured on the Java
  server), the request targets refused before routing (`TestRejectsAmbiguousTargets`, each rule established by
  sending the raw target to the Java server), and a declared request body: answered as without it, the
  connection then closed instead of drained.
- **A client that disconnects** while its page is read does not cancel the read on either server: the page is
  counted as served and only its write fails (`TestClientGoneFinishesThePage`).
- **The Azure route's markers** are the Java server's bytes for the same fixture identity (`azure`
  `TestMarkerGolden`), and its bodies the Java server's for the same rows (`TestRenderListGolden`).
- **The GCS route's synthetic item metadata** is the wire contract's, so both servers answer a fixture that is
  not a `swath/gcs-objects/v1` dataset identically; the Java server does not read `swath/gcs-objects/v1`.
  `gcs1.` tokens are the same bytes for the same state (`TestRenderGolden` checks Java tokens).
- **The S3-shaped schemas.** `swath/canonical` is the capture and recording schema of the Java tools, and the
  schema the Java server reads; the S3-shaped schemas' answers on every route are the Java server's
  (`server` `TestEmptyAndNullStringsEndToEnd`).
- **The latency profile grammar** reads its numbers in the Java floating-point grammar (`1.5e3ms`, `2dms`,
  `0x1p0s`; `latency` `TestNumberGrammar`), and a request draws the same jitter from either server, except a
  continuation page, whose key hashes the token text (`s3` `TestLatencyKeyGolden`).

## The meters, side by side

The Java server's meters are documented in
[`../../../docs/swath-replay.md`](../../../docs/swath-replay.md#metrics-and-tuning). A scrape after the same
requests lists the same meters with the same types and tags on either server, apart from the rows below
(`server` `TestMeterSetMatchesTheContract` compares the Go set with a Java scrape after the same mix,
`testdata/meters.txt`). Every meter of the user page's table not named here is the same on both.

| Meter | Go server | Java server |
|---|---|---|
| `serving.fallback{reason}` | absent: there is no other serving mode to fall back to, and a fixture sorted serving refuses ends startup with the reason | with `--serving-mode sorted`, a fixture sorted serving refuses also ends startup with the reason, and nothing is recorded |
| `serving.refused{reason}` | `row_group_disorder` counted for every read that finds it, a range read or a rollup | counted for the rollup's only |
| `parquet.query.*` | a window cache fill is one read; a read answered from a window is none | the same |
| `parquet.queries.in_flight`, `parquet.queries.peak` | one pool serves every read, so the peak is at most `--parquet-connections` | a pool per file and per read kind, so its peak can exceed the flag (46 against 8 in the measurements below) |
| `delimiter.skipscan.row_group_opens`, `.decoded_key_rows` | a key cursor stays open on its group for the whole rollup and decodes whole key pages, so `row_group_opens` is lower and `decoded_key_rows` higher for the same rollup | higher `row_group_opens` and lower `decoded_key_rows` for the same rollup |
| `delimiter.reader_pool.open.latency`, `delimiter.reader_pool.readers_opened` | absent: the Go reader has no reader pools; every part is read through one descriptor cache, `handle_cache.opens`, `.evictions`, `.waits`, `.open`, `.peak`, `.limit` | a row-group reader pool per file, opened on its first rollup; no `handle_cache.*` |
| `sort.progress`, `sort.steal_reason{outcome,reason}` | absent: `sort-fixture` sorts in memory | the Java sorter's own scheduling |
| `jvm.*`, `process.*`, `system.*` | absent: read the process from `/proc` and the limits from `/runtime-attestation` | the Java runtime's and the host's meters |

## What the Java server has that this one does not

- **The DuckDB serving mode** (`--serving-mode duckdb`), which serves an unsorted capture; the Go server serves
  sorted fixtures only and refuses the mode with the reason. `sort-fixture` turns a capture into a fixture.
- **The in-process `bench` command**, a token-walk benchmark that starts each serving mode and walks it over
  loopback. `bench/replayload` drives a running server instead ([the user page](../replay.md#with-the-bench-scripts)).
- **The HAR conformance kit** (`swath-replay-conformance`): a HAR of a live S3 listing replayed against the server
  and its canonical XML compared, covering the request shapes the lister sends. It was not ported; the Go
  counterparts are `bench/replaydiff -live-endpoint` (a request matrix against the live provider) and
  `bench/faultproxy -record` with `replaydiff -capture` (a client's whole recorded stream)
  ([the user page](../replay.md#live-conformance)).
- **The `swath_replay_metrics` summary line at shutdown**, logged below the Java launcher's default log level.
  Scrape `/metrics` before stopping the Go server instead.
- **The JVM, process and host meters and the sorter's meters** (table above).
- **Reader pools.** The Java server opens a row-group reader pool per file; the Go server reads every part through
  one descriptor cache bounded by the soft descriptor limit (table above).
- **A bounded `sort-fixture`.** The Java command sorts with the lister's bounded sort library; the Go one holds
  the capture in memory, about 300 bytes a row.

## Where the answers differ, and why

The rule: where the live provider and the Java server disagree, the Go server answers as the live provider does.
Each such answer was probed on the provider and is pinned by a test of the Go route; the differential driver
reports each request it touches as a documented divergence (below). Everywhere else the Go server keeps the
Java server's bytes.

### The S3 route

Probed on 2026-11-07 against a public `us-east-1` bucket, anonymous (`s3` `TestLive*`, `s3/livegolden_test.go`):

| Request or element | Go server (as live S3) | Java server |
|---|---|---|
| the query's form reading | `+` is a space, a parameter with a malformed escape (`%ZZ`, a trailing `%`) is dropped | keeps `+` and malformed escapes as bytes |
| a repeated parameter | the first value | the last value |
| `prefix`, `delimiter`, `start-after` | read as UTF-8, each malformed sequence one U+FFFD | raw bytes |
| an empty or blank `continuation-token`, a blank `encoding-type` | `400 InvalidArgument` | treated as absent |
| `encoding-type` other than `url` | `400 InvalidArgument` | ignored |
| `marker` with `list-type=2` | `400 InvalidArgument` | ignored |
| a NUL in `prefix` or in a used `start-after` | `400 InvalidArgument` | served |
| `max-keys=+2` (raw `+`) | `400 InvalidArgument`, " 2" not an integer | accepts the sign: 2 |
| an empty `start-after` | echoed as `<StartAfter></StartAfter>` | `StartAfter` omitted |
| `encoding-type=url` | a space as `+`, `*` as it is | a space as `%20`, `*` escaped |
| a C0 control character in listing text | a hexadecimal character reference | the raw byte |
| the XML declaration | followed by a newline | no newline |
| the error document | S3's elements and messages | its own: `Resource` in place of S3's elements, other messages for `max-keys` and the method |
| continuation tokens | unpadded base64url of the boundary and a CRC-32; an altered token is refused | `v2:` and base64url of flag and boundary (`v1:` and the key), unchecked; an altered token still decodes to a position |
| a flat walk's token sent with `delimiter=/` | resumes at its key and relists the common prefix that holds it | resumes after that whole prefix |

ListObjects V1 (no `list-type`) and `list-type=3` are refused by both servers, where live S3 serves them. The Go
server refuses a Java token, and for the same boundary issues different bytes (`s3` `TestTokenGolden` records
both); a continuation page's latency jitter hashes the token text, so it differs from the Java server's for the
same walk.

The live runs of 2026-11-07 ([the user page](../replay.md#live-s3-2026-11-07)) found these differences with the
replay answering byte for byte as the Java server does; the "Replay at the first runs" column of that table is
the Java server's answer to each case today.

### The other routes

- **Azure: more than 254 path segments.** The Java server answers a blob name of more than 254 `/`-separated
  segments `500 too_many_segments`; native Azure lists one (the evidence archive's capture `azure-12a`), and so
  does the Go route (`azure` `TestRenderRefusals`).
- **GCS: two refused features of one group.** Both servers check the four boolean features and then `matchGlob`,
  `filter`, `fields` in the same group order, but within a group the Java server takes its set's iteration
  order, which is randomized each time the JVM starts: with two refused features of one group (say
  `versions=true&softDeleted=true` or `fields=x&filter=x`) its `x-swath-replay-error` names either, while the Go
  order is fixed. The status and the envelope are the same either way.

### Request framing

Five request cases are not byte-identical with the Java server, by decision: the first is the Go server's own
error page, and Go's HTTP layer decides the other four before any handler runs ([the user page](../replay.md#limits)
describes the Go behaviour):

- **The error page of a target refused before routing.** Status `400`, `Content-Type:
  text/html;charset=iso-8859-1` and `Cache-Control: must-revalidate,no-cache,no-store` match; the HTML body is
  the Go server's own page naming the problem, not a copy of the Java server's HTTP layer's page.
- **A request line Go's HTTP parser refuses** (whitespace or a control byte in the target, a malformed `%`
  escape in the path, a target with neither a leading `/` nor a scheme, a head over about 132 KiB). Both servers
  answer `400` (`431` for the long head); the Go server with Go's `text/plain; charset=utf-8` page and no
  `Cache-Control`.
- **An absolute target whose authority differs from the `Host` header.** The Java server refuses it with `400`;
  the Go server serves it, because Go's HTTP layer drops the `Host` header for an absolute target (RFC 9112
  section 3.2.2), so the disagreement cannot be seen.
- **More than four CR or LF bytes between a bodiless request and the next request line.** The Java server skips
  them all and answers the next request; Go's parser discards four, answers what follows as a malformed request
  line with `400` and closes the connection. Up to four are skipped by both.
- **A CRLF before the first request line on a connection.** The Java server counts the two bytes toward the head
  (a GET whose head is exactly the limit then gets `431`); Go's parser refuses the empty request line with `400`
  and closes the connection.

## The differential driver against the Java server

`bench/replaydiff` starts the Java launcher and the Go binary on one fixture and compares every answer byte for
byte ([the internals page](replay.md#the-differential-driver) describes the requests and the comparison):

```bash
go build -o /tmp/replaydiff ./bench/replaydiff
/tmp/replaydiff -java $JAVA_ROOT/swath-replay/build/install/swath-replay/bin/swath-replay -go ./swath-replay \
  -fixture /data/sorted -bucket bkt -report /tmp/diff.json     # exit 1 and FAIL lines on a difference
/tmp/replaydiff -protocol azure -walk-limit 300 ...                   # the Azure route, both servers on s3,azure
/tmp/replaydiff -protocol gcs -java $JAVA_ROOT/swath-replay/build/install/swath-replay/bin/swath-replay \
  -go ./swath-replay -fixture /data/sorted -bucket bkt -report /tmp/gcs-diff.json   # the GCS route, on gcs
```

- **S3 divergences.** The driver holds the Go answer to what the Java server's answers imply under the live-S3
  rules (the Java answer to the equivalent request, or the probed refusal), lists each request whose Java answer
  differs as a `DOCUMENTED DIVERGENCE` line with both answers, and follows each server's own tokens through a
  walk, handing the Go server each Java token in the Go form and listing the requests whose answers differ only
  in token bytes.
- **Residuals.** Of the five request-framing cases above, the driver compares the first three on the status and
  the headers the Java server sets, sends the last two and reports what each server answered (`DOCUMENTED
  RESIDUAL` lines and a count of their own, outside the compared, passed and failed counts; a residual request
  that gets no answer fails the run), and says which in each case's name. The absolute-target case is not sent.
- **The GCS route** ([the internals page](replay.md#the-gcs-driver-and-the-evidence-replay)) and **the Azure route**
  ([the internals page](replay.md#the-drivers-azure-section)) compare the request matrix, token or marker walks and a
  seeded random walk, each server following its own tokens.
- **The native GCS rendering.** The Java server does not read `swath/gcs-objects/v1`, so the cross-check on real
  names is the `cloud-samples-data` rows written into the S3 capture columns and sorted by the Java
  `sort-fixture`: `replaydiff -protocol gcs` with both servers on that fixture compared 2,343 requests (matrix
  1,558, walks 285 with `-walk-limit 300`, random 500) with no difference; with the Go server on the native
  dataset (`-go-fixture`) and `-gcs-mask-metadata`, every name, size, time, prefix, status and page boundary
  agreed, and the only differences were the page tokens' bytes (453 pages; each token is bound to its fixture's
  identity) and the 8 token cases that send the Java server's token to the Go server, which refuses a token of
  another fixture (`invalid_page_token`).

## The conformance suite and the evidence

- The live and capture comparisons of [the user page](../replay.md#live-conformance) use the conformance suite's
  field policy, the Java `ProviderExchangeComparator` with its S3 and GCS profile comparators, ported to Go.
- The GCS evidence's SDK captures were made with the Java client library; the evidence replay runs them through
  the Go one (`google.golang.org/api/storage/v1`), and the Azure SDK captures through the Azure SDK for Go.
- `SWATH_PROVIDER_REPORT=DIR` writes `DIR/<fixture>/gcs-outcomes.json` in the shape of the Java suite's
  `-PproviderReport` file.

## Measurements, both servers

The configuration of [the user page](../replay.md#measurements) (a 4 vCPU shared host, the client on the same
vCPUs over loopback, 2026-11-07), with the Java server: swath `main` at `01dd55b` (0.3.3-SNAPSHOT) on Temurin
25.0.5+8 with the launcher's heap defaults and `JAVA_TOOL_OPTIONS` as the host sets it (trust store and proxy
properties only), as `bench/fault_replay.sh` leaves it, its window cache at its defaults. One server at a time,
nothing else running, the two measured back to back for each configuration (12:50 to 12:58 UTC), both with
`--serving-mode sorted --parquet-connections 8 --max-concurrent-requests 1024`. The `synthetic-1m/sorted-rolled`
fixture was written by the Java `sort-fixture` (`bench/replay/make-fixtures.sh`). Runs vary by about 15% from
one to the next on this host.

Throughput and memory (the commands of the user page's "Throughput and memory"):

| Server | Fixture | Walks | Object rows/s | Request p50 / p99 ms | RSS mean / max MB | Reader peak |
|---|---|---|---|---|---|---|
| Go | synthetic-1m rolled | 16 | 1,868,585 | 6.9 / 21.6 | 401 / 402 | 8 |
| Java | synthetic-1m rolled | 16 | 1,261,226 | 10.4 / 32.9 | 1,056 / 1,105 | 16 |
| Go | synthetic-1m rolled | 64 | 2,602,299 | 21.8 / 66.6 | 677 / 678 | 8 |
| Java | synthetic-1m rolled | 64 | 1,876,586 | 29.7 / 90.9 | 1,472 / 1,473 | 46 |
| Go | noaa-gk2a-pds | 16 | 1,224,641 | 10.6 / 33.0 | 887 / 894 | 8 |
| Java | noaa-gk2a-pds | 16 | 738,430 | 16.2 / 83.9 | 1,752 / 1,860 | 10 |
| Go | noaa-gk2a-pds | 64 | 1,811,143 | 30.1 / 96.9 | 900 / 909 | 8 |
| Java | noaa-gk2a-pds | 64 | 1,039,262 | 50.6 / 183.3 | 2,043 / 2,595 | 16 |

The Go server serves 1.4 to 1.7 times the Java server's rows a second here, in about half its memory. Idle, the
Go server holds 14 MB and answers `/healthz` 0.22 s after it starts on either fixture; the Java server holds
about 175 MB and answers after 2.0 to 2.2 s. Both serve a walk's pages from their window caches: at 64 walks over
the recording the Go server read the fixture 7,617 times for 108,248 pages answered from windows, the Java server
4,062 times for 54,840.

Overruns at 64 walks (a 50 ms profile on every shape, 30 s, synthetic-1m rolled, 1,000-key pages):

| Server | Object rows/s | Request p50 / p99 ms | Requests | `inject.overrun` |
|---|---|---|---|---|
| Go | 1,185,970 | 52.2 / 76.6 | 35,611 | 51 |
| Java | 988,981 | 54.5 / 204.3 | 29,719 | 262 |

Both servers fall a little short of the 1.28 million rows a second the profile asks for, the Go server
overrunning 0.14% of its pages and the Java server 0.9%.

The bench scripts (`bench/fault_replay.sh` and `bench/resume_storm.sh` as the user page runs them; wall seconds
and the proxy's requests, throttled in parentheses):

| Injected latency | Server | baseline | storm |
|---|---|---|---|
| 95 ms | Go | 1.38 s, 251 | 1.34 s, 249 (0) |
| 95 ms | Java | 1.98 s, 253 | 1.40 s, 251 (0) |
| 1,000 ms | Go | 11.26 s, 248 | 35.30 s, 266 (15) |
| 1,000 ms | Java | 11.38 s, 244 | 35.37 s, 268 (15) |

| Server | shipped | slowstart | off |
|---|---|---|---|
| Go | 5.1 + 45.4 s, 301 (19) | 5.1 + 45.3 s, 271 (10) | 5.1 + 11.6 s, 398 (80) |
| Java | 5.1 + 45.4 s, 303 (19) | 5.1 + 46.3 s, 274 (10) | 5.1 + 11.6 s, 399 (80) |

The scripts start whichever `swath-replay` comes first on `PATH` after sourcing `$REPLAY_ENV`, pass the same
flags and read `/healthz` and `swath.replay.http.requests` the same way on either server; they behave the same
on both, and nothing in the Go server needed a change for them. `bench/replayload` drives either server.

## CI and the scripts

- The `go` job's image smokes need no Java: both fixtures are written in the job by Go tools.
- The `go-fixtures` job builds the Java distribution (`./gradlew :swath-replay:installDist`), which is why the
  Java `swath-replay`, `swath-core` and `swath-model` modules and the Gradle build are among its inputs:
  `bench/replay/make-fixtures.sh` sorts the synthetic and evidence captures with the Java `sort-fixture`, and
  the store's fixture tier (`TestJavaWrittenFixtures`) compares each fixture's identity with the Java server's.
  The Azure evidence replay sorts the Java-written `evidence-azure/capture` itself.
- `scripts/ci/smoke-replay-container.sh IMAGE FIXTURE` without the third argument, or with `duckdb`, smokes the
  Java replay image; with `sorted` it smokes the Go image ([the user page](../replay.md#container-image)). A
  dataset the Java lister wrote without `--sort` is one the Go smoke sorts first.
