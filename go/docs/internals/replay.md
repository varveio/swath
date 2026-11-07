# Replay server internals

How `cmd/swath-replay` is put together, and what holds it to the Java server's wire behaviour. The user's view
is [../replay.md](../replay.md).

## Packages

| Package | Role | Imports |
|---|---|---|
| `cmd/swath-replay` | flags (standard `flag`, the Java launcher's names and defaults), the startup line, signals, opening the fixture; `sort-fixture` as a thin command over `internal/replay/sortfixture` | server, latency, store, memstore, sortfixture |
| `internal/replay/server` | the HTTP server: request-target checks, route classification, the S3, GCS and Azure routes' request flow, admission, response memory, the delay deadline, shutdown, the metrics port and the runtime attestation | s3, gcs, azure, latency, metrics, output, store, sortedparquet (its disorder error) |
| `internal/replay/gcs` | objects.list: request parsing, the pager over `store.Store` (or the GCS schema's columns through `store.Sorted`), `gcs1.` tokens, the JSON page (rendered into an `output.Output`) and error envelope, the request's latency key | s3 (key carries), output, store, sortedparquet |
| `internal/replay/azure` | Azure flat List Blobs: request parsing, the UTF-16-ordered cursor, the pager, markers, the XML (rendered into an `output.Output`) and error bodies, the request's latency key | s3, output, store, latency |
| `internal/replay/s3` | ListObjectsV2: request parsing, the pager, continuation tokens, the XML documents, the request's latency key | output, javanum, store |
| `internal/replay/output` | the response byte budget and the chunked sink every route renders into | – |
| `internal/replay/javanum` | the contract's number grammar: BMP-digit integers, the latency profile's floats, saturating conversion | – |
| `internal/replay/latency` | profiles, jitter, scale, and the provider-neutral request classification | javanum |
| `internal/replay/metrics` | the meter registry (definitions, tagged instances, timer windows) and the schema-2 JSON | – |
| `internal/replay/store` | the serving seam: `Store`, `Object`, `Projection`, `Open` | – |
| `internal/replay/store/memstore` | an in-memory `Store` and the TSV fixture format | store |
| `bench/replaydiff` | the differential driver (a `main` package, never linked into the server) | s3, store, memstore, server (tests) |
| `internal/replay/evidence` | the provider evidence replay (tests only) | server, sortfixture, store |

`internal/replay` imports nothing from the lister's pipeline; the lister imports nothing from it.

## The store seam

A `store.Store` answers byte ranges and nothing else: `Rows(from, fromInclusive, toExclusive, limit,
projection)` returns the first rows of `[from, toExclusive)` in unsigned byte order (nil bounds are open), and
`Projection.Owner` says whether the owner columns are needed. Everything S3 means by a page lives above it, in
the pager, so two stores behind the same pager can only disagree by returning different rows, and the route
code does not change when the store does.

The one exception is `DelimitedRollup`, an optional fast path for the `/` delimiter: the store returns the common
prefixes and the objects directly under the prefix, interleaved in key order, up to `limit + 1` entries, so a
listing of a wide directory costs one pass instead of one read per common prefix. Its contract is that the page
built from it is byte-identical to the range walk's; it may decline (`ok == false`), and the pager walks. The
`suppressPrefixAtOrBeforeFloor` argument is the floor rule: S3 never re-emits a prefix that sorts at or before the
resume point (the client already has it), while a route whose inclusive start offset can land inside a prefix
(GCS) passes false.

`Object`'s fields other than the key are nullable; rendering policy is the route's: a null size is `0`, a null
time the epoch, a null ETag, storage class, checksum or owner field an absent element.

## The pager

`s3.Pager.List` turns a request into ranges:

- The prefix window is `[prefix, PrefixUpper(prefix))`. The upper bound is the 0xFF carry (increment the last
  byte below 0xFF, drop what follows). The same carry is also `Successor(P)`, the least key after every key
  starting with P; the two are separate functions because an all-0xFF input means "no upper bound, read to the
  end" for one and "nothing follows, the listing is over" for the other, and mixing them up either drops keys or
  restarts a finished listing.
- The resume boundary is the token's (exclusive or inclusive at its key) if the request has a non-blank token,
  else `start-after` exclusive; the read starts at whichever of it and the prefix is later.
- Without a delimiter one read of `pageSize + 1` rows answers the page; the extra row is the truncation signal,
  and the token resumes exclusive at the last key returned.
- With a delimiter the store's rollup answers when it accepts. Otherwise the pager walks: it reads batches of
  4,096 rows; an object is emitted; a key with the delimiter after the prefix rolls up to its common prefix P,
  emitted unless it is at or before the request's boundary, and the walk resumes at `Successor(P)` inclusive,
  past P's whole subtree. Short runs of P's keys already in the batch are skipped in place (up to 32), a long
  run reads again from the successor: the threshold changes the number of reads, never the output. After a full
  page one more one-row read decides truncation. A page that ends on a prefix resumes inclusive at its
  successor, one that ends on an object exclusive at it.
- `max-keys=0` is an empty, untruncated page (a malformed token is still an error).

`TestRollupMatchesWalk` holds the fast path to the walk: random keyspaces over `/`, `//`, 0xFF and the bytes
around them, every prefix, start, and page size, the two stores' pages byte-identical and a full token walk
equal to the listing computed directly.

## Tokens

`v2:` + unpadded base64url of a flag byte (0 exclusive, 1 inclusive) and the boundary key; `v1:` + the key
decodes as exclusive. Decoding checks the shape only: there is no signature, because a forged token chooses a
resume point the client could already choose with `start-after`. The encoding is exactly the Java server's, so
a token from either server resumes on the other and the differential driver can compare bodies that contain
them byte for byte (`TestTokenGolden` holds a token the Java server issued).

## Text and the wire

Three conversions decide bytes on the wire, and each was measured against the Java server rather than assumed:

- **The raw query.** The request line's query is read as UTF-8 text, each maximal ill-formed subsequence
  becoming one replacement, then every non-ASCII code point becomes `?`; only `%XX` escapes carry other bytes
  into a parameter (`queryText`, identical to the Java server on 4,000 random byte strings; table
  `TestQueryTextMatchesTable`).
- **Parameters and keys as text.** A percent-decoded parameter (`list-type`, `max-keys`, the token) and an
  unencoded key in the response become text with a different replacement grouping (a truncated sequence before
  a non-continuation byte is one replacement, an encoded surrogate one for all three bytes, a truncated sequence
  at the end one for the rest): `decodeText`, identical to the Java server's decoding on 546,200 generated inputs
  (exhaustive to four bytes over the boundary bytes of every sequence class); table `TestDecodeTextMatchesTable`.
  Go's own decoders group malformed bytes differently, which would change echoed prefixes and rendered keys.
- **Integers and blanks.** `max-keys` accepts a sign and decimal digits of any BMP script, within int32, and
  refuses a digit outside the BMP, because the contract reads the value one UTF-16 code unit at a time and a
  surrogate is no digit (`javanum` keeps the BMP digit runs as a table rather than following Go's Unicode
  tables, `TestDigitTable`); "blank"
  (an absent token, a default `max-keys`) is the Unicode separators except the no-break spaces plus the ASCII
  and information-separator controls.

The XML is written byte by byte rather than marshalled: `encoding/xml` escapes `"` as `&#34;` and control bytes
as character references, and the element order, `&apos;`, ETag quoting as `&quot;`, millisecond `LastModified`
rounded toward the earlier millisecond with the year of era (`+10000`, `0001` for year 0, `TestLastModifiedYears`)
and the `encoding-type=url` grammar are fixed by the wire contract (`TestRenderListGolden` is a page the Java
server answered).

It is written into an `output.Output`, not a slice, so memory is charged before it exists. Each append admits
the same unit the contract's renderer admits (a tag in three parts, an entity, a byte, a number, a value's exact
percent-encoded length, or a percent-encoded value written in one pass into room already charged), so a
response that crosses the per-response cap or the budget is refused at the same byte, with the same reason, as
on the Java server.

## Latency

`latency.Classify(delimiter, maxResults)` is the shape: delimiter present is a structure probe, else at most one
result a pivot probe, else a worker page. It takes provider-neutral inputs so every listing route classifies alike.
`Profile.Delay(shape, commonPrefixes, key)` is the target: the shape's base, plus its per-prefix term times the
prefixes returned, then, with jitter, scaled by `1 + jitter × u` where `u` in [-1, 1) comes from the
SplitMix64-avalanched request key. The S3 key (`Request.LatencyKey`) hashes prefix, delimiter, start-after,
token and max-keys as the Java server does, so a request draws the same delay from either server
(`TestLatencyKeyGolden`); avalanching matters because successive pages of one walk differ only in nearby tokens.

The server applies the target as a deadline from the start of paging: it sleeps for the remainder, or counts an
overrun with the excess when there is none. Parsing reads numbers with the contract's floating-point grammar
(`javanum.ParseDouble`: decimal or hexadecimal, a `d`/`f` suffix), and every conversion and sum saturates
(`TestHugeDelaysSaturate`), because a wrapped delay is negative and a non-positive delay is read as none.

## Admission and shutdown

- Every route goes through the same path: the request-target check, one execution slot, one response permit,
  one `output.Output` charged against one budget, and a write after the slot is released. A GCS or Azure
  request is refused `response_count_exhausted` when slow S3 readers hold every permit
  (`TestRoutesShareTheResponseCeiling`). The GCS and Azure renderers format one entry at a time and write it
  into the sink, so a refusal stops them before the next entry; their append units are an entry, not the
  contract's own (its JSON renderer writes through a buffering generator), so at the exact edge of the cap or
  the budget the refused entry can differ by one.
- The request target is checked first (`targetProblem`), as the contract's HTTP layer checks it before any
  route: an ambiguous or illegal target is a 400 with an HTML page, counted by no meter
  (`TestRejectsAmbiguousTargets`). The rules were established by sending each raw target to the Java server.
- `--max-concurrent-requests` is a channel semaphore, the execution slot: excess requests wait, as they would
  for a full worker pool. It is held through parsing, paging, rendering and the injected delay, and released
  before the response is written.
- The response is held for longer: a response permit (one of twice the request ceiling) and the budget bytes
  backing it are taken when the request is admitted and released when the write returns. So clients that read
  slowly hold responses, not slots, and once twice the ceiling are outstanding the next request is refused
  `response_count_exhausted` instead of waiting (`TestSlowReadersReachTheResponseCeiling`).
- The byte budget follows the contract's credit model (`output`): a reservation of
  `max(4096, 512 + 320 × pageSize)`, capped at `--max-response-bytes`, before paging; a first chunk sized from
  the entries actually returned; each chunk taken from the reservation before the budget is charged again; the
  unused reservation returned when rendering ends, before the delay (`TestSparseResponsesReturnTheirReservation`);
  the chunks released after the write. Refusals are 503s (500 `response_too_large` for the cap) counted in
  `response.admission.refused{reason}`; nothing queues on the budget. The store bounds its own page decodes
  (`--parquet-connections`); the server adds no read permit of its own.
- Writes get a deadline when writing starts (`http.ResponseController.SetWriteDeadline`); `http.Server`'s
  `WriteTimeout` would run from the end of the request headers and so include the injected delay.
- Shutdown cancels one context: queued requests and sleeping delays answer 503 `shutdown` at once, a store read
  sees cancellation, both listeners close, and the store is closed, on its own goroutine, after the last request
  holding it returns. The HTTP stop and that close share one deadline: past it the server logs
  `shutdown_incomplete` and returns, leaving a read that ignores cancellation, or a slow close, to finish on its
  own (`TestUninterruptibleReadDefersStoreClose`, `TestShutdownBoundsABlockingStoreClose`). A close failure is
  logged and returned once (`TestShutdownIsIdempotentAndSurfacesCloseFailureOnce`).
- A store failure is a 500 with no detail, except the typed disorder of a row group, which the contract answers
  `fixture_disordered` with the row group and the part's file name (`TestDisorderedFixtureNamesOnlyThePart`).

## Metrics

A meter is a `metrics.Def` (name, kind, tag keys); a tagged instance is created on first use, and the snapshot
sorts by name, then by the tag list spelled `[tag(k=v), ...]`, the order the Java server's snapshot uses. Adding
a meter is adding a `Def` to `defs.go`; the names and tags are the Java server's, so a dashboard reads either
([replay.md](../replay.md#the-meters) lists the few that differ). The meters the Java server registers at startup
are created when the server starts, so a scrape tells a zero from a meter that is not there.

- **Timers** keep a lifetime count and sum, and a rotating window for the max and percentiles (`window.go`): three
  log-linear histograms (exact below 32 ns, then 32 buckets per power of two), each sample recorded into all
  three, the oldest read and cleared every 40 s. That is the window a Micrometer timer publishes with its
  defaults, so a scrape of either server covers the last 80 to 120 s. A percentile is the upper bound of the
  bucket holding the nearest-rank sample, capped at the window's max. A bounded reservoir of recent samples was
  the first design; it covers a sample count, which at a few thousand requests a second is under a second.
  `Def.NoPercentiles` renders `null` percentiles for the one-off timers the Java server builds without them.
- **Function counters and gauges** (`SetCounter`, `SetGauge`) read a count another package keeps at each
  snapshot: the store's reader-slot counters and the descriptor cache's `FileCacheStats`.
- **The store's measurements** arrive through `store.Meters`, installed by `server.New` when the store is a
  `*store.Sorted` (`storemeters.go`): the slot hold time of each read (`page.read.latency`), each call with its
  rows and failure (`parquet.query.*`), each rollup's `sortedparquet.Stats` (`delimiter.skipscan.*`) and each
  read-time refusal (`serving.refused`). The fixture open's duration and routing entries come from the command
  (`Config.IndexLoad`, `Config.IndexEntries`).
- **Spans.** `http.request.latency` ends where the write begins, and only a page records the `write` stage, as on
  the Java server, whose request meter stops when the response is handed to the write
  (`TestRequestMeterSpans`, `TestAzureErrorHasNoWriteStage`). A page's reads run under a context that a client
  disconnect does not cancel, only shutdown, so a page whose client left is finished and counted as served
  (`TestClientGoneFinishesThePage`).

## The differential driver

`bench/replaydiff` is the oracle for this package. It starts the Java launcher and the Go binary on the same
fixture (free ports, read from the startup lines; ready on `/healthz`), lists the reference to learn the keys,
and sends both servers the same requests:

- the scripted matrix (`s3Matrix`): `max-keys` edges (0, 1, 2, 999, 1000, 1001, 100000) and garbage (`abc`,
  `-1`, blank, signed, out of range, digits of other scripts in and outside the BMP), prefixes at every depth of
  the keys (sampled when there are many) and one that matches nothing, each with no delimiter, `/` and a
  two-byte delimiter taken from the keys; `start-after` before, at, between and after keys and at a prefix and
  its successor; keys that need encoding with and without `encoding-type=url`; `fetch-owner`; malformed, blank,
  foreign and v1 tokens; wrong, escaped and missing buckets; extra path segments; HEAD, POST and DELETE;
- token-dependent requests: a seed page of one entry (and, with `/`, of the first prefix that rolls up to two or
  more entries, so it truncates) whose token is then sent with a `start-after` before, after and blank. When a
  seed issues no token the dependent requests are not sent and are reported as skipped, never as passing;
- raw request targets, sent byte for byte on their own connection: the ones the contract's HTTP layer refuses
  (compared on status, the compared headers and `Cache-Control`, since each server's error page is its own),
  unusual ones it lets through (compared in full), and request lines Go's HTTP parser refuses itself (status
  only, see the user page's limits);
- token walks to the end at each page size (`-walks`, default 1, 7, 1000) over the bucket and each top-level
  prefix, with and without the delimiter, and walks that begin with a `start-after` at, inside and at the
  successor of a few directory prefixes (at the root and inside the parent prefix), carrying the token and the
  `start-after` together on every later page; each follows the reference's tokens and compares every page;
- a seeded random walk (`-random`, `-seed`).

"Identical" is: the same status, the same `Content-Type` and the same value for every `x-amz-*` and
`x-swath-replay-*` header, and the same body bytes. `Date`, `Server` and framing (`Content-Length` against
chunked) are not compared. A difference is reported with the first differing body byte and the bytes around it.
The request set is data (a `request` list and a `comparator`), so another route adds its matrix and, where byte
identity cannot hold, a structural comparator; `-live-endpoint` uses one (leaf elements, tokens masked, error
codes only) to compare the Go server with live S3, where the bucket may have changed since it was recorded.

In the package's own tests the driver compares two Go servers over the same keys, one whose store answers the
`/` rollup and one that declines it (`TestDriverFindsNoDifferenceBetweenRollupAndWalk`), and checks that
token-dependent requests are skipped, not passed, where no token exists (`TestTokenCasesSkipWithoutAToken`).

The driver only sees the shapes a fixture has. The adversarial ones (a giant prefix across many row groups, a
successor that is a real key, high-scalar keys, a wide rollup with trailing plain keys, a multi-byte delimiter,
a rolled multi-part fixture, start-after at and inside a common-prefix boundary) are built in the server's tests
as stamped Parquet fixtures and served through the real store and through memstore's range walk, every page of
every walk compared byte for byte (`sorted_differential_test.go`, the contract's
`SortedServingFullDifferentialTest` case by case).

## Tests

| Contract | Test |
|---|---|
| request parsing and its errors | `s3` `TestParseRequest`, `TestParseRequestErrors`, `TestParseHTTPRoutesByFirstSegment` |
| query and text decoding | `s3` `TestQueryTextMatchesTable`, `TestDecodeTextMatchesTable`, `TestIsBlank`, `TestParseInt32`; `javanum` `TestDigitTable`, `TestParseInt`, `TestParseDouble`, `TestSaturate` |
| pager rules | `s3` `TestListsByPrefixAndExclusiveStartAfter`, `TestTruncatesAndResumesFromToken`, `TestMaxKeysZeroIsEmptyAndNotTruncated`, `TestRollsUpAndResumesPastARolledUpPrefix`, `TestStartAfterFiltersCommonPrefixes`, `TestBadTokenIsRejectedEvenWithAStartAfter`, `TestTokenTakesPrecedenceOverStartAfter`, `TestBlankTokenLeavesStartAfterAsTheBoundary`, `TestShortRunScansLongRunSeeksSameOutput`, `TestPrefixResumeIsInclusiveAtSuccessor`, `TestAllFFPrefixEndsTheListing` |
| rollup equals walk | `s3` `TestRollupMatchesWalk`; `replaydiff` `TestDriverFindsNoDifferenceBetweenRollupAndWalk`, `TestTokenCasesSkipWithoutAToken` |
| the 0xFF carry | `s3` `TestPrefixUpperAndSuccessor`, `TestPrefixRangeLaw` |
| tokens | `s3` `TestTokenGolden`, `TestTokenRoundTripsAndLegacyV1`, `TestBlankTokenIsAbsent`, `TestMalformedTokensAreRejected`, `TestTokenProperties`, `TestHasTokenAgreesWithDecode` |
| response memory | `output` (the contract's sink tests by name), `s3` `TestOneKeyAndEmptyPagesUseSmallFirstChunks`, `TestRenderStopsAtTheCap`; `server` `TestSparseResponsesReturnTheirReservation`, `TestBudgetRefusesAnOverlappingPage`, `TestOversizedResponseStopsAtTheCap`, `TestSlowReadersReachTheResponseCeiling` |
| XML | `s3` `TestRenderListGolden`, `TestRenderListElementsAndOrder`, `TestRenderAdversarialUnencodedValues`, `TestRenderSurrogateBytesUnencoded`, `TestLastModifiedYears`, `TestRenderNullSizeAndTimeAsZeroAndEpoch`, `TestPercentEncodesEveryUnsafeByte`, `TestTokenSuppressesTheIgnoredStartAfter`, `TestRenderError`, `TestPercentEncodeMatchesS3` |
| latency | `latency` `TestClassify`, `TestDelayByShapeAndPerCommonPrefix`, `TestScaleDividesEveryTerm`, `TestParseRejects`, `TestJitterIsDeterministicAndBounded`, `TestNumberGrammar`, `TestHugeDelaysSaturate`; `s3` `TestLatencyKeyGolden`; `server` `TestContinuationPagesDrawDistinctJitter`, `TestCountsNoOverrunWhenInjectionIsOff` |
| serving | `server` `TestServesPathStyleListObjectsV2`, `TestErrorsAreS3Answers`, `TestStoreFailureCarriesNoDetail`, `TestEchoesMaxKeysButClampsThePage`, `TestConcurrentRequests`, `TestRequestCeilingQueues`, `TestInjectedDelayIsADeadline`, `TestShutdownInterruptsALongDelay`, `TestUninterruptibleReadDefersStoreClose`, `TestShutdownBoundsABlockingStoreClose`, `TestShutdownIsIdempotentAndSurfacesCloseFailureOnce`, `TestRejectsAmbiguousTargets`, `TestDisorderedFixtureNamesOnlyThePart`, `TestDisorderedPartOverHTTP` |
| sorted serving | `server` `sorted_differential_test.go` (eleven cases of the contract's `SortedServingFullDifferentialTest`) |
| metrics port | `server` `TestMetricsPort`, `TestAttestation`; `metrics` `TestSnapshotIsStableAndTyped` |
| meters | `server` `TestMeterSetMatchesTheContract` (names, types and tags after a three-route mix against `testdata/meters.txt`, the Java scrape's set; the bench scripts' `http.requests` extraction), `TestStoreMetersFollowTheReads`, `TestRequestMeterSpans`, `TestAzureErrorHasNoWriteStage`, `TestClientGoneFinishesThePage`, `TestDisorderedPartOverHTTP` (`serving.refused`); `metrics` `TestTimerWindowCoversTheLastTwoMinutes`, `TestHistogramBuckets`, `TestCounterFunc`; `sortedparquet` `TestFileCacheBoundsHandles` and `TestCacheWaitEndsWithContextAndClose` (evictions, waits) |
| diagnostics | `server` `TestSlowRequestLogLine`; `javanum` `TestFormatDouble` |
| resource bounds under load (`SWATH_REPLAY_LOAD=1`) | `server` `TestLoadRequestCeilingUnderFaultProxy`, `TestLoadReadersBoundPageDecodes`, `TestLoadNoOverrunAtSixtyFourWalks`; `cmd/swath-replay` `TestServeWithinADescriptorLimit` (default suite) |
| the command | `cmd/swath-replay` `TestFlagSpellings`, `TestConfigRefusals`, `TestParseTimeout`, `TestServeEndToEnd` (startup line keys, a listing, SIGTERM exit 143), `TestSigtermWithActiveWorkIsBoundedAndIdempotent`, `TestSortFixtureThenServe` (a Java-era capture sorted by the command and listed through the S3 route), `TestSortFixtureRefusals` (stderr line and exit codes) |
| the Java server | `bench/replaydiff` against the Java launcher (not a unit test: it needs the Java build and a fixture) |

## The GCS route

`internal/replay/gcs` is the second route over the same store seam; `server/gcs.go` gives it the S3 route's
admission (the execution slot released before the write), response memory (reservation `1024 + 384 ×
pageSize`, first chunk sized from the entries returned, the page rendered into the shared `output.Output` one
entry at a time), delay deadline and shutdown. The hooks into the shared server are a `Protocols` field, one
classification call at the top of `ServeHTTP` (`serveOtherRoute`, which returns false when only S3 is enabled,
so the S3 path is unchanged), and the protocols and profiles in the metrics payload and startup line.

- **Parsing** (`Parse`). The request line's query is first read as UTF-8 with maximal-subpart replacement
  (`requestText`, `TestRequestTextGrouping`): unlike the S3 route, a raw non-ASCII byte reaches the parameter
  as itself, not as `?`. Then each `&`-separated part is percent-decoded strictly and must be valid UTF-8, and
  the checks run in the wire contract's order (`TestParseRefusals` holds each reason to the Java server's).
- **The pager** (`Pager.List`). The lower bound is the latest of the prefix (inclusive), `startOffset`
  (inclusive) and the token's boundary; the upper bound the earlier of the prefix's `PrefixUpper` and
  `endOffset`; a lower bound at or after the upper is an empty, final page. Without a delimiter one read of
  `pageSize + 1` rows answers the page. With one, the store's rollup answers when it accepts (`/` on the sorted
  store), with `suppressPrefixAtOrBeforeFloor` false: GCS lists a prefix even when `startOffset` lands inside
  it. Otherwise the pager walks in batches of up to 4,096 rows, emitting an object or the rolled-up prefix and
  resuming at the prefix's `Successor`, and decides truncation by the next row in range. A page ending on a
  prefix resumes inclusive at its successor, one ending on an object exclusive at it.
  `TestRollupMatchesWalkAndReference` holds the two paths to each other and every full walk to the listing
  computed directly, over random keyspaces, prefixes, offset pairs, delimiters and page sizes.
- **Tokens.** `gcs1.` + unpadded base64url of a flag byte, a progress byte (0), 16 bytes of
  SHA-256 over the length-prefixed scope (`gcs-json-v1`, `synthetic-v1`, the fixture identity, bucket, prefix,
  delimiter, startOffset, endOffset, the pagination profile) and the boundary key. Decoding accepts correct
  padding and refuses line breaks, as the contract's decoder does (`TestTokenShapes`). A token from another
  scope or fixture is refused before the request takes a response permit (`TestTokenRejectsAnotherScopeOrFixture`).
  The same state gives the same bytes on either server (`TestRenderGolden` checks Java tokens against the
  binding).
- **The renderer** (`AppendPage`) appends bytes: compact, fields in the contract's order, strings escaped only
  for `"`, `\` and C0 controls (`\b \t \n \f \r`, else `\u00XX` upper-case), so DEL, U+2028 and every
  non-ASCII character are written as UTF-8; `encoding/json` would escape `<`, `>`, `&` and U+2028.
  `TestRenderGolden` rebuilds five pages the Java server answered over the evidence corpus (control characters,
  a BOM, U+2028, backslashes and quotes, a full projection, delimiter pages with offsets) and requires the same
  bytes. Rows the API cannot represent are refused while rendering with the contract's reasons.
- **Native metadata.** `NewPager` reads a `swath/gcs-objects/v1` fixture's columns by name through
  `store.Sorted.ReadRows` and `RollupRows` (the same cursor and reader slots); any other store goes through
  `Rows` and the synthetic profile. `TestNativeSchemaRendersTheBucketsMetadata` lists a fake bucket with the
  Go lister, serves the dataset and checks each item against the fake's objects.

### The GCS driver and the evidence replay

`replaydiff -protocol gcs` builds `gcsMatrix` (the parameter table's values, `maxResults` edges, prefixes at
every depth with `/` and a two-byte delimiter, offsets around keys and inside subtrees, malformed, foreign and
rescoped tokens, wrong buckets, paths and methods, raw and escaped non-ASCII), `gcsWalks` (bucket, offset window
and top-level prefixes, with and without the delimiter, at each page size) and a random walk. `gcsComparator`
compares status, the compared headers plus `Allow`, and the body byte for byte (both servers are deterministic);
a difference only in `nextPageToken` is named, and walks follow each server's own tokens, so tokens are judged by
their effect. Measured with zero differences, no skipped token case and no token byte difference (the Java oracle at swath
`main` 01dd55b, `--parquet-connections 2`, default `-walks 1,7,1000` with `-walk-limit 300` on the large fixtures,
plus a separate full `-walks 1000 -random 0` run): the GCS evidence corpus (1,065 compared), synthetic 10k
(2,079), synthetic 1M rolled into 6 parts and 110 row groups (2,079 plus 1,565 with the full 1,000-page walks),
and the recordings of `noaa-normals-pds` (1,241 + 715) and `giab` (2,133 + 1,583).

`internal/replay/evidence` ports the conformance comparison: `compare_test.go` is the GCS field policy and
exchange comparator with the suite's messages (`TestComparatorMessages`; a pin is the whole message,
`TestPinsMatchWholeMessages`), a small order-keeping JSON node that refuses ill-formed UTF-8, repeated members
and trailing tokens stands in for the suite's strict parser, `evidence_test.go` is the capture replay (token
mapping and mutation, receipt checks of the request scope, walks), and `sdk_test.go` runs the SDK captures
through the Go client library with a recording transport (`TestSDKMakesOneAttempt` holds it to one attempt per
call). `TestEvidenceRejectsCorruptedReplayResults` corrupts matched answers (a dropped, repeated or reordered
item, a decoded name, an early end, a repeated prefix, a wrong status or reason, an ill-formed UTF-8 byte, and
the same corruptions of SDK runs) and requires each to be rejected.

| Contract | Test |
|---|---|
| GCS parsing | `gcs` `TestParseRefusals`, `TestParseValues`, `TestRequestTextGrouping` |
| GCS pager | `gcs` `TestFlatBoundsAreInclusiveThenExclusiveAndTokenSurvivesPageSizeChange`, `TestDelimiterOffsetInsideSubtreeEmitsPrefix`, `TestEmptyIntersection`, `TestRollupMatchesWalkAndReference` |
| GCS tokens and JSON | `gcs` `TestTokenShapes`, `TestTokenRejectsAnotherScopeOrFixture`, `TestRenderGolden`, `TestLatencyKeyGolden` |
| GCS native metadata | `gcs` `TestNativeSchemaRendersTheBucketsMetadata` |
| GCS serving | `server` `TestServesGCSObjectsList`, `TestGCSErrorsAreJSONEnvelopes`, `TestRouteClassification`, `TestGCSAdmissionRefusals`, `TestGCSFixtureAndStoreFailures`, `TestGCSInjectedDelayUsesTheSharedShapes`, `TestValidateProtocols`; `cmd/swath-replay` `TestProtocolSelection`, `TestServeGCSEndToEnd` |
| GCS driver | `replaydiff` `TestGCSComparator`, `TestGCSMatrixNamesAreUnique`, `TestGCSDriverFindsNoDifferenceBetweenRollupAndWalk` |
| GCS evidence | `evidence` `TestGCSNativeCapturesMatchReplay`, `TestEvidenceRejectsCorruptedReplayResults` (with `SWATH_REPLAY_FIXTURES`), `TestExpectationRules`, `TestComparatorMessages`, `TestPinsMatchWholeMessages`, `TestSDKMakesOneAttempt`, `TestProfileComparatorAcceptsAndRefuses`, `TestFullProjectionRequiresEmptyACLAndNoACLForbidsIt`, `TestWalksCompareInventoriesAcrossPageBoundaries`, `TestExchangeHeadersAreClassified`, `TestSDKRunReceiptNeedsEveryStep`, `TestRetryRunCapturesAreRefusedRatherThanWalked` |
| GCS ports of the Java unit tests | `gcs` `TestTimestampsMatchAnIndependentFormatter`, `TestRendersProfileTypes`, `TestRawEscapesAndOmitsEmptyArrays`, `TestCompleteWireGolden`, `TestRefusesTooLongAndReservedNamesAndNegativeSize`, `TestClampedPageLimitReportsItsPath`, `TestDelimiterWalkReadsInBatches`, `TestIndexedDelimiterMatchesWalkAcrossRowGroups`; `server` `TestGCSShapeCountsAndRejectionMeter`, `TestOfficialClientUsesHierarchyOffsetsAndUnicode`, `TestOfficialClientDecodesAReplayFault` |

The Java route's and conformance suite's own unit tests are held against this code where a Go counterpart
exists (`gcs` `ported_test.go`, `server` `TestGCSShapeCountsAndRejectionMeter` and `gcs_sdk_test.go`,
`evidence` `ported_test.go`). Not ported, with the reason:

| Java test | Reason |
|---|---|
| `GcsJsonTest.rendersProfileTypesAndKeepsOutputLeaseOpen` (the lease half) | the output lease is the Java response sink's; the profile types are `TestRendersProfileTypes` |
| `GcsPagerTest.optInEmptyAndShortPagesCarryBoundedProgressInToken` | the injected empty and short pages are a Java constructor-only test profile with no Go counterpart; the progress byte they set is held by `TestTokenProgressIsRetained` |
| `GcsSdkTest.officialSdkDecodesReplayFixtureFaultAndRecordsNormalRetries` (the retry half) | the Go client makes one attempt per call; the decoded fault is `TestOfficialClientDecodesAReplayFault` |
| `ProviderMatchReceiptRunTest.sdkHeadersIgnoreOnlyTheJvmVersionToken` | it compares the Java client's identity tokens; SDK runs here use the Go client, whose identity `TestSDKRunReceiptNeedsEveryStep` checks |
| `ProviderEvidenceTest.gcsKeepsObjectAndPrefixArraysSeparateAndChecksWireTypes`, `tokenWalkerFollowsEachEndpointsOwnOpaqueTokensAndRejectsCycles` | they test the Java capture tool's page parser and inventory walker, which have no Go counterpart; the wire-type and repeated-token refusals are in `TestProfileComparatorAcceptsAndRefuses` and `TestWalksCompareInventoriesAcrossPageBoundaries` |
| `ProviderCaptureRoundTripTest.sanitizedGcsErrorStillHasTypedEnvelope` | the capture sanitization pipeline is the capture tool's, not replay's |
| `GcsPagerTest.fixtureIdentitySurvivesRelocationAndInvalidatesChangedManifest` | the identity half is `store` `TestIdentity`; the token half `TestTokenRejectsAnotherScopeOrFixture` |

## The Azure route

### Routing

`serveOtherRoute` hands the Azure route, after the GCS check, a request whose raw path is
`/{account}/{one segment}` and whose query has a `restype` or `comp` name (percent-decoded), before the method
is looked at, so a POST to the Azure route is the Azure route's `405`. A claimed request that also has
`list-type` is the replay's ambiguous `400`; an unclaimed one goes to the S3 handler, or, without S3, gets the
replay's `404`. The Azure flow takes the same request
slot (released before the write), `begin`/`end` store registration, response permit and response memory as S3
(reservation `2048 + 512 × pageSize`, first chunk sized from the entries returned, the XML rendered into the
shared `output.Output` one entry at a time), applies the latency profile as a deadline, and answers shutdown
with `503 ServerBusy`.

### Order

Azure lists names in UTF-16 code-unit order. For valid UTF-8 that is byte order except at the first differing
byte, where lead bytes F0..F4 (a surrogate pair in UTF-16) rank before EE..EF (U+E000 to U+FFFF): byte ranks
`00..ED, F0..F4, EE..EF, F5..FF` (`rank`, `Compare`). A prefix window is contiguous in both orders, and the
orders disagree only between an EE/EF branch and an F0..F4 branch under the same parent, so provider order over
a byte range is the byte order with each such pair of sibling branches swapped. Two pieces build it on the
byte-sorted store:

- `rangesFrom(lower, inclusive, prefix)` turns a lower bound into byte ranges in provider order: the subtree
  below the bound and, outward position by position, the siblings that rank after the bound's byte, which are
  one range except at an EE..F4 byte (two or three). A 4,096-byte bound of U+10000 is 2,049 ranges.
- `cursor` reads each range in byte order, a batch at a time. When the next item has an EE/EF byte whose F0..F4
  siblings are still inside its range (`firstSwap`), the range splits into the F0..F4 branch, the EE/EF branch
  from that item, and the rest; items already read go to their part, so a split costs a read only for a part
  not yet read. Most parts of a long bound are empty: when a read comes back empty, one probe for the first key
  at or after the lowest unread store position settles every part below that key (`probeLowest`, `learn`), and
  parts split later inherit the finding. A page whose names have no such bytes costs the same reads as byte
  order; the 4,096-byte U+10000 bound costs under ten (`TestLongStartFromCostBound`).

### The pager

`Pager.List` takes the later, in provider order, of the prefix, `startFrom` (inclusive) and the marker's
boundary; a bound past the prefix window is an empty last page. Flat pages read rows through the cursor. With a
delimiter, a bound inside a rolled prefix first decides that prefix alone (listed when a name of it is at or
after the bound, read with a flat cursor over its subtree), then continues at the prefix's provider-order upper
bound (`upperOf`), so every later range holds whole subtrees; the remaining ranges are read through the store's
rollup when it accepts the delimiter (`suppressPrefixAtOrBeforeFloor` false: the range bounds decide membership)
or a row walk that seeks past each rolled prefix. A page ends one item early to learn truncation; the marker
resumes after a Blob's name (exclusive) or at a BlobPrefix's `upperOf` (inclusive).

Markers are `az1.` + base64url(flag, progress (always 0), 16 bytes of SHA-256 over the length-prefixed scope,
boundary). The scope digest is the Java server's, so markers are the same bytes for the same fixture identity
(`TestMarkerGolden`); decoding accepts padding and spare trailing bits and refuses CR/LF, a foreign scope, a
flag or progress above 1, and boundaries over 4,096 bytes (`TestMarkerDecodeLeniency`).

### Text and the wire

The raw query is read as UTF-8 with each maximal ill-formed subsequence one U+FFFD (`requestLineText`; unlike
the S3 route, valid non-ASCII passes through, which the Java server does on this route). Components are then
percent-decoded strictly and must be valid UTF-8. The body is appended byte by byte: the BOM and
`<?xml version="1.0" encoding="utf-8"?>` and a newline, the echoes in the order Prefix, Marker, MaxResults,
Delimiter (each when the parameter was sent, `MaxResults` as sent), five-character XML escaping, CR in a name as
`&#13;`, `Encoded="true"` percent-encoding (RFC 3986 unreserved bytes literal, upper-case hex) for a name holding
U+FFFE or U+FFFF, `Last-Modified` in RFC 1123 at second precision (`TestRenderListGolden` is the Java server's
answer over the same rows). The route drops the Java server's 254-segment limit on blob names: native Azure lists
a 255-segment name (`TestRenderRefusals`; evidence capture `azure-12a`).

### The driver's Azure section

`replaydiff -protocol azure` serves both sides with `--protocols s3,azure --azure-account replay` and runs
`azureMatrix` (both versions; `maxresults` edges and garbage; prefixes at directory and inside-segment depths;
delimiters `/`, multi-byte, astral, empty; `startFrom` around names, inside rolled prefixes and with EE..F4
characters including the 4,096-byte U+10000 bound; header, `Accept`, query-grammar and raw-byte refusals;
routing, HEAD, POST, DELETE), marker cases in which each server resumes with its own marker (page 2, other
scope, other version, truncated, flipped, padded), marker walks at `-walks` sizes (default 1, 7, 5000), and a
seeded random set. `azureComparator` requires the same status and classified headers (`Content-Type`,
`x-ms-version`, `x-ms-error-code`, `x-ms-client-request-id`, `Allow`, `x-swath-replay-error`), a well-formed
`x-ms-request-id` and `Date` on each side, the BOM, and the same body bytes after masking the
`ServiceEndpoint` authority and replacing every marker by its effect (flag, progress, boundary); it counts the
answers that matched only by effect, which is zero when both servers report the same fixture identity. With
`-java-args "... --serving-mode duckdb"` and `-go-fixture` it compares the Java server serving an unsorted
capture with the Go server serving the same rows sorted; `-accept-segment-improvement` then counts the Java
server's `too_many_segments` refusals answered by the Go route as the known improvement.

### The evidence replay

`azure` `TestAzureEvidenceReplay` (`SWATH_REPLAY_FIXTURES`) extracts `docs/replay-provider-evidence.tar.gz`, serves
the archive's corpus (names up to 2,010 bytes, sorted with a 4,096-byte key limit and checked against the
manifest) on the Azure route alone, and replays each capture as the conformance tree's
`NativeEvidenceReplayTest` does, with a port of its comparators (`evidencecmp_test.go`) whose failure messages
are spelled as the pins in `azure/replay-expectations.json` spell them. SDK runs are replayed as the requests
the SDK sends (captured headers, client request ids `prefix-N`). A pinned match must match; a pinned mismatch
must reproduce or improve; `SWATH_REPLAY_PROVIDER_REPORT=DIR` writes `DIR/azure-outcomes.json`.

### Azure tests

| Contract | Test |
|---|---|
| provider order, flat and delimited, against brute force | `azure` `TestProviderOrderMatchesUTF16`, `TestStartFromInsideRolledPrefix` |
| empty-read settlement cost | `azure` `TestLongStartFromCostBound` |
| markers | `azure` `TestMarkerGolden`, `TestMarkerDecodeLeniency` |
| request checks and their order | `azure` `TestParseRefusalOrder`, `TestAcceptRule`, `TestRequestLineText` |
| bodies | `azure` `TestRenderListGolden`, `TestRenderRefusals` |
| latency key | `azure` `TestLatencyKeyGolden` |
| routing, headers, meters, configuration | `server` `TestAzureRouting`, `TestAzureMarkerWalk`, `TestValidateAzure`; `cmd/swath-replay` `TestAzureFlagsAndStartupLine` |
| driver | `replaydiff` `TestAzureComparator`, `TestAzureDriverAgreesWithItself` |
| native captures | `azure` `TestAzureEvidenceReplay` (fixture tier) |
