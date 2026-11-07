# Replay server internals

How `cmd/swath-replay` is put together, and what holds it to the Java server's wire behaviour. The user's view
is [../replay.md](../replay.md).

## Packages

| Package | Role | Imports |
|---|---|---|
| `cmd/swath-replay` | flags (standard `flag`, the Java launcher's names and defaults), the startup line, signals, opening the fixture; `sort-fixture` as a thin command over `internal/replay/sortfixture` | server, latency, store, memstore, sortfixture |
| `internal/replay/server` | the HTTP server: request-target checks, the S3 route's request flow, admission, the delay deadline, shutdown, the metrics port and the runtime attestation | s3, latency, metrics, output, store, sortedparquet (its disorder error) |
| `internal/replay/s3` | ListObjectsV2: request parsing, the pager, continuation tokens, the XML documents, the request's latency key | output, javanum, store |
| `internal/replay/output` | the response byte budget and the chunked sink responses are rendered into | – |
| `internal/replay/javanum` | the contract's number grammar: BMP-digit integers, the latency profile's floats, saturating conversion | – |
| `internal/replay/latency` | profiles, jitter, scale, and the provider-neutral request classification | javanum |
| `internal/replay/metrics` | the meter registry (definitions, tagged instances) and the schema-2 JSON | – |
| `internal/replay/store` | the serving seam: `Store`, `Object`, `Projection`, `Open` | – |
| `internal/replay/store/memstore` | an in-memory `Store` and the TSV fixture format | store |
| `bench/replaydiff` | the differential driver (a `main` package, never linked into the server) | s3, store, memstore, server (tests) |

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
a meter is adding a `Def` to `defs.go`; the names and tags are the Java server's, so a dashboard reads either.
The meters that must read zero rather than be absent (requests, errors, the latency timers per shape, the
serving path) are created when the server starts.

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
| the command | `cmd/swath-replay` `TestFlagSpellings`, `TestConfigRefusals`, `TestParseTimeout`, `TestServeEndToEnd` (startup line keys, a listing, SIGTERM exit 143), `TestSigtermWithActiveWorkIsBoundedAndIdempotent`, `TestSortFixtureThenServe` (a Java-era capture sorted by the command and listed through the S3 route), `TestSortFixtureRefusals` (stderr line and exit codes) |
| the Java server | `bench/replaydiff` against the Java launcher (not a unit test: it needs the Java build and a fixture) |
