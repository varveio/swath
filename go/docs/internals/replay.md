# Replay server internals

How `cmd/swath-replay` is put together, and what holds it to the Java server's wire behaviour. The user's view
is [../replay.md](../replay.md).

## Packages

| Package | Role | Imports |
|---|---|---|
| `cmd/swath-replay` | flags (standard `flag`, the Java launcher's names and defaults), the startup line, signals, opening the fixture | server, latency, store, memstore |
| `internal/replay/server` | the HTTP server: route classification, the S3 and GCS routes' request flow, admission, the delay deadline, shutdown, the metrics port and the runtime attestation | s3, latency, metrics, store |
| `internal/replay/gcs` | objects.list: request parsing, the pager over `store.Store` (or the GCS schema's columns through `store.Sorted`), `gcs1.` tokens, the JSON page and error envelope, the request's latency key | s3 (key carries), store, sortedparquet |
| `internal/replay/s3` | ListObjectsV2: request parsing, the pager, continuation tokens, the XML documents, the request's latency key | store |
| `internal/replay/latency` | profiles, jitter, scale, and the provider-neutral request classification | – |
| `internal/replay/metrics` | the meter registry (definitions, tagged instances) and the schema-2 JSON | – |
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
- **Integers and blanks.** `max-keys` accepts a sign and decimal digits of any script, within int32; "blank"
  (an absent token, a default `max-keys`) is the Unicode separators except the no-break spaces plus the ASCII
  and information-separator controls.

The XML is appended byte by byte rather than marshalled: `encoding/xml` escapes `"` as `&#34;` and control bytes
as character references, and the element order, `&apos;`, ETag quoting as `&quot;`, millisecond `LastModified`
rounded toward the earlier millisecond and the `encoding-type=url` grammar are fixed by the wire contract
(`TestRenderListGolden` is a page the Java server answered).

## Latency

`latency.Classify(delimiter, maxResults)` is the shape: delimiter present is a structure probe, else at most one
result a pivot probe, else a worker page. It takes provider-neutral inputs so every listing route classifies alike.
`Profile.Delay(shape, commonPrefixes, key)` is the target: the shape's base, plus its per-prefix term times the
prefixes returned, then, with jitter, scaled by `1 + jitter × u` where `u` in [-1, 1) comes from the
SplitMix64-avalanched request key. The S3 key (`Request.LatencyKey`) hashes prefix, delimiter, start-after,
token and max-keys as the Java server does, so a request draws the same delay from either server
(`TestLatencyKeyGolden`); avalanching matters because successive pages of one walk differ only in nearby tokens.

The server applies the target as a deadline from the start of paging: it sleeps for the remainder, or counts an
overrun with the excess when there is none.

## Admission and shutdown

- `--max-concurrent-requests` is a channel semaphore taken first and held for the whole request: excess
  requests wait, as they would for a full worker pool. A request in an injected delay holds its slot.
- A response-count ceiling (twice the request ceiling) and a byte budget sit behind it. The byte budget is
  charged with an estimate before paging (`max(4096, 512 + 320 × pageSize)`, capped at the per-response cap) and
  settled after rendering; a response over `--max-response-bytes` is a 500. Both refuse with a 503 rather than
  queue, and count `response.admission.refused{reason}`. The store bounds its own page decodes
  (`--parquet-connections`); the server adds no read permit of its own.
- Writes get a deadline when writing starts (`http.ResponseController.SetWriteDeadline`); `http.Server`'s
  `WriteTimeout` would run from the end of the request headers and so include the injected delay.
- Shutdown cancels one context: queued requests and sleeping delays answer 503 `shutdown` at once, a store read
  sees cancellation, both listeners close, and the store is closed after the last request holding it returns.
  If the deadline passes first the server logs `shutdown_incomplete` and a read that ignores cancellation keeps
  the store open until it returns (`TestUninterruptibleReadDefersStoreClose`).

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
  `-1`, blank, signed, out of range), prefixes at every depth of the keys (sampled when there are many) and one
  that matches nothing, each with no delimiter, `/` and a two-byte delimiter taken from the keys; `start-after`
  before, at, between and after keys and at a prefix and its successor; keys that need encoding with and without
  `encoding-type=url`; `fetch-owner`; malformed, blank, foreign and v1 tokens, and a real token with a
  `start-after` on either side; wrong, escaped and missing buckets; extra path segments; HEAD, POST and DELETE;
- token walks to the end at each page size (`-walks`, default 1, 7, 1000) over the bucket and each top-level
  prefix, with and without the delimiter, following the reference's tokens and comparing every page;
- a seeded random walk (`-random`, `-seed`).

"Identical" is: the same status, the same `Content-Type` and the same value for every `x-amz-*` and
`x-swath-replay-*` header, and the same body bytes. `Date`, `Server` and framing (`Content-Length` against
chunked) are not compared. A difference is reported with the first differing body byte and the bytes around it.
The request set is data (a `request` list and a `comparator`), so another route adds its matrix and, where byte
identity cannot hold, a structural comparator; `-live-endpoint` uses one (leaf elements, tokens masked, error
codes only) to compare the Go server with live S3, where the bucket may have changed since it was recorded.

In the package's own tests the driver compares two Go servers over the same keys, one whose store answers the
`/` rollup and one that declines it (`TestDriverFindsNoDifferenceBetweenRollupAndWalk`).

## Tests

| Contract | Test |
|---|---|
| request parsing and its errors | `s3` `TestParseRequest`, `TestParseRequestErrors`, `TestParseHTTPRoutesByFirstSegment` |
| query and text decoding | `s3` `TestQueryTextMatchesTable`, `TestDecodeTextMatchesTable`, `TestIsBlank`, `TestParseInt32` |
| pager rules | `s3` `TestListsByPrefixAndExclusiveStartAfter`, `TestTruncatesAndResumesFromToken`, `TestMaxKeysZeroIsEmptyAndNotTruncated`, `TestRollsUpAndResumesPastARolledUpPrefix`, `TestStartAfterFiltersCommonPrefixes`, `TestBadTokenIsRejectedEvenWithAStartAfter`, `TestTokenTakesPrecedenceOverStartAfter`, `TestBlankTokenLeavesStartAfterAsTheBoundary`, `TestShortRunScansLongRunSeeksSameOutput`, `TestPrefixResumeIsInclusiveAtSuccessor`, `TestAllFFPrefixEndsTheListing` |
| rollup equals walk | `s3` `TestRollupMatchesWalk`; `replaydiff` `TestDriverFindsNoDifferenceBetweenRollupAndWalk` |
| the 0xFF carry | `s3` `TestPrefixUpperAndSuccessor`, `TestPrefixRangeLaw` |
| tokens | `s3` `TestTokenGolden`, `TestTokenRoundTripsAndLegacyV1`, `TestBlankTokenIsAbsent`, `TestMalformedTokensAreRejected`, `TestTokenProperties`, `TestHasTokenAgreesWithDecode` |
| XML | `s3` `TestRenderListGolden`, `TestRenderListElementsAndOrder`, `TestRenderAdversarialUnencodedValues`, `TestRenderNullSizeAndTimeAsZeroAndEpoch`, `TestPercentEncodesEveryUnsafeByte`, `TestTokenSuppressesTheIgnoredStartAfter`, `TestRenderError`, `TestPercentEncodeMatchesS3` |
| latency | `latency` `TestClassify`, `TestDelayByShapeAndPerCommonPrefix`, `TestScaleDividesEveryTerm`, `TestParseRejects`, `TestJitterIsDeterministicAndBounded`; `s3` `TestLatencyKeyGolden` |
| serving | `server` `TestServesPathStyleListObjectsV2`, `TestErrorsAreS3Answers`, `TestStoreFailureCarriesNoDetail`, `TestEchoesMaxKeysButClampsThePage`, `TestConcurrentRequests`, `TestRequestCeilingQueues`, `TestResponseCountAndBudgetRefusals`, `TestOversizedResponseIsRefusedAndTheBudgetRecovers`, `TestInjectedDelayIsADeadline`, `TestShutdownInterruptsALongDelay`, `TestUninterruptibleReadDefersStoreClose` |
| metrics port | `server` `TestMetricsPort`, `TestAttestation`; `metrics` `TestSnapshotIsStableAndTyped` |
| the command | `cmd/swath-replay` `TestFlagSpellings`, `TestConfigRefusals`, `TestParseTimeout`, `TestServeEndToEnd` (startup line keys, a listing, SIGTERM exit 143) |
| the Java server | `bench/replaydiff` against the Java launcher (not a unit test: it needs the Java build and a fixture) |

## The GCS route

`internal/replay/gcs` is the second route over the same store seam; `server/gcs.go` gives it the S3 route's
admission, budget, delay deadline and shutdown. The hooks into the shared server are a `Protocols` field, one
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
their effect. Measured with zero differences (the Java oracle at swath `main` 01dd55b, `--parquet-connections 2`,
default `-walks 1,7,1000`, `-walk-limit 300` and a separate full `-walks 1000` run on the large fixtures): the
GCS evidence corpus (1,051 compared), synthetic 10k (2,065), synthetic 1M rolled into 6 parts and 110 row groups
(2,065 plus 1,551 with the full 1,000-page walks), and the recordings of `noaa-normals-pds` (1,227 + 701) and
`giab` (2,119 + 1,569).

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
| GCS evidence | `evidence` `TestGCSNativeCapturesMatchReplay`, `TestEvidenceRejectsCorruptedReplayResults`, `TestExpectationRules`, `TestComparatorMessages` (with `SWATH_REPLAY_FIXTURES`) |
