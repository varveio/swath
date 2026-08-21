# S3 implementation compatibility notes

Deviations between real AWS S3 and S3-compatible implementations (LocalStack, MinIO, and
similar) that swath has had to design around, because they are not visible from the S3 API
contract alone — only from running against the real thing.

## `%` in an echoed `start-after`/`prefix`/`marker`

**The deviation.** When a `ListObjectsV2` request is made with `encoding-type=url`, S3 percent-
encodes the `Prefix`/`StartAfter`/`Marker`/`ContinuationToken`/`Delimiter` fields it echoes back
in the response. Real S3 does this correctly for every input,
including one containing a lone or trailing `%` (which is not itself a valid percent-escape).
The tested LocalStack build instead echoes those fields back **verbatim** — whatever byte
sequence was sent on the wire, unmodified — rather than re-encoding. The tested MinIO build
percent-encodes the echo conformantly; this is an endpoint-specific compatibility gap, not a
property of MinIO or S3-compatible servers in general.

**Why real S3 is safe.** The AWS SDK for Java always installs a response interceptor
(`DecodeUrlEncodedResponseInterceptor`) that strict-decodes those echoed fields with
`java.net.URLDecoder` while unmarshalling the response (this happens regardless of
`encoding-type`, since it is unconditional in the SDK, not driven by the request). Against real
S3, the echoed value is always correctly percent-encoded, so a well-formed request value
(including one containing `%`) round-trips through encode-on-the-way-out /
decode-on-the-way-back losslessly.

**Why a verbatim-echo endpoint crashes.** Because such an endpoint does not re-encode, a request value
containing a lone or trailing `%` — one that is not the start of a valid `%XX` escape — is
echoed back unchanged. `URLDecoder` then throws:

```
java.lang.IllegalArgumentException: URLDecoder: Incomplete trailing escape (%) pattern
```

which surfaces to swath as an SDK-side response-unmarshalling failure, not an S3 error — it
aborts the whole listing rather than producing a normal error response swath's retry/backoff
logic can reason about.

**The synthesis exposure swath prevents.** swath invents two kinds of string that
travel through `start-after`/`prefix`: (1) split-pivot cursors synthesized by
`ByteMidpoint.between`/`forwardReflect` (`swath-model`, `io.varve.swath.model.ByteMidpoint`), and (2)
seed cut points synthesized by `SeedStep` (`swath-core`,
`io.varve.swath.engine.SeedStep`, where the original mitigation landed). A synthesized value is built from an
observed key prefix plus ONE invented code point (the divergence/append/reflected scalar, or the
seed cut's appended byte) — and without the exclusions below `%` (`U+0025`) is one byte synthesis could invent as a
1-byte trailing scalar, e.g. `a` (byte `0x61`) synthesizing to `a%` (bytes `0x61 0x25`).

**The fix.** Both synthesis points exclude `0x25`/`U+0025` from the set of code points they will
ever invent:

- `SeedStep`'s `UNSAFE_SCALAR` (`swath-core`) excludes it from the appendable printable-ASCII
  alphabet — the original mitigation.
- `ByteMidpoint.isSafe` (`swath-model`) excludes it from the safe-scalar set `E^c` used by pivot
  synthesis — this note's subject. Excluding one more scalar from an already-sparse
  safe set is correctness-neutral for the no-gap/no-overlap tiling: at worst a split that would
  have landed on `%` instead falls back to the next candidate (e.g. `a ++ U+0020`), which is
  still strictly between the same bounds — betweenness, not the specific scalar chosen, is what
  the tiling invariant (I12) requires.

The test-side mirror of `ByteMidpoint.isSafe`'s excluded set,
`swath-model`'s `io.varve.swath.testkit.ScalarSafety.isExcludedScalar` (testFixtures), carries the
same `0x25` exclusion so the properties that guard synthesis check against the same set
the production code enforces.

**Why swath's own replay server doesn't catch this.** `swath-replay` is deliberately
S3-faithful in its XML encoding — it re-percent-encodes echoed fields the way real S3 does, not
verbatim — so replaying a fixture through it can never reproduce this crash. The actual
LocalStack integration-test path exercises the verbatim-echo behavior; a hermetic hand-
built LocalStack-shape response (a unit/IT-level fixture that mimics LocalStack's verbatim-echo
XML without a container) is the closest thing swath's own test suite can carry.

**The user-supplied-prefix limitation.** This exclusion applies only to code points swath
*invents*. A user-supplied `--prefix` (or any bound copied verbatim from user input or from a
bucket's real keys) may legitimately contain `%`, including a lone/trailing one, and that is a
load-bearing filter the user asked for — swath must not silently strip or rewrite it. If a user
targets a verbatim-echo endpoint with a prefix ending in a lone `%`, the same
`URLDecoder` crash can still occur, and swath does not — and should not — work around it by
mangling the user's input. This is a documented, known limitation of running swath against a
verbatim-echo endpoint with such a prefix, not a defect in the fix above.

**Upstream.** This is a LocalStack conformance gap against real S3's re-encoding
behavior. A minimal upstream report can use this note and a
minimal repro (a `ListObjectsV2` call with `encoding-type=url` and a `StartAfter`/`Prefix`
containing a lone trailing `%`).

**End-to-end guard.** `swath-s3`'s `PercentEchoLocalStackIT` closes the gap the
hermetic `ByteMidpointPercentEchoSafetyTest` (above) cannot: it proves, against a REAL LocalStack
container, both (1) that the wire-level crash described above is genuine (a positive control —
`startAfter` ending in a lone `%` really does throw `URLDecoder`'s "Incomplete trailing escape"
from a raw SDK `listObjectsV2` call), and (2) that the fixed, normal swath path — real keys
containing `%`-sequences and `+`, listed through the production `S3PageFetcher` (real
`encoding-type=url`) — round-trips byte-exact with no crash.

## `%`-containing key used as a `start-after` cursor — silent under-count

**The deviation.** LocalStack **double-URL-decodes** the `start-after` (and, by the same code
path, `marker`/`prefix`) request parameter — it applies URL-decoding one extra time versus real S3.
When the `start-after` value is itself a real key containing a literal `%25` sequence, that extra
decode turns `%25` into `%`, moving the effective cursor to a lexicographically LATER position than
intended and silently skipping every key between the two. Real S3 (and MinIO — verified below)
decode `start-after` exactly once, so the cursor round-trips losslessly.

**Why it surfaced as a silent under-count, not a crash.** Driving a full `WORK_STEALING` run
(`ListRunner`/`S3PageFetcher`) against a bucket whose REAL keys contain a literal `%` (e.g.
`hot/%25percent-encoded-0009`), with a small `maxKeys` that forces many internal pages, under-counts
against LocalStack: `errors=0` and `quiescence_reached` are logged, but some `%`-keys never reach
the Parquet output (reproduced deterministically at n=20, `maxKeys=3`, `workerCount=2`: 4 of 7
`%`-keys missing, no exception, no WARN). This is not the percent-echo crash above (no exception at all) and is
unrelated to synthesized-pivot exclusion (these are real observed keys becoming cursors, not
`ByteMidpoint`-invented ones).

**Root cause — NOT a swath defect (verdict: LocalStack encoding artifact).** swath paginates purely
by `start_after = last in-range source key` (`RangeScanner`, algorithms.md §2) and sends that cursor as the
**raw decoded key** (`S3PageFetcher.toRequestParam` → `new String(raw, UTF_8)`), which is exactly
correct per S3 semantics: `StartAfter` is a raw key, and the SDK URL-encodes the query parameter at
the HTTP layer. The engine's cursor logic is right — it round-trips losslessly against any
conformant endpoint. The skip is entirely on the LocalStack side.

**Decisive evidence (deterministic wire trace, n=20 / maxKeys=3 fixture).** For a page whose last
key is `hot/%25percent-encoded-0006`:

- swath sends `start-after = hot/%25percent-encoded-0006` (raw decoded key). The SDK percent-encodes
  it on the wire (`%` → `%25`, so the key's `%25` becomes `%2525`).
- LocalStack's response `<StartAfter>` echo, after the SDK's always-on
  `DecodeUrlEncodedResponseInterceptor` decodes it once, reads **`hot/%percent-encoded-0006`** — an
  extra decode (`%25` → `%`) versus what was sent.
- That double-decoded effective cursor `hot/%percent-…` sorts AFTER every `hot/%25percent-…` key
  (byte after `hot/%` is `p`=0x70 in the cursor vs `2`=0x32 in the real keys) but before
  `hot/plain-…` — so the response jumps straight to the plain keys, silently dropping
  `hot/%25percent-encoded-0009/0012/0015/0018` (exactly the 4 missing keys).
- The `NextContinuationToken` on the preceding page base64-decodes to `hot/%25percent-encoded-0009`
  — the CORRECT successor — proving the data and its correct next-position exist server-side; only
  the `start-after` comparison path is wrong. (swath cannot switch to continuation-token pagination:
  the range-stealing engine needs an arbitrary sub-range lower bound, which an opaque continuation
  token cannot express — algorithms.md §2.)

**Adjudication across implementations.**

- **LocalStack 3.8** — double-decodes `start-after`; under-counts (raw-SDK manual pagination, with
  no swath engine involved, misses the identical 4 keys — isolating the defect below swath).
- **MinIO (tested build)** — decodes `start-after` exactly once and percent-encodes the response `<Key>`
  and `<StartAfter>` fields faithfully; a manual `start-after = hot/%25percent-encoded-0006`
  pagination returns `0009/0012/0015` correctly, no skip. This is real-S3-faithful behavior and
  confirms the defect is **LocalStack-specific**, not general to S3-compatible endpoints.
- **Real AWS S3** — not directly testable (no write access to create `%`-keys, and no known public
  bucket with such keys). But the SDK sends a correctly single-encoded `start-after`, and both the
  documented S3 semantics and MinIO's conformant behavior imply real S3 decodes once and never
  skips.

**Consequence for swath.** No product change. swath's cursor round-trip is correct against real S3;
the under-count is a LocalStack conformance gap. The mechanism is pinned as a raw-SDK positive
control (`PercentEchoLocalStackIT#localStackDoubleDecodesAPercentContainingStartAfterAndSilentlySkipsKeys`),
mirroring the percent-echo crash control above, so a future LocalStack fix — or any real-S3/MinIO run, which never
skips — is caught as a change. **User impact against LocalStack (and any other double-decoding
endpoint):** a listing whose keys contain literal `%25` sequences and that paginates across such a
key as a cursor may under-count; this is the same class of documented verbatim-echo/decode
limitation as the lone-`%` crash above, not a swath correctness defect on real S3. The tested MinIO
build (above) is unaffected — it decodes `start-after` exactly once. Worth filing upstream
against LocalStack alongside the re-encoding gap above.
