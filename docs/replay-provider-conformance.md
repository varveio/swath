# Replay provider conformance matrix

This is the human-readable matrix for [issue #221](https://github.com/varveio/swath/issues/221)
within [epic #220](https://github.com/varveio/swath/issues/220). The
[machine-readable ledger](replay-provider-ledger.json) owns row IDs and evidence status;
the offline validator requires this matrix to match it.
`UNMEASURED` means no native-provider capture exists. `OBSERVED` requires a sanitized
native capture and its SHA-256 checksum. `REPLAY_MATCH` additionally requires an
independent replay test against that capture. Documentation, model reviews, local
emulators, and agreement with a future swath fetcher do not advance a row.

**Current release gate:** all rows below are `UNMEASURED`. The GCS and Azure profiles
are provisional; neither can be called provider-conformant. Live tests were postponed
for the initial implementation. The fixed corpus, native request captures, SDK
auto-paginator runs, and measured replay matches remain mandatory before release.
Each Azure ledger row names its required service versions: the rollout rows are
version-specific, while shared XML behavior needs separate evidence for both
`2026-06-06` and `2026-10-06` before its status can advance.

| ID | Provider | Feature or probe | Profile promise pending evidence | Capture SHA-256 | Replay test | State |
| --- | --- | --- | --- | --- | --- | --- |
| gcs-01 | GCS JSON v1 | `startOffset` inclusive and `endOffset` exclusive, including equality | Provisional bounded list | — | — | UNMEASURED |
| gcs-02 | GCS JSON v1 | Prefix intersection with both offsets | Provisional bounded list | — | — | UNMEASURED |
| gcs-03 | GCS JSON v1 | Delimiter with offset at, inside, and after a subtree | Provisional; fast path gated | — | — | UNMEASURED |
| gcs-04 | GCS JSON v1 | BMP versus supplementary Unicode ordering | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| gcs-05 | GCS JSON v1 | Literal plus, percent escapes, duplicate query fields | Strict native parser; duplicate policy gated | — | — | UNMEASURED |
| gcs-06 | GCS JSON v1 | Zero and overflow `maxResults` | Zero refused pending evidence | — | — | UNMEASURED |
| gcs-07 | GCS JSON v1 | Short/empty pages and repeated prefixes | Complete native-token walk required | — | — | UNMEASURED |
| gcs-08 | GCS JSON v1 | Name length/encoding and raw wire spelling | Strict UTF-8 subset | — | — | UNMEASURED |
| gcs-09 | GCS JSON v1 | HTTP Java SDK default projection, fields, and auto-pagination | Must run unmodified with endpoint override | — | — | UNMEASURED |
| gcs-10 | GCS JSON v1 | Retry-disabled one-attempt errors; normal SDK retries | Separate request counts | — | — | UNMEASURED |
| azure-01 | Azure Blob XML `2026-06-06` | Account accepts service version and echoes it | Explicit version only | — | — | UNMEASURED |
| azure-02 | Azure Blob XML `2026-10-06` | Account rollout and version echo | Version individually gated | — | — | UNMEASURED |
| azure-03 | Azure Blob XML | `startFrom` inclusive and prefix intersection | Provisional flat list | — | — | UNMEASURED |
| azure-04 | Azure Blob XML | Delimiter with `startFrom` at, inside, after subtree | **Unsupported until native evidence** | — | — | UNMEASURED |
| azure-05 | Azure Blob XML | `marker` plus `startFrom` precedence | Matching `startFrom` retained on continuation; other combinations refused pending evidence | — | — | UNMEASURED |
| azure-06 | Azure Blob XML | BMP versus supplementary Unicode ordering | Provisional unsigned UTF-8 order | — | — | UNMEASURED |
| azure-07 | Azure Blob XML | Encoded names, raw plus/percent escapes, exact XML text/attributes | Explicit encoding; raw-wire assertion required | — | — | UNMEASURED |
| azure-08 | Azure Blob XML | Duplicate parameters; zero/overflow `maxresults` | Zero refused pending evidence | — | — | UNMEASURED |
| azure-09 | Azure Blob XML | Short/empty pages, repeated prefixes, native 5k pages | Complete native-marker walk required | — | — | UNMEASURED |
| azure-10 | Azure Blob XML | Java SDK flat/hierarchy auto-pagination and `setStartFrom` | Must run unmodified with endpoint override | — | — | UNMEASURED |
| azure-11 | Azure Blob XML | Retry-disabled one-attempt errors; normal SDK retries | Separate request counts | — | — | UNMEASURED |
| azure-12 | Azure Blob XML | Name length units and slash-segment boundary cases | Provisional 1,024 UTF-16 units and 254 segments; native creation probes required | — | — | UNMEASURED |

The corpus manifest must record exact object names, content SHA-256 values, account
namespace mode (GCS flat with uniform bucket-level access; Azure flat with HNS off),
region, API and pinned SDK versions, capture date, probe request, expected result,
and sanitized capture checksum. Keep credentials and private infrastructure
identifiers out of committed captures. A capture checksum is calculated **after**
sanitization. Every token walk uses the token returned by its own endpoint; tokens
and page boundaries are not compared byte for byte. Compare complete object and
prefix inventories, order, status, and applicable metadata instead.

Offline SDK smoke tests use official `google-cloud-storage:2.73.0` over HTTP/JSON and
`azure-storage-blob:12.35.1` with explicit `V2026_06_06` and
`azure-core-http-jdk-httpclient:1.1.5`. The GCS SDK's local requests send
`projection=full` and no `fields`; the Azure SDK's local requests send
`x-ms-version: 2026-06-06`, retain `startFrom` on continuation, and transmit `/`
literally in delimiter and startFrom query values. These are client-side request
observations against replay, not native-provider behavior. Both clients complete
auto-paginated local walks; retry-disabled 503 recipes each assert one attempt.

| Feature/error | GCS JSON v1 provisional behavior | Azure Blob XML provisional behavior |
| --- | --- | --- |
| Page capacity | Default 1,000; larger valid requests cap at 1,000; zero refused | Default 5,000; signed-int32 values above 5,000 cap at 5,000; zero, negative and overflow refused |
| Range | Inclusive `startOffset`, exclusive `endOffset`, intersected with prefix | Inclusive `startFrom`; `endBefore` refused for XML |
| Delimiter | Objects and unique prefixes consume one capacity each; offset intersections remain evidence-gated | Interleaved Blob and BlobPrefix consume capacity; `startFrom` plus delimiter refused until observed |
| Marker | Replay-owned `gcs1.` token bound to fixture and listing scope | Replay-owned `az1.` marker bound to fixture, namespace, version and listing scope; matching startFrom retained |
| Unsupported features | Versions, soft deletion, glob/filter evaluation, ACL expansion and partial fields | HNS/ADLS, versions, snapshots, deleted/include expansions, Arrow and `endBefore` |
| Client error | JSON `invalid` 400; not-found 404; wrong-method 405 | XML `InvalidQueryParameterValue` / `OutOfRangeQueryParameterValue` / `InvalidHeaderValue` 400; `ContainerNotFound` 404 |
| Shared serving failure | JSON `backendError` 503 or `internalError` 500 | XML `ServerBusy` 503 or `InternalError` 500 |

Completed replay metrics expose `swath.replay.provider.path` with protocol, path,
and reason tags. GCS records a requested `maxResults` above 1,000 as
`page_limit_clamped/requested_above_1000`. A fixture value that cannot be rendered
under the GCS profile records `fixture_rejected` with a bounded reason such as
`invalid_utf8_name` or `negative_size`; the client receives a provider-shaped 500.
These counters describe replay behavior and do not advance any native evidence row.

| Field class | GCS JSON v1 | Azure Blob XML | Comparison rule |
| --- | --- | --- | --- |
| Exact | Object name, size, timestamp instant at fixture microsecond precision, ordering and bounds | Blob/prefix name text, order, size, Last-Modified instant at second precision, `Encoded` attribute, version/error behavior | Compare membership, type, value, and order; preserve absent versus empty; require canonical replay timestamp spelling |
| Synthetic typed | ETag, generation, metageneration, storage class, content type, full-projection empty ACL | ETag, blob type, tier, lease state/status | Assert replay profile value and type; assert native field validity without asserting native value identity |
| Profile omitted | Creation fields, checksums, owner, no-ACL projection ACL, native links | Creation fields, encryption, empty optional content properties | Native may contain these; replay must omit them |
| Namespace mapped | Bucket origin and self links where supported | `ServiceEndpoint` and `ContainerName` | Map explicit test namespace to replay namespace; verify URL shape and trailing slash |
| Opaque | `nextPageToken` | `NextMarker` | Verify terminal absence/emptiness and completed walk; echoed Azure `Marker` equals that endpoint's own request token |
| Named volatile | Request IDs and HTTP `Date` | Request IDs and HTTP `Date` | Validate spelling/nonempty value when present; do not compare values or require matching presence |
| Classified transport | `server`, `cache-control`, `vary`, `transfer-encoding`, `content-length` | `server`, `transfer-encoding`, `content-length` | Require classified nonempty values; validate Content-Length against each sanitized body, not across providers; do not compare transport values |
| Replay diagnostic | `x-swath-replay-error` | `x-swath-replay-error` | Replay-only reason; nonempty when present, never equated to a native header |

Unknown response fields or attributes require review in the future capture campaign.
Missing objects, boundary errors, wrong types, duplicate prefixes, and encoded-name
changes must become comparison failures before conformance can advance. Offline
`GcsProfileComparator` and `AzureProfileComparator` tests now enforce field types,
exact membership/order, namespace mapping, synthetic profile values, and omitted
metadata against adversarial sample pages. Native captures still need to exercise
every row before any behavior becomes `OBSERVED`. The parsers and bounded token walker live in
`swath-replay/src/conformance/java/io/varve/swath/replay/conformance/provider/`;
they do not call replay pager logic.

`ProviderExchangeComparator` adds status, classified header and error-envelope
checks to those body policies. It checks GCS JSON error reasons, Azure XML and
`x-ms-error-code`, the requested Azure service version and client request ID,
content type, and body length. Date and provider request IDs are validated but
their values are volatile; Content-Type media types are compared while charset
parameters may differ. Azure BOM and XML prolog spelling are compared as raw
preamble bytes. Sanitization keeps only classified response headers, hashes
provider request IDs, and adjusts retained Content-Length to sanitized body bytes;
an unclassified response header halts the capture for review. Local tests exercise
these rules; native status, header, and error behavior remains UNMEASURED.
GCS JSON sanitization re-encodes its body, so the `gcs-08` raw-spelling row cannot
advance without a separately reviewed, sanitized raw-wire artifact and checksum.
Native captures may establish that a provider repeats a rolled prefix across pages;
that observation can be recorded without changing the profile. The current replay
comparison refuses a repeated GCS prefix or Azure BlobPrefix explicitly. It never
silently deduplicates one to make a walk appear to match. Azure walk checks each
page's Marker echo against that page's own requested marker and checks Prefix,
Delimiter, and MaxResults echoes against their request values; page size may change
between pages while Prefix and Delimiter remain bound to the same scope.

`scripts/provider-conformance/evidence.py` validates a corpus manifest, the checked-in
ledger and this matrix, and sanitizes independently captured HTTP evidence before
calculating its SHA-256.
For an `exchange` row its input is one JSON exchange with `request.method`, absolute `request.url`,
header name/value pairs, and `response.status`, headers, and `body_base64`. Run
`evidence.py sanitize --input RAW --output SAFE --provider gcs --bucket LIVE_BUCKET
--manifest MANIFEST --probe-id gcs-01` or substitute
`--provider azure --account LIVE_ACCOUNT --container LIVE_CONTAINER` for Azure.
The raw input must also contain `captured_at` in ISO form. The script retains only
named listing headers/query fields, removes the origin, replaces namespace fields
structurally, hashes opaque tokens so consecutive requests can still be linked,
and refuses unknown query fields so a SAS parameter cannot slip into a capture.
For `walk`, `sdk_run`, and `retry_run` rows, pass `--kind` and a raw JSON object
with `steps: [{"phase":"walk","exchange":{...}}, ...]`. Retry runs use one
`retry_disabled` step and at least two `normal_retry` steps. The sanitizer
checks complete, bounded native token chains and emits `provider-capture-run-v1`;
the ledger refuses a single exchange for those rows. Each sanitized request
retains a SHA-256 of the raw token spelling as well as its strict-decoded opaque
token digest. Malformed percent escapes and invalid UTF-8 tokens are refused.
It retains only classified SDK product/version tokens from User-Agent and
x-goog-api-client, removing platform, host and project strings. SDK rows require a
captured token exactly matching the manifest's `sdk_product/sdk_version`.
Azure native captures must use the real account host and `/{container}` path over
HTTPS. The sanitizer checks the raw `ServiceEndpoint` scheme and trailing slash,
then replaces only the account label; it also hashes client request IDs while
checking the response echoed the request value. Error messages/details that can
carry request IDs are redacted without removing the typed error code or changing
the native XML BOM and declaration. Azure SDK replay receipts require the same
explicit client request ID on the captured and replay request, with each response
echo checked independently.
Azure `HEAD` 4xx/5xx error probes retain an empty body and compare status, service version,
error-code and classified headers; nonempty `HEAD` bodies and `HEAD` success
captures are outside this provisional evidence profile. `GET` errors still require
their typed XML envelope and native preamble.
SDK run receipts also require every retained native product/version token to appear
in the matching replay request header. Use the same pinned SDK and JDK for both
runs; a changed runtime or transport may alter auxiliary SDK tokens even when
the listing response is unchanged.
`AzureNamespaceMapping` compares that sanitized native host-style path and
ServiceEndpoint with replay's `/{replay-account}/{container}` path and local
`/{replay-account}/` ServiceEndpoint explicitly; it does not infer equivalence
from a user agent or authentication header.
It never rewrites object names: if a private identifier occurs in a name, sanitization
fails for manual review. Review the sanitized output for additional private data before
publishing it; the printed SHA-256 identifies those exact sanitized bytes.

Run `evidence.py validate --ledger docs/replay-provider-ledger.json
--matrix docs/replay-provider-conformance.md` to check the current UNMEASURED rows.
Promotion to OBSERVED also requires the provider's `--manifest MANIFEST`, a capture
under `--captures DIR`, exact probe request and expected result, and a checksum that
matches that capture's bytes and manifest link. REPLAY_MATCH additionally requires
`--repo-root DIR`, an existing test method, and a checksummed passing result receipt
linked to the same capture. `ProviderMatchReceipt` emits a PASS receipt only after
its GCS or Azure exchange or complete-run comparison succeeds, including the
captured and replay request scopes after explicit namespace and continuation-token
mapping. It requires a clean tested source tree and binds the test source, Git
commit and distribution SHA-256. Validation accepts a later commit only when the
receipt commit is an ancestor and the test source and distribution hashes still
match; pass `--distribution REPLAY_JAR` to
validate against that binary. Structural validation does not replace reviewing
the provider exchange or rerunning the named test.
Rows that bundle multiple boundary or error cases (gcs-01/03/06 and
azure-04/08/12) are intentionally blocked from promotion until they are split
into case-specific ledger rows during the deferred live campaign preparation;
one exchange cannot establish every case in those rows.
Pass `--manifest` once for GCS and once per Azure service version used by promoted
rows; the validator rejects a row if any required version lacks its own matching
manifest, request/response version headers, capture and replay result.

Azure replay fixture timestamps are provisionally representable only in UTC
years 1 through 9999; out-of-range values receive a typed fixture rejection.
This is a replay encoding bound, not an observation about native Azure limits.
