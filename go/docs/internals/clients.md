# The list clients: hand-rolled on the SDK's auth

Each object-store provider makes one call, its list request, through a client of its own, and takes
authentication from the vendor's SDK modules. The default build sends it through a hand-rolled client, described
here. S3 and GCS can instead be built over the vendor's SDK service client with the build tag `swath_sdkclient`
(experiment; [below](#the-sdk-backed-clients-swath_sdkclient-experiment)).

**The SDK's.** S3: the credential chain (environment, shared config and profiles, SSO, web identity and STS,
IMDS) and the SigV4 signer (`aws-sdk-go-v2/config`, `credentials`, `aws/signer/v4`). GCS: Google's
credential loading (service-account and authorized-user files, Application Default Credentials, the metadata
server) and token refresh (`golang.org/x/oauth2`, `cloud.google.com/go/compute/metadata`). Azure Blob: Entra ID
tokens through `DefaultAzureCredential` (environment, workload identity, managed identity, Azure CLI) and the
`TokenCredential` type (`azidentity`, `azcore`); a SAS is a query string and Shared Key is signed in the client.

**Ours.** The one list call per provider (ListObjectsV2; `objects.list`), built and sent by
`internal/source/s3/client` and `internal/source/gcs/client` (the S3 client also sends ListObjectVersions, for
versioned listing, on the same path; its parser keeps `<Version>` and `<DeleteMarker>` entries in one slice in
wire order, decodes keys and the `NextKeyMarker` once, keeps version IDs verbatim, and refuses a truncated page
without a `NextKeyMarker` as an invalid page); S3's response parser, a hand-written scanner
over the body that allocates per page, not per object (an `<Error>` body, the cold path, goes through
`encoding/xml`); GCS's decoding of only the listed fields with
`encoding/json` into a fixed envelope; bodies read up to 16 MiB; envelope and page validation (a 200 that is
not a listing envelope is a `parse` failure and retried, never an empty final page); redirects never
followed; and, shared through `internal/source/objhttp`, the retry loop, failure classification, client
counters and the admission limiter.

**Why.** CPU per listed object, measured on fake buckets (synthetic; [measurements](../measurements.md#client-cpu-per-object-and-binary-size)): S3, the
SDK's client 8.7 µs against ours 1.0 (70 % of the SDK's in its REST-XML deserializer), and a stripped binary
36 % smaller; GCS, Google's generated JSON client 3.1 µs against ours 2.5, half the allocations per page (a
small gain: both decode JSON with the standard library), and a binary 47 % larger with that client linked.
And control: the durability reviews rely on points the SDK clients do not expose, such as the attempt-level
retry and requeue that never fail a run, one failure vocabulary across providers, liveness counted from
answered requests, the refusal of redirects (the resume identity freezes the route), bounded bodies and the
validation of every page.

**Azure Blob** (`internal/source/azblob/client`; the provider is in [providers/azblob.md](providers/azblob.md)). One call, List Blobs:
`GET {endpoint}/{container}?restype=container&comp=list` with `prefix`, `delimiter`, `startFrom`, `marker` and
`maxresults` only when set (never `endBefore` or `include`; spaces as `%20`, as the SDK sends them),
`x-ms-version: 2026-10-06` pinned (the replay also serves 2026-06-06; the two behave alike), a fresh
`x-ms-client-request-id` per attempt and the swath user agent, to the endpoint and with the credentials the
provider page describes ([providers/azblob.md](providers/azblob.md)). `AccountInfo` sends Get Account Information on the container path and reads `x-ms-is-hns-enabled`;
`NamespaceInfo` sends Filesystem Get Properties (a HEAD) on the account's Data Lake host and reads
`x-ms-namespace-enabled`, for anonymous admission (an endpoint other than `ACCOUNT.blob.core.windows.net` has
no such host, and so has any endpoint that is not exactly `https://ACCOUNT.blob.core.windows.net`: `ErrNoDFSEndpoint`); in both a refusal or a missing header is an error, never "flat". Every body starts with a UTF-8 BOM, which is stripped
before `encoding/xml`'s streaming decoder reads `EnumerationResults` one entry at a time, keeping `Blob` and
`BlobPrefix` in one slice in wire order and only the listed properties. A `<Name Encoded="true">` (a name with
U+FFFE or U+FFFF) is percent-decoded once with path semantics (`+` stays `+`); a malformed escape or invalid
UTF-8 is an invalid page, as are names that do not ascend strictly in UTF-16 code-unit order (byte order with
lead bytes F0..F4 before EE..EF, compared in the client) and a `NextMarker` equal to the marker sent; an empty or
absent `NextMarker` is the end (Azure has no truncation flag). A 200 must be a listing envelope:
`EnumerationResults` holding a `Blobs` element (empty for an empty page) and no `Error` element, with nothing
after its close; anything else is a `parse` failure, retried, never an empty final page. The body cap is fixed
at 64 MiB, the memory one in-flight response may hold (bodies are read whole, outside the engine's memory
budget). A page past the cap is a terminal refusal; the requested page size stays unchanged.
Normalization keeps the HTTP status and provider code with transient, throttle and timeout signals:
`ServerBusy` and 429 throttle, `OperationTimedOut` and 408 signal timeouts; retried are 408, 429, 502, 503,
504 and a 500 only when it is `OperationTimedOut` or `InternalError`. Report labels are diagnostics,
independent of retry decisions, and a `Retry-After` (seconds or an HTTP date) longer than
the drawn backoff is waited out (`objhttp.RetryAfter`), at most the request timeout (a longer one is cut to it
and counted in `retry_after_capped`; the attempt budget still ends the request, which the engine requeues).
Redirects are never followed and their target is never quoted: the client's transport drops `Location` from
every 3xx, and request errors, 400 query echoes and messages are scrubbed of the SAS through their whole chain.
`TestClientPagesMatchSDK` walks the replay's Azure route with the client and the
SDK's flat and hierarchy pagers; `TestCaptures` serves the live captures of the evidence archive to the client.

In the default build the SDK service clients (`service/s3`, `google.golang.org/api/storage/v1`,
`sdk/storage/azblob`) are test-only oracles (`internal/source/s3/sdkoracle`, `TestClientPagesMatchSDK`,
`TestClientPagesMatchGoogleClient`); the binary links none of them ([testing.md](../testing.md);
`TestBinaryLinksAzidentityNotAzblob`). The `swath_sdkclient` build links all three, as the next section
describes.

## The SDK-backed clients (`swath_sdkclient`, experiment)

Experimental, and built only with `-tags swath_sdkclient`. `internal/source/s3/sdkclient`,
`internal/source/gcs/sdkclient` and `internal/source/azblob/sdkclient` take the same `client.Config` as the
hand-rolled clients and return the same wire types (`client.Page`, `client.VersionPage`, `client.ListOutput`), so
the provider code above them, the schemas and the resume identity do not change.

**What the SDK owns.** S3 (`service/s3`): request construction, endpoint resolution (virtual-hosted and
path-style, FIPS, dual-stack, custom endpoints), and credentials and SigV4 signing for the credentials swath
resolves for it. GCS (`google.golang.org/api/storage/v1`): building the `objects.list` URL and decoding the JSON
page. Azure (`sdk/storage/azblob` v1.7.0, the service and container clients): building the List Blobs and Get
Account Information requests, Shared Key signing, and decoding the XML page. All three clients are built with the
SDK's retries off (`aws.NopRetryer`; the generated GCS `Do` makes one request; azcore `MaxRetries: -1`), so one
SDK call is one attempt.

**What swath keeps.**

- *One retry owner.* `objhttp.Do` retries, backs off, honors `Retry-After` and admits requests through the
  limiter, as for the hand-rolled clients. The SDK never retries.
- *Normalized errors and attempt accounting.* `objhttp.Normalize` computes one `*objhttp.Failure`,
  reachable through `errors.As`: transient/throttle/timeout signals, HTTP status, provider code or reason,
  and a cause that has passed the provider's redaction boundary. Admission, counters, retry, reporting
  and engine requeue consume that value; exhaustion wraps it. `errors.Is` still reaches cancellation and
  invalid-page sentinels. Diagnostic labels are `auth`, `access_denied`, `throttle`, `timeout`, `parse`
  and `unsupported`; other errors keep their status/code without a report label. Credentials and bearer
  tokens are fetched before the attempt is counted, so a credential failure is a
  `*objhttp.CredentialError`, with no listing attempt counted.
- *Redirects refused, and route identity.* The HTTP client follows no redirect. `Route` is the URL the SDK's
  endpoint resolver names (S3) or the generated client sends (GCS), and the resume identity freezes it. S3 tests
  that every request, retried and continuation requests included, goes to exactly `Route`. A GCS endpoint whose
  path the generated client would send differently (an escaped separator or a dot segment) is refused when the
  client is built, with a message to use the hand-rolled client. The Azure clients are built on the hand-rolled
  client's account root (`Core.Base`), so `Route` is the same URL in both builds.
- *`objhttp.Guard`.* An `http.RoundTripper` below the SDK, shared by the three clients. It reads a success body
  up to 16 MiB (Azure: 64 MiB) and an
  error body up to 64 KiB (longer success bodies fail the round trip; longer error bodies are truncated), closes
  the original body on every path, and validates every 2xx before the SDK decodes it. A Guard failure surfaces
  through `objhttp.Cause`, so the `url.Error` the HTTP client wraps around it is never read as a transport
  failure.
- *The User-Agent.* Swath's product token (`buildinfo.UserAgent`, `swath-go/<version>`) comes first and the SDK's
  own string follows, as the Java tool's `USER_AGENT_PREFIX` does: S3 through a build middleware inserted after
  the SDK's, GCS by setting the header before the generated client's, Azure through azcore's telemetry policy
  (`TelemetryOptions.ApplicationID`, which azcore cuts to 24 bytes; a development token is 23).
- *`Stats.Client`* names the client that ran: `sdk` here, `hand-rolled` in the default build.

**S3 decoding.** The hand-rolled scanner (`client.ParseList`, `client.ParseVersions`) is the only decoder of
listing pages in the tagged build. The Guard captures the validated body (`objhttp.Capture`), and the SDK decodes
a minimal replacement envelope in its place. The scanner is kept for two measured reasons. Through the same SDK
stack, over a 1,000-object page with URL-encoded keys, the SDK's decoder cost about 14 µs per object and about
102,000 allocations per page, the scanner about 1.8 µs and about 400 ([measurements](../measurements.md#sdk-backed-clients-2026-11-09)).
And a versioned listing needs `<Version>` and `<DeleteMarker>` entries in wire order, which the SDK's two
separate arrays cannot give back (`TestStockSDKLosesVersionsOrder`). The scanner reads the operation's own root
element and refuses anything else, so an empty body or another operation's envelope is a parse failure and never
an empty final page. For that reason the S3 Guard's `Validate` checks only the status: a 2xx other than 200 is an
answer, read through `client.ResponseError` (an `<Error>` body's code, else the status text).
Keys, prefixes and markers are requested with `encoding-type=url` and decoded once, by the scanner.

**GCS decoding.** The generated client decodes the page, and swath checks what it decodes. The Guard's `Validate`
rejects a 200 that carries a top-level `error` member (`"error": null` included), which the generated decoder
would drop. A body is parsed for that only when it can hold one (a backslash, or `"error"` in any letter case).
The decoded page must have `kind == "storage#objects"`, and every item must be present. A size that does not fit
`int64` and a time that is not RFC 3339 are parse failures. The fields selector starts with `kind` in both
clients.

**Azure decoding and requests.** The SDK's decoder reads the page, and the Guard's `Validate` checks the envelope
first, because the decoder reads an empty body, any root element, or a listing holding an `<Error>` as an empty
final page: a listing's 200 must have `EnumerationResults` as its root, nothing but whitespace after it, and no
`Error` element in it; the decoded page must hold `Blobs`. Names are checked for UTF-8. A hierarchy page comes
back from the SDK as two arrays, blobs and prefixes; both are in wire (UTF-16 code-unit) order and no name is in
both, so merging them with `utf16order.Compare` restores the one sequence the byte-order adapter needs
(`TestDifferentialReplay`, `TestMergeOrder`); the merged page then passes `ListOutput.Validate` as the hand-rolled
page does. One per-call policy, `pin`, runs before azcore's signing policy and sets what azblob v1.7.0 cannot:
`x-ms-version` to the pinned `client.Version` (the SDK sends its own and refuses `ClientOptions.APIVersion`,
`TestServiceVersionPinned`) and `startFrom`, which the v1.7.0 pagers drop (`TestStartFromSent`). Credentials:
anonymous, SAS (in the account root the client is built on) and Entra ID use the SDK's no-credential client, and
the Entra token comes from swath's `tokensource.Shared` before the attempt is counted and goes in as a request
header (`policy.WithHTTPHeader`), so the SDK's bearer policy and its 401 challenge re-send are never in the
pipeline (`TestBearer`: one request per 401); Shared Key is the SDK's own signing policy. `AccountInfo` goes
through the SDK and reads `x-ms-is-hns-enabled` from the header as sent, through the same `client.HNS` as the
hand-rolled client (exactly `true` or `false`; the SDK's own reading would take `0` for flat). `NamespaceInfo`
stays the hand-rolled client's HEAD on the Data Lake host; with Shared Key it is not asked (`ErrNoDFSEndpoint`).
The SAS is kept out of every error: the SDK's decoder errors quote what they could not read, so each refusal is
scrubbed and re-wrapped (`TestRedactedRefusal`), and azcore's own log (written to stderr when
`AZURE_SDK_GO_LOGGING=all`; it quotes failed requests' URLs) is replaced, process-wide, by one that scrubs each
SAS client's token (`TestLogScrubbed`). `TestScriptedAnswers` holds the hand-rolled and SDK clients to the same
class, requeue decision, error, page, counters and body-close counts over about forty scripted answers.

**Selection.** The build tag chooses the client for all three providers; there is no runtime flag. The default
build uses the hand-rolled clients. `SWATH_S3_DECODER=sdk` makes the tagged S3 client decode current-object pages
with the SDK's decoder instead of the scanner. It exists to measure the decoders against each other and is not
a production path: nothing checks the envelope there, and the SDK reads an empty body as an empty final page.
Versions always go through the scanner.

**Binary.** The default binary links no SDK service client. `TestSDKClientBuildTag` (GCS and Azure) and
`TestSDKClientOnlyWithTag` (S3) run `go list -deps` of `cmd/swath` with and without the tag and require the SDK
packages to be present only with it. The tagged GCS build is about 9 MB larger; the Azure client adds about
143 KB, since azcore and azidentity are linked in the default build already
([measurements](../measurements.md#sdk-backed-clients-2026-11-09)).

**Known differences from the hand-rolled clients.**

- *GCS.* All stricter. A `{}` body without `kind` is a parse failure, not an empty page (the servers swath talks
  to send `kind`, an empty listing included). A negative size is a parse failure. A `null` item is a parse
  failure.
- *S3, region.* A FIPS region alias (`fips-us-east-1`, `us-east-1-fips`) resolves as the SDK resolves it: the
  region without the alias and FIPS on, giving `https://BUCKET.s3-fips.us-east-1.amazonaws.com/`. The hand-rolled
  client takes the alias as a region name, so a journal it wrote with an alias region does not resume through the
  SDK client.
- *S3, errors.* `Message` carries the status text when the error body has none. On a 2xx other than 200 the
  request ID and region come only from an `<Error>` body, not from headers.
- *S3, request.* `cfg.Now` is not used: the SDK signs with its own clock. The payload hash is the SDK's. There is
  no clock-skew correction, because the SDK's retry path is off.
- *Azure, names.* An `Encoded` name is percent-decoded with query semantics (a literal `+` reads as a space; Azure
  sends `+` as `%2B`, and every captured `Encoded` name escapes every reserved byte), and a malformed escape is a
  parse failure, retried, not an invalid page. A root element with a namespace is refused; a second `Blobs` is
  not; an XML comment after the root is refused (the hand-rolled parser allows it).
- *Azure, pages.* A hierarchy page out of order across blobs and prefixes is merged into order and not detected
  (the hand-rolled parser refuses it); one out of order within either kind still is. Any malformed date property,
  not only the three a row keeps, fails the page, as does a malformed `Date` header on a listing. No
  `x-ms-client-request-id` is sent, and the attempt's in-flight time includes the SDK's decoding.
- *Transport replays.* `net/http` may replay an idempotent GET on a reused connection below the Guard, for both
  families of client. The counted unit is an application attempt.
