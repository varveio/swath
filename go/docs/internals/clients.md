# The list clients

Each object-store provider lists through one client, built on the vendor's SDK service client:
`internal/source/s3/client` over `service/s3` (ListObjectsV2 and ListObjectVersions),
`internal/source/gcs/client` over Google's generated `google.golang.org/api/storage/v1` (`objects.list`), and
`internal/source/azblob/client` over `sdk/storage/azblob` v1.7.0 (List Blobs and Get Account Information). The SDK
sends the request; swath keeps what a listing tool must guarantee and the SDKs do not. The providers above the
clients are described in [providers.md](providers.md) and the page of each provider.

## What the SDK owns

- **Requests.** Building and serializing the call: query parameters, escaping, the request path. S3 also endpoint
  resolution (virtual-hosted and path-style addressing, FIPS and dual-stack hosts, partitions, custom endpoints).
- **Authentication.** S3: the AWS credential chain (environment, shared config and profiles, SSO, web identity and
  STS, IMDS) and SigV4 signing. GCS: Google's credential loading (service-account and authorized-user files,
  Application Default Credentials, the metadata server) and token refresh (`golang.org/x/oauth2`). Azure: Entra ID
  through `DefaultAzureCredential` (environment, workload identity, managed identity, the Azure CLI and Developer CLI
  logins) and Shared Key signing; a SAS is part of the account URL the SDK client is built on.
- **Decoding**, except where a paragraph below says swath decodes (S3's listing pages) or checks what was decoded.
- **The transport**, under the Guard (below).

## What swath keeps

- **One retry owner.** Every SDK client is built with its retries off (`aws.NopRetryer`; the generated GCS `Do`
  makes one request; azcore `MaxRetries: -1`), so one SDK call is one attempt. `objhttp.Do` retries with
  full-jitter backoff, honours `Retry-After` up to the request timeout, admits each attempt through the limiter and
  the request rate, counts attempts, retries, throttles, timeouts and parse failures, and hands an exhausted
  failure to the engine, which requeues the range; a run never fails on a transient answer. The counted unit is an
  application attempt: `net/http` may replay an idempotent GET on a reused connection below it.
- **Credentials before the attempt.** Credentials and bearer tokens are fetched, and waited for under the attempt's
  deadline (all waiters sharing one outstanding call, `tokensource.Shared`), before an attempt is counted: a
  credential failure is a `*objhttp.CredentialError` (class `auth`, or retried when transient) and no request is
  sent. Azure's Entra token goes in as a request header, never through azcore's bearer policy and its 401
  challenge re-send (`TestBearer`).
- **Explicit anonymity.** Requests go unsigned only with `--anonymous`. Without it, credentials the SDK's chain does
  not find or cannot load fail the run with `auth` before anything is listed; swath never falls back to anonymous
  requests, and the command's error ends with "(a public bucket needs --anonymous)".
- **`objhttp.Guard`**, an `http.RoundTripper` below each SDK client: it reads a 2xx body up to its cap (16 MiB; Azure
  64 MiB, past which the request fails with `ErrBodyCap`, terminal) and hands it to the provider's validation
  before the SDK decodes it, so an empty body, a `null`, another operation's envelope or an embedded error is a
  `parse` failure, retried, and never an empty final page. Another status has its body cut to 64 KiB. A non-2xx
  whose body read is cut (the connection closed mid-body) keeps its status and the bytes read, so the status still
  decides its class; only a cut 2xx is a transport failure (`TestGuardCutErrorBodyKeepsAnswer`, and a
  real-truncation row in each provider's answers table). The original body is closed once on every path. A Guard
  failure is a `*GuardError`, recovered through `objhttp.Cause`, so the `url.Error` `net/http` wraps around it is
  never read as a retryable connection failure.
- **Redirects are answers**, never followed, also through a supplied HTTP client: the resume identity freezes where
  requests go. Azure drops the `Location` header before `net/http` sees it, so a malformed one never reaches an
  error.
- **The transport pool** (GCS and Azure, `objhttp.Pool`): one transport per 16 workers, 1 to 8, because Go's HTTP/2
  transport opens a second connection only at the server's stream limit; the transports share one TLS session
  cache. A supplied HTTP client is used alone. Each provider installs its own Guard and SDK client on every pooled
  transport; the report's `transport.pool_size` is the pool's size.
- **Secret-safe diagnostics.** An endpoint URL with user information is refused at construction, so no password
  reaches the journal identity or the report (`TestEndpointUserinfoRefused`). The SAS is scrubbed by parameter name
  from every error, 400 query echoes and SDK decoder errors included (`TestSASRedacted`, `TestScrubSASParameters`,
  `TestRedactedRefusal`). The SDKs' own logs, which quote request headers or URLs, are off: S3 and GCS give their
  clients a logger that discards everything (`TestSDKLogsNothing`), and the command turns azcore's off at startup
  (`TestAzureSDKLogSuppressed`).
- **Identity and admission.** The resume identity is the inputs the listing was asked with (bucket or container,
  prefix, endpoint, region, addressing flags), not a resolver's route; each provider page lists its fields.
  Admission (S3's directory-bucket refusal, Azure's flat-namespace check) is the provider's.

**The failure vocabulary.** Each failure reaches the engine, the counters and the limiter with its transient,
throttle and timeout signals and, for an answer, its HTTP status and the service's code or reason; the report's
error classes are the ones [cli.md](../cli.md#run-report) documents. The mapping is `objhttp`'s (`Normalize`,
`Classify`, `Do`) with each provider's table in its `classify.go`, held by the shared contract
`objhttptest.Contract` that every client passes (`TestRetryContract`).

**The User-Agent.** swath's product token (`buildinfo.UserAgent`, `swath-go/<version>`) comes first and the SDK's own
string follows, as the Java tool's `USER_AGENT_PREFIX` does: S3 through a build middleware inserted after the
SDK's, GCS by setting the header before the generated client's, Azure through azcore's telemetry policy
(`TelemetryOptions.ApplicationID`, which azcore cuts to 24 bytes; a development token is 23). `TestUserAgent`,
`TestCredentialsAndHeaders` and `TestServiceVersionPinned` pin it.

## S3 decoding

swath's scanner (`xml.go`) is the only decoder of listing pages. The Guard captures the validated body
(`objhttp.Capture`) and the SDK decodes a minimal replacement envelope in its place. The scanner is kept for two
measured reasons: through the same SDK stack, over a 1,000-object page with URL-encoded keys, the SDK's decoder
cost about 14 µs per object and about 102,000 allocations per page, the scanner about 1.8 µs and about 400
([measurements](../measurements.md#sdk-backed-clients-2026-11-09)); and a versioned listing needs `<Version>` and
`<DeleteMarker>` entries in wire order, which the SDK's two arrays cannot give back
(`TestStockSDKLosesVersionsOrder`, `TestVersionsKeepWireOrder`). The scanner reads the operation's own root element
and refuses anything else, so an empty body or another operation's envelope is a parse failure; it allocates per
page, not per object; keys, prefixes and markers are requested with `encoding-type=url` and decoded once; version
IDs are kept verbatim; a truncated versions page without a `NextKeyMarker` is an invalid page.
`TestScannerConformsToSDKDecoder` holds it to the SDK's own decoder, field by field, on a fake bucket and on every
captured body. The Guard's validation therefore checks only the status: a 2xx other than 200 is an answer, read
through its `<Error>` body's code, else the status text, and an error's `Message` carries the status text when the
body has none. The request is the SDK's, signed with the SDK's clock and payload hash; there is no clock-skew
correction, because the SDK's retry path is off. A FIPS region alias (`fips-us-east-1`, `us-east-1-fips`) resolves as
the SDK resolves it, to the region's FIPS host; the identity records the region as given.

## GCS decoding

The generated client decodes the page, and swath checks what it decodes. The Guard's validation rejects a 200 that
carries a top-level `error` member (`"error": null` included), which the generated decoder would drop; a body is
parsed for that only when it can hold one (a backslash, or `"error"` in any letter case). The decoded page must
have `kind == "storage#objects"` (the fields selector, sent on Google's endpoint only, starts with `kind`, so an
empty listing names it too): a `{}` body is a parse failure, not an empty page. Every item must be present; a size
that does not fit `int64`, a negative size, a `null` item and a time that is not RFC 3339 are parse failures. Times
become Unix microseconds and generations stay exact (`TestTimeDeleted`, the captured generation mappings). The
endpoint is `client.Endpoint`'s one normalization, which the identity records: a literal trailing slash added,
escapes kept, a URI-template brace refused ([providers/gcs.md](providers/gcs.md)).

## Azure decoding and requests

The SDK's decoder reads the page, and the Guard's validation checks the envelope first, because the decoder reads an
empty body, any root element, or a listing holding an `<Error>` as an empty final page: a listing's 200 must have
`EnumerationResults` as its root, nothing but whitespace after it, and no `Error` element in it; the decoded page
must hold `Blobs`. Names are checked for UTF-8. A hierarchy page comes back from the SDK as two arrays, blobs and
prefixes; both are in wire (UTF-16 code-unit) order and no name is in both, so merging them with
`utf16order.Compare` restores the one sequence the byte-order adapter needs (`TestWireOrder`, `TestMergeOrder`); a
page out of order across the two kinds is merged into order and not detected, one out of order within a kind
still is (`ListOutput.Validate`). One per-call policy, `pin`, runs before azcore's signing policy and sets what
azblob v1.7.0 cannot: `x-ms-version` to the pinned `client.Version` (the SDK sends its own and refuses
`ClientOptions.APIVersion`, `TestServiceVersionPinned`) and `startFrom`, which the v1.7.0 pagers drop
(`TestStartFromSent`). No `x-ms-client-request-id` is sent, and an attempt's in-flight time includes the SDK's
decoding.

The SDK's readings, each pinned by `TestSDKDecoding`: an `Encoded` name is percent-decoded with query semantics (a
literal `+` reads as a space; Azure sends `+` as `%2B`, and every captured `Encoded` name escapes every reserved
byte), and a malformed escape is a parse failure, retried; a root element with a namespace is refused, a second
`Blobs` is not, an XML comment after the root is; any malformed date property, not only the three a row keeps,
fails the page, as does a malformed `Date` header on a listing.

`AccountInfo` goes through the SDK and reads `x-ms-is-hns-enabled` from the header as sent: exactly `true` or
`false`, anything else unknown (the SDK's own reading would take `0` for flat; `TestAccountInfoExact`).
`NamespaceInfo`, for anonymous admission, is swath's own HEAD (Filesystem Get Properties) on the account's Data Lake
host, which azblob does not have, reading `x-ms-namespace-enabled`; an endpoint other than
`https://ACCOUNT.blob.core.windows.net` has no such host, and with Shared Key it is not asked (`ErrNoDFSEndpoint`).
In both a refusal or a missing header is an error, never "flat". The error code of a non-2xx answer comes from
`x-ms-error-code`, else the body's `<Code>`; `AuthenticationFailed` arrives as a 403 and `ServerBusy` as a 503, so
the code decides before the status, and a 500 is retried only when it is `OperationTimedOut` or `InternalError`.

## Measurements

CPU per object, allocations and binary sizes of these clients against the hand-rolled clients they replaced, and the
whole-process comparison that decided the adoption, are in
[measurements.md](../measurements.md#client-cpu-per-object-and-binary-size) and its
[2026-11-09 section](../measurements.md#sdk-backed-clients-2026-11-09).
