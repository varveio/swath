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
- **Service-call transport integration.** The SDK sends through the Go HTTP clients swath supplies;
  [pool construction](#pool-construction) and the Guard below govern those clients.

## What swath keeps

- **One retry owner.** Every SDK client is built with its retries off (`aws.NopRetryer`; the generated GCS `Do`
  makes one request; azcore `MaxRetries: -1`), so one SDK call is one attempt. `objhttp.Do` retries with
  full-jitter backoff, honors a provider-supplied `Retry-After`, admits each attempt through the limiter and
  the request rate, counts attempts, retries, throttles, timeouts and parse failures, and hands an exhausted
  engine listing failure to the engine, whose `Description.Requeue` is `objhttp.Requeue`: only an exhausted
  call with a stored transient signal is requeued. Provider admission runs before the engine exists and
  can refuse instead; see [Azure admission](providers/azblob.md#provider-implementation). A nil
  requeue predicate does not authorize requeue. The counted unit is an application attempt: `net/http` may replay an idempotent GET on a reused connection below it.
- **Credentials before the attempt.** Credentials and bearer tokens are fetched, and waited for under the attempt's
  deadline (all waiters sharing one outstanding call, `tokensource.Shared`), before an attempt is counted: a
  credential failure is a `*objhttp.CredentialError` (class `auth`, or retried when transient) and no request is
  sent. Azure's Entra token goes in as a request header, never through azcore's bearer policy and its 401
  challenge re-send (`TestBearer`).
- **Explicit anonymity.** Requests go unsigned only with `--anonymous`. Without it, credentials the SDK's chain does
  not find or cannot load stop the run before listing; swath never falls back to anonymous
  requests. Credential failures normally have class `auth`; failed Entra acquisition during Azure
  namespace admission can instead yield `refused`, cause `unsupported` (exit 2), when flatness
  cannot be established. Provider-specific diagnostics describe the credential source.
- **`objhttp.Guard`**, an `http.RoundTripper` below each SDK client: it accepts 2xx bodies of at most
  16 MiB for S3/GCS or 64 MiB for Azure, then invokes provider-specific validation before passing the
  body upward. These are accepted-body byte limits, not memory limits: the buffer can have spare
  capacity (and grow while reading up to cap + 1 bytes), and decoded rows and strings allocate separately.
  Bodies and decoded pages sit outside the engine's row budget; worker calls and concurrent seed/probe
  calls each hold their own allocations. An Azure body past its cap is a terminal refusal (`ErrBodyCap`),
  with no page-size reduction. Non-2xx bodies are truncated to 64 KiB for diagnostics. A cut non-2xx
  keeps its status and bytes read; a cut 2xx is a retryable connection failure. The original body is
  closed once on every path. Guard failures are `*GuardError`, recovered through `objhttp.Cause`,
  so the wrapping `url.Error` is never mistaken for a retryable connection failure.
  Guard validation is only part of page validation: S3 scans its captured listing body after the SDK
  call; GCS decodes in the SDK, then maps and validates rows; Azure checks the XML envelope in the
  Guard, then checks and maps the SDK result. Malformed-page failures can therefore occur after the
  Guard delivers status 200. A captured 200 does not certify a valid listing page.
- **Redirects are answers**, never followed, also through a supplied HTTP client: the resume identity freezes where
  requests go. Azure drops the `Location` header before `net/http` sees it, so a malformed one never reaches an
  error.
- **The transport pool** (`objhttp.Pool`): see [pool construction](#pool-construction). S3 takes one client;
  GCS and Azure size their pools by workers and rotate their SDK services.
- **Secret-safe diagnostics.** An endpoint URL with user information is refused at construction without quoting
  it (`source` `TestRegisteredEndpointRefusals`). `registry.RedactArgv`, shared by the command and listing, replaces the value
  of every URL flag the shared flag groups declare (`source.FlagGroup.URLs`: the store group's `--endpoint-url`,
  which every object store takes) with `[redacted]` when it carries user information or
  cannot be parsed, in either argument spelling. Valid endpoints without user information are retained for
  resume. This covers failure reports, successful reports and journal arguments, including recorded arguments
  supplied to listing (`cmd/swath` `TestEndpointUserinfoReports`, `listing` `TestRecordedEndpointUserinfo`,
  `registry` `TestRedactEndpointArgv`).
  The SAS is scrubbed by parameter name from every error's code and message, 400 query echoes and SDK decoder
  errors included (`TestSASRedacted`, `TestScrubSASParameters`, `TestRedactedRefusal`). The SDKs' own logs, which quote request headers or URLs, are off: S3 and GCS give their
  clients a logger that discards everything (`logging.Nop` for S3; `gcs/client` `TestSDKLogsNothing`), and the command turns azcore's off at startup
  (`TestAzureSDKLogSuppressed`).
- **Identity and admission.** The resume identity is the inputs the listing was asked with (bucket or container,
  prefix, endpoint, region, addressing flags), not a resolver's route; each provider page lists its fields.
  Admission (S3's directory-bucket refusal, Azure's flat-namespace check) is the provider's.

**The failure vocabulary.** `objhttp.Normalize` computes one `*objhttp.Failure` per failed attempt, reachable
through `errors.As`: transient, throttle and timeout signals, the HTTP status, the provider's code or reason, and a
cause that has passed the provider's redaction boundary. The shared vocabulary lives in `objhttp`, including
`Counters`, `CredentialError`, `ExhaustedError`, `ErrSyntax` and `ErrInvalidPage`; clients do not re-export it.
`objhttp.Label` supplies the factory's report label (`ClassOther` becomes an empty label). Admission, counters, the limiter, retry, reporting and the
engine's requeue consume that value; exhaustion wraps it, and `errors.Is` still reaches the cancellation and
invalid-page sentinels. The report's diagnostic labels are `auth`, `access_denied`, `throttle`, `timeout`, `parse`
and `unsupported`, the ones [cli.md](../cli.md#run-report) documents; other errors keep their status and code
without a label. Each provider's `Failure` method in its `classify.go` produces the signals from status and code,
held by the shared contract `objhttptest.Contract` that every client passes (`TestRetryContract`).

**The User-Agent.** swath's product token (`buildinfo.UserAgent`, `swath-go/<version>`) comes first and the SDK's own
string follows, as the Java tool's `USER_AGENT_PREFIX` does: S3 through a build middleware inserted after the
SDK's, GCS by setting the header before the generated client's, Azure through azcore's telemetry policy
(`TelemetryOptions.ApplicationID`, which azcore cuts to 24 bytes; a development token is 23). `TestUserAgent`,
`TestCredentialsAndHeaders` and `TestServiceVersionPinned` pin it.

## Retry policy and accounting

Every client `Config` embeds `objhttp.Retry`: maximum attempts (default 9), base backoff (100 ms), maximum
backoff (5 s), limiter and request-rate cap. Request timeout is separate; constructors use
`source.DefaultRequestTimeout` (30 s) when it is unset. A built client embeds `*objhttp.Meter`, made by
`NewMeter(cfg.Retry)`, which applies retry defaults and owns `Retry` plus the atomic `Tally Counters`.
Do not copy a used meter. Clients pass `Retry` to `objhttp.Do` and `&Tally` to attempt accounting;
`Answered()`, `Live() source.Requests` and `Counters() objhttp.Counts` read that same account. `Live` adds the
limiter's current limit, and `Counters` adds limiter and rate activity. Retry-loop `retries` and
`timeouts` include failures during credential waits, before HTTP accounting starts; either can
increase while `attempts` remains zero.

`objhttp.Activity` observes optional `Counters`, `Live` and `Answered` methods on the chosen client, whether
built or supplied. It owns no retry policy or counters. Providers embed `objhttp.Observe(client)` and use
`Activity.Store(set, first)` in their `Stats` method; an unsupported observation reads zero and does not
store counters. Pool-size and Azure capped-wait probes, counter order and JSON report structs stay
provider-owned.

`source.SetWatch` installs one process-wide `source.Watch`, which the command sets for a run (the command
imports no provider package): every objhttp Guard reports each round trip to its `Exchange` (method, URL,
status or transport error, latency with the body read), and `objhttp.Do` each failed attempt to `Failed` (the
normalized `Failure`'s class, status, code and cause, retried with its wait, or the last). The
command's request log (`-vv`, `-vvv`) and its persistent-failure warning read them; the URL's query is the
reader's to redact. Both are called on the request path and must return promptly (the command's stderr
writes are bounded); without a watch they cost one atomic load.

## Token waits

`objhttp.WaitToken(ctx, ts, source, timeout, transient)` waits on `*tokensource.Shared` for GCS and Azure.
A nil source needs no token. It returns `*objhttp.CredentialError` on failure, before a listing request is
sent or counted. A wait whose context ends is transient and timed out; another error uses the provider's
transient predicate and records a wrapped deadline as a timeout. The retry loop gives caller cancellation
precedence over a credential-internal deadline. `objhttp.TransientStatus` shares only the credential HTTP
predicate (status at least 500, 408 or 429); SDK error inspection and precedence stay in each client.
S3 retains its AWS retrieval and signing path and its bearer-header wrapper.

`tokensource.NewShared(src)` coalesces concurrent refreshes and caches tokens with `oauth2.ReuseTokenSource`.
A source that already caches keeps its expiry margin. Canceling a wait leaves the one refresh running, so
later waiters join it and no second refresh starts. The source's own exchange lifetime is separate from the
attempt's wait; the [GCS provider page](providers/gcs.md) describes metadata refresh's timeout boundary.

## Capture timing

One `objhttp.Capture`, installed by `WithCapture`, belongs to one attempt and is never shared across
goroutines. The Guard copies `URL` before sending, even when no response is delivered. `Status` and `Header`
mean a response delivered upward through the Guard: they are set only after the body is bounded and a 2xx
body passes validation. A body-read or validation failure therefore leaves status zero even if raw response
headers arrived. This distinction keeps GCS body-deadline classification and Azure decoder-refusal detection
aligned with what the SDK received. A non-2xx with a cut body is still delivered and retains its status.
`Body` holds the 2xx bytes accepted by the Guard; `Replace`, when set by validation's owner, is the minimal envelope the
SDK decodes instead. `TestCaptureDelivery` and `TestCaptureWithoutAnswer` in `source/objhttp` pin the timing.

GCS uses the capture directly for SDK errors and success status. Azure retains its per-attempt `sent` state
for `startFrom`, policy context, a scrubbed URL and decoder checks; S3 uses `Body` / `Replace` for its scanner.
The captured URL is not a diagnostic redaction boundary: each client must sanitize it before retaining it
in an error.

## Pool construction

`objhttp.Pool(workers, supplied, trust, wrap)` returns HTTP clients. With no supplied client it clones
`http.DefaultTransport` for each, retains the clone's TLS settings and sets `ClientSessionCache` on its TLS
configuration, and with `trust` (`source.Trust.Config`: `--ca-bundle`'s roots, or `--no-verify-ssl`) the roots
and whether they are verified; a supplied client keeps its own TLS settings. Transports share that cache, set `MaxIdleConns` to 0 (no global idle cap) and
`MaxIdleConnsPerHost` to 1,024. The size is `ceil(workers/16)`, clamped to 1..8: below a server's HTTP/2
stream limit a single transport uses one connection. S3 calls `Pool(1, ...)[0]`; GCS rotates generated
services; Azure rotates corresponding service and HTTP-client slices. Selection stays provider-owned.

With a supplied HTTP client the pool is one copy of it, preserving its timeout and transport (the default
transport when nil) and leaving the original unchanged. Every returned client refuses redirects. Each
provider installs its Guard and SDK client over the returned transport; Azure also removes `Location`
before `net/http` can parse it. GCS and Azure report `transport.pool_size`.

## Request admission

All three object-store providers build `objhttp.Admission.New(workers)` for their own clients, beside the
engine's `keyrange.Policy`. The CLI enables the adaptive limiter by default (`--no-adaptive-limit` disables
it); a library caller's zero `Admission` leaves it off. A supplied listing client owns its admission.
`Admission.Off` supplies feature states for reporting; the range engine does not admit HTTP requests.
Namespace admission (for example Azure's flat-namespace check) is a separate provider decision.

`objhttp.Do` takes a limiter permit, then a rate token, before invoking each attempt: data pages, seed and
structure probes, retries and requeues all use it. It releases the permit however the attempt ends, including
a panic, and holds none during backoff. Lowering the limit stops new grants until in-flight attempts drain;
it does not cancel requests already sent. Admission waits are excluded from latency samples.

The limiter starts at the worker count. Its window has `clamp(4 * workers, 256, 4096)` slots, recording usable
successes and throttle answers to attempts sent since the last cut. Unfilled slots count as successes.
A throttle at or above 15 % of the window multiplies the limit by 0.7, floored at 1, and clears the window.
A cut due at the floor changes neither the window nor its cut timestamp (`limiter.floor_throttles`).
Answers to attempts sent before the cut cannot cut again (`limiter.cuts_suppressed.round_trip`); a throttle
below the threshold is tolerated (`limiter.cuts_suppressed.tolerated`).

On successful answers, growth adds one permit at most once per second, up to the worker count, while the
window is below 7.5 % throttles and has filled since a cut, or after 10 s with no throttle answer.
Three attempt timeouts in the 10 s timeout window freeze growth. An EWMA of successful attempt latency
above twice its rolling-minimum baseline also freezes growth; the baseline re-floors every minute. During
a latency freeze a +1 valve step is allowed at most every 30 s with more than `max(1, limit/32)` successes
in that valve window. Timeouts end slow start and hold growth; they do not lower the limit.

`swath diag list --adaptive-limit-slow-start` starts at `min(4, workers)` and doubles at paced growth steps until
the worker count, the first cut or an attempt timeout. A tolerated throttle does not end the ramp, and the
latency valve remains +1. Slow start is off by default. Counters `limiter.*` and
`slow_start.{doubles,ramp_ns,end_limit}` distinguish engagement and brakes; feature `slow_start` distinguishes
a disabled ramp from an enabled one that did not engage.

`objhttp.Rate` (`--request-rate N`, disabled by default) reserves tokens `1/N` seconds apart,
with one token immediately available. Actual sends can bunch when a caller is delayed after its
reservation by scheduling. A canceled wait returns its reservation only if no later reservation
exists. `rate.*` and feature `request_rate` report it.
Azure parses `Retry-After` and caps it at the request timeout; `Do` takes the larger of that wait and its
jittered backoff for the receiving call. It does not pause the limiter or other callers. S3 and GCS do not
extract `Retry-After`.

The tuning evidence is S3-shaped synthetic listings and the simulated streams in `objhttp/limiter_test.go`:
`TestLimiterToleratesRandomThrottles` covers random 1 % and 5 % backgrounds, and
`TestLimiterPlateausUnderRateCap` covers rate caps. It does not establish fit for live S3, GCS or Azure
throttling. [Issue 385](https://github.com/varveio/swath/issues/385) tracks per-provider validation and tuning
gaps; its candidate rules are not implemented. Each provider page describes the signals it feeds this shared
controller.

## S3 decoding

swath's scanner (`xml.go`) is the only decoder of listing pages. The Guard captures the bounded body
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
In both a refusal or a missing header is an error, never "flat". If both checks fail, admission wraps both
causes, preserving credential refusal and the command's `--anonymous` hint (`TestPublicBucketHint`). The error
code of a non-2xx answer is `x-ms-error-code` when present; otherwise swath decodes the bounded XML body's
`Code`, including CDATA and XML character/entity references, falling back to azcore's `ResponseError.ErrorCode`
(which also accepts JSON). The header always takes precedence (`TestScriptedAnswers`). Both the code and the
XML message are scrubbed of the SAS by parameter name before storage; the stored code is then capped at 64 KiB
(`TestSASRedacted`). `NamespaceInfo`'s HEAD has no body: its code is the header's.
`AuthenticationFailed` arrives as a 403 and `ServerBusy` as a 503, so
the code decides before the status, and a 500 is retried only when it is `OperationTimedOut` or `InternalError`.

## Measurements

CPU per object, allocations, binary sizes and whole-process comparisons are recorded in
[measurements.md](../measurements.md#client-cpu-per-object-and-binary-size) and its
[2026-11-09 section](../measurements.md#sdk-backed-clients-2026-11-09).
