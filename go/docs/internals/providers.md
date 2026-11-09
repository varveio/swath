# Providers

**Does the range engine apply?** Dynamic range splitting (ranges, owner-side splits at synthesized pivots,
seeding) applies to a source only when all three hold: it has **no rank oracle** (nothing can say how many
keys precede a key, which would allow an exact partition instead), it has a **fabricable seek** (a listing can
start after any key the caller makes up, as S3's `start-after` and GCS's `startOffset`), and its pages are **byte-ordered** (every
page ascends in unsigned byte order and continues the last). A listing sorted in another order (UTF-16 code
units put U+10000 before U+E000, the reverse of byte order, and the disorder can cross pages) or unsorted does
not qualify, even if the provider sorts its output: it normalizes behind `source.Provider` instead. A source
with a token-only seek is listed as one sequential chain per root; one with no seek reads each range whole and
sorts it inside the provider. Each provider declares this in `source.Capabilities` (`Seek`: `fabricable`,
`token_only`, `none`; `Order`: `bytes`, `other`, `none`; `RankOracle`; `RangeEngine()` is the test, and
`keyrange.New` refuses a `Description.Listing` that fails it): S3 and GCS are fabricable and byte-ordered without a
rank oracle (`token_only` with `--sequential`); Azure Blob lists in UTF-16 code-unit order, and is byte-ordered
through its provider's order adapter, which serves every engine query in byte order (below); the filesystem has no seek and no order (a directory is a
range, read whole and sorted).

## The contract (`internal/source`)

```go
type Emitter[R, P any] interface {
    Rows(rows []schema.Row[P]) error // next rows of this range, ascending; the slice now belongs to the caller
    Child(r R) error                 // a child range whose whole subtree sorts here
}
type Provider[R, P any] interface {
    Root() R
    Expand(ctx context.Context, r R, e Emitter[R, P]) error
}
```

`P` is the provider's row payload (`fs.Entry`, `s3.Object`), bound to columns by its `schema.Schema[P]`, which
lives beside the provider.

A range is a provider-defined value. `Expand` emits its content in ascending key order: every key of a child's
subtree sorts after everything emitted before the child and before everything emitted after it. `Expand` may
call `Rows` many times (streaming) and runs concurrently for different ranges. Per-entry problems (permission
denied, a vanished entry) are counted by the provider; `Expand` returns an error only when the listing must
stop, including any error returned by the emitter (cancellation or a failed sink).

## Filesystem (`internal/source/localfs`)

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCALFS_ENGINE=portable`): `os.File.ReadDir` + `Lstat`, re-checking that an opened child directory is
  the one that was listed.
- **Tiers** (`--metadata`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `birth_time`).
- **Cancellation:** a huge directory stays cancellable. getdents checks the context every 64 reads, the sort
  is bracketed by checks, the portable engine checks every 1,024 entries, and stat checks as before.
- Symlinks are never followed (`O_NOFOLLOW`). Names are bytes: one that is not valid UTF-8 is listed with
  its exact bytes, like any other. Mount points are crossed; there is no `-one-file-system` yet.
- A link target is at most 4,096 bytes (`link_target`'s `schema.Spec.MaxBytes`, PATH_MAX): the writer's
  admission prices it at that width, and a longer target stops the run with the typed `schema.ErrValueTooLong`, as
  a key over its cap does; a target within it is written byte for byte.

## The list clients: hand-rolled on the SDK's auth

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

**Azure Blob** (`internal/source/azblob/client`; the provider is [below](#azure-blob-internalsourceazblob)). One call, List Blobs:
`GET {endpoint}/{container}?restype=container&comp=list` with `prefix`, `delimiter`, `startFrom`, `marker` and
`maxresults` only when set (never `endBefore` or `include`; spaces as `%20`, as the SDK sends them),
`x-ms-version: 2026-10-06` pinned (the replay also serves 2026-06-06; the two behave alike), a fresh
`x-ms-client-request-id` per attempt and the swath user agent, to the endpoint and with the credentials the
provider section describes ([below](#azure-blob-internalsourceazblob)). `AccountInfo` sends Get Account Information on the container path and reads `x-ms-is-hns-enabled`;
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
after its close; anything else is a `parse` failure, retried, never an empty final page. The body cap is fixed at 64 MiB, the
memory one in-flight response may hold (bodies are read whole, outside the engine's memory budget): a page past it
is asked again with `maxresults` halved, marker and query unchanged, down to 1 (`body_cap_shrinks`), so memory stays
bounded and a valid page is never refused; only a single entry past the cap is a `parse` failure. Classification takes `x-ms-error-code`
first, the body's `<Code>` when the header is absent, then the status, because `AuthenticationFailed` arrives as a
403 and `ServerBusy` as a 503: `auth` (`NoAuthenticationInformation`, `AuthenticationFailed`,
`InvalidAuthenticationInfo`, 401), `access_denied` (`AuthorizationFailure`, `AuthorizationPermissionMismatch`,
another 403), `no_such_bucket` (`ContainerNotFound`), `throttle` (`ServerBusy`, 429), `timeout`
(`OperationTimedOut`, 408), `server` (`InternalError`, another 5xx); retried are 408, 429, 502, 503, 504 and a 500
only when it is `OperationTimedOut` or `InternalError`, and a `Retry-After` (seconds or an HTTP date) longer than
the drawn backoff is waited out (`objhttp.RetryAfter`), at most the request timeout (a longer one is cut to it
and counted in `retry_after_capped`; the attempt budget still ends the request, which the engine requeues).
Redirects are never followed and their target is never quoted: the client's transport drops `Location` from
every 3xx, and request errors, 400 query echoes and messages are scrubbed of the SAS through their whole chain.
`TestClientPagesMatchSDK` walks the replay's Azure route with the client and the
SDK's flat and hierarchy pagers; `TestCaptures` serves the live captures of the evidence archive to the client.

In the default build the SDK service clients (`service/s3`, `google.golang.org/api/storage/v1`,
`sdk/storage/azblob`) are test-only oracles (`internal/source/s3/sdkoracle`, `TestClientPagesMatchSDK`,
`TestClientPagesMatchGoogleClient`); the binary links none of them ([testing.md](../testing.md);
`TestBinaryLinksAzidentityNotAzblob`). The `swath_sdkclient` build links all three, as the next subsection
describes.

### The SDK-backed clients (`swath_sdkclient`, experiment)

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
- *The failure vocabulary and attempt accounting.* Every failure maps to the classes of the hand-rolled client
  (throttle, timeout, server, redirect, auth, access denied, no such bucket, parse, other), and an attempt is
  counted from just before the SDK call until it returns. Credentials and bearer tokens are fetched before the
  attempt is counted, so a credential failure is a `*objhttp.CredentialError` and not an attempt (for Azure, a
  `*client.CredentialError`).
- *Redirects refused, and route identity.* The HTTP client follows no redirect. `Route` is the URL the SDK's
  endpoint resolver names (S3) or the generated client sends (GCS), and the resume identity freezes it. S3 tests
  that every request, retried and continuation requests included, goes to exactly `Route`. A GCS endpoint whose
  path the generated client would send differently (an escaped separator or a dot segment) is refused when the
  client is built, with a message to use the hand-rolled client. The Azure clients are built on the hand-rolled
  client's account root (`Core.Base`), so `Route` is the same URL in both builds.
- *`objhttp.Guard`.* An `http.RoundTripper` below the SDK, shared by the three clients. It reads a success body
  up to 16 MiB (Azure: 64 MiB, the hand-rolled client's cap, with the same `maxresults` halving above it) and an
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

## S3 (`internal/source/s3`)

**Client** (`internal/source/s3/client`). The ListObjectsV2 request, the retry policy and the response parser
are this package's. Credentials and SigV4 come from the AWS SDK's auth modules (`aws-sdk-go-v2/config`,
`credentials`, `aws/signer/v4`). The SDK's S3 service client (`service/s3`) is not linked into the binary:
`go version -m swath` lists no `service/s3`.

- *Request:* one signed GET with `list-type=2`, `prefix`, `start-after`, `continuation-token`, `max-keys`,
  `delimiter`, `fetch-owner` and `encoding-type=url`. The query is built sorted and SigV4-encoded (smithy-go's
  `httpbinding.EscapePath`), so the URL sent is the canonical query that is signed.
- *Endpoints:* the client supports these forms:
  - virtual-hosted `bucket.s3.REGION.amazonaws.com` (the default);
  - path-style (`-path-style`; also used automatically for bucket names with dots and for IP endpoints);
  - FIPS (`-fips`: `s3-fips.REGION…`) and dual-stack (`-dualstack`: `s3.dualstack.REGION…`), alone or
    combined;
  - the China partition (`*.amazonaws.com.cn` for `cn-` regions);
  - custom endpoints (`-endpoint`, S3-compatible stores).

  **Not supported:** access points and Multi-Region Access Points (ARNs), S3 Express One Zone directory
  buckets (a bucket name ending in `--x-s3` is refused before any request, exit 2, class `refused`, as Java:
  they do not list in lexicographic order; `TestDirectoryBucketRefused`), Outposts, the GovCloud-specific and ISO partitions beyond the plain regional host,
  `use_fips_endpoint` and `use_dualstack_endpoint` read from the shared config (use the flags), and
  automatic region redirects.
- *Signing:* the SDK's SigV4 signer with `UNSIGNED-PAYLOAD` (`x-amz-content-sha256`), plus the session token
  when the credentials have one.
- *Credentials:* `-anonymous` is authoritative: nothing is looked up and one warning is printed if `AWS_*`
  variables are set. Otherwise the SDK's default chain runs, with `-profile` selecting the shared-config
  profile. The chain covers:
  - environment keys (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`);
  - the shared `config` and `credentials` files and their profiles (`AWS_PROFILE`, `AWS_CONFIG_FILE`,
    `AWS_SHARED_CREDENTIALS_FILE`);
  - IAM Identity Center (SSO) profiles;
  - web identity (`AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`);
  - `credential_process`;
  - `role_arn` with `source_profile` or `credential_source` (AssumeRole through STS);
  - container credentials (ECS, EKS Pod Identity);
  - EC2 instance roles (IMDSv2).

  Verified by tests against a fake STS and the fake bucket (`TestCredentialSources`, `TestCredentialRefresh`,
  `TestCredentialChain`): environment keys, shared-file profiles, web identity, a role chained from web
  identity with `external_id` (passed to AssumeRole), and credentials that expire mid-listing (fetched again;
  requests are signed with the new keys, none fails). SSO, container credentials and instance roles are the
  SDK's own providers, not exercised here. **STS region:** the bucket's `--region` is the signing region for
  S3 only; the credential chain's STS calls use the environment's or the profile's region, else `us-east-1`
  (`TestCredentialSources` lists buckets in eu-west-2 and ap-southeast-2 and sees STS signed for us-east-1).
  Credentials are cached by the SDK and refreshed before they expire. **Fail closed** (`TestCredentialsFailClosed`):
  requests go unsigned only for an empty chain, one where no source is configured anywhere (no environment
  keys, `AWS_PROFILE`, web-identity token, container endpoint, metadata endpoint; no profile with keys, a role,
  a credential process, SSO or a login session; no EC2 sign in the DMI vendor, asset tag or hypervisor id, or
  IMDS disabled) and the lookup, bounded at 3 s, finds nothing. A configured source (or `--profile`) is waited
  for up to 30 s (a web-identity token file not yet mounted is polled for), both waits inside the command's
  context, so a signal or deadline ends them (`client.New` takes it); a lookup that fails for good is a
  `CredentialError` (class `auth`, exit 1); one that fails transiently (an STS 5xx or throttle, a cut
  connection, past the wait) keeps the source with a note on stderr, and each request's signing retries it
  (then the range is requeued, ending in 75 if it never answers), as a refresh failure mid-run does.
  Credential intent the SDK drops silently is refused before any request, as configured but broken
  (`CredentialError`, class `auth`; `TestPartialCredentialsRefuse`): an access key without its secret or the
  reverse (`AWS_ACCESS_KEY_ID`/`AWS_ACCESS_KEY`, `AWS_SECRET_ACCESS_KEY`/`AWS_SECRET_KEY`), a session token
  without both keys, `AWS_ROLE_ARN` without `AWS_WEB_IDENTITY_TOKEN_FILE`; a configuration the SDK cannot load
  (a profile that does not exist, a token file without a role) is class `auth` too. GCS has no such gap: a
  `GOOGLE_APPLICATION_CREDENTIALS` naming a missing file is refused already (`TestADCConfiguredButBroken`).
- *Region:* `-region`, else the SDK's resolution (`AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's
  `region`), else `us-east-1`. A 301 or `AuthorizationHeaderMalformed` answer names the bucket's region in
  the error.
- *Retries:* up to 9 attempts with full-jitter exponential backoff (100 ms doubling, capped at 5 s) on 5xx,
  429, `SlowDown` and other throttling codes, `RequestTimeout`, connection errors and cut bodies, and attempts
  past the per-request timeout (`-request-timeout`, default 30 s), and 200 answers whose body does not parse
  (a cut or garbled body is a transport fault: retried, counted as `parse_failures`, never a skipped page).
  Other 4xx answers are typed `*client.Error`
  values (status, code, message, request id) and are not retried. Attempts, retries, throttles and timeouts are
  counted in the summary.
- *Retry exhaustion never fails a run.* After its attempts, the client returns a `*client.ExhaustedError`; the
  provider asks the same page again after a jittered backoff (2 s doubling, capped at 60 s), for as long as
  the run lives, and counts `requeues`. Only the command's watchdog decides that a run cannot go on (no
  progress in key space for `--stall-timeout`: exit 75, resumable; the engine's `Frontier`, through
  `source.Advancing`, counts the pages that held an entry or ended a chain and empty continuing pages within
  the first 1,024 of their run, never a failed request; `objhttp.Counters.Answered`, through `source.Live`, is
  the fallback for a source without it). Every request, including probes and the seed page,
  goes through the same path.
- *Shared with GCS* (`internal/source/objhttp`): the retry loop (`objhttp.Do`: attempts, backoff, exhaustion),
  the class vocabulary, transport classification, `CredentialError` (an S3 credential refresh that fails is one:
  a credential service's 5xx, 408, 429 or throttling code, a transport failure or a timeout is transient, retried
  and requeued, `TestCredentialOutageRecovers`; a refused grant is `auth`; `Classify` reads a credential failure
  before a wrapped deadline, so a token exchange's own timeout is never taken for the caller's cancellation), the client counters and route canonicalization; `objhttptest.Contract` is the retry and
  classification contract both clients' tests run with their own wire answers (`TestRetryContract`). The
  report brief, coverage and capabilities come from `keyrange.Report` and `keyrange.Capabilities`. Each client
  keeps its answers' meaning (status, codes, reasons), its parser and its authentication.
- *Admission* (`objhttp.Admission`, the transport configuration both providers' options carry beside the
  engine's `keyrange.Policy`, and its one construction `Admission.New`: `objhttp.Limiter`, both clients; on by
  default in the command, `--limiter=false` turns it off; a library caller's zero `Admission` leaves it off;
  the engine only reports its features, `Admission.Off`): every HTTP attempt (pages, seed,
  structure and split probes, client retries, engine requeues) takes a permit before it is sent and returns it
  when its answer is read, so in-flight requests never exceed the limit. The limit starts at the worker count.
  The throttle signal (S3 429/503/`SlowDown`; GCS 429 and rate-limit reasons) is read over a *window*: the
  last 4 × workers answers (at least 256, at most 4,096) to attempts sent since the last cut.
  - *Cut:* a throttle answer multiplies the limit by 0.7 (floor 1; a cut due at the floor removes nothing,
    `limiter.floor_throttles`) only while at least 15 % of the window are throttles (so at least 39, an unfilled
    window's missing answers counted as successes), and empties the window. Answers to attempts sent before the
    last cut (now − latency earlier than the cut) describe a limit already cut: they are neither counted nor cut
    on (`limiter.cuts_suppressed.round_trip`), so a round trip of throttles cuts once. A throttle under the 15 %
    is tolerated (`limiter.cuts_suppressed.tolerated`).
  - *Growth:* +1 per second back to the worker count while the window, full since the last cut, holds under
    7.5 % throttles, or after 10 s without any throttle answer (the slow-rate fallback: at a few requests a
    second the window turns over slowly); otherwise held (`limiter.growth_cooldown`).
  - *The statistics.* A 5 % background over 256 answers reads 12.8 throttles with a standard deviation of 3.5;
    39 is 7.5 deviations above it, out of reach by chance over the thousands of overlapping windows a long run
    slides through. The merge review probed the first rule, 10 % of 4 × workers with a floor of 64, locally (build
    `04cbff74`, a 2M-key synthetic list through a static S3 fake at 50 ms with 5 % random throttles, one run per
    arm on a shared 4-core dev box): at 16 workers 7 random throttles cut, and the limit fell from 16 to 7
    (13.0 s against 9.6 s with the limiter off);
    at 10 % of 256 (3.7 deviations) a 120 s simulated run at 5 % still cut on some seeds. Hence the 256 floor at
    every worker count and the 15 %. Its price is reaction time at few workers: past a real cap 77 % of answers
    are throttles, so the 39 arrive within about 51 answers, three round trips at 16 workers, thirteen at 4.
  - *Why it diverges from Java's gauge,* which cuts on every throttle answer and grows only after 10 s
    without a decrease: one round trip of 64 throttled answers cut 64 to 1 there, and a load-independent
    background (S3 returns `SlowDown` from partitions at random; the client's retries absorb it) never left
    10 s clean, so 1 % throttled answers held the limit at the floor (red team: 59 s against 3.8 s with the
    limiter off; 5 %: 111 s). Growth's 7.5 % is half the cut's: between them (a limit just past a real cap, or a
    7.5-15 % background) the limit holds, neither cutting nor growing. A cap is then a plateau, not a sawtooth,
    where the limit one past the cap reads 7.5 to 15 % throttles (`TestLimiterPlateausUnderRateCap`: 15 per
    round trip holds at 17, 40 at 44); a cap of fewer than about 7 per round trip, where one permit past it is
    already over 15 %, still cuts and regrows. `TestLimiterToleratesRandomThrottles`: 1 % and 5 % stay at the
    worker count at 16 and 64 workers over four 120 s streams each, 25 % is cut. Kept from Java: the factor, the
    floor, the pace, the freezes below. `limiter.throttle_permille` and `limiter.limit` are the window and the limit at the end of the run. Growth (never the limit)
  is held while three or more attempt timeouts fell in the last 10 s, or while the EWMA of successful attempts'
  latency exceeds twice its baseline (a rolling minimum re-floored every minute), with one step per 30 s let
  through when there is progress. Its only inputs are the attempts' own latency and answers; no permit is held
  during a backoff, and admission waits are not latency samples. A permit is returned however the attempt ends,
  a panic included. The rules and constants are Java swath's
  `ConcurrencyGauge`. Java's slow start is behind `swath diag list --limiter-slow-start` (off by default until measured,
  `objhttp.NewSlowStartLimiter`): the limit starts at min(4, workers) and each paced growth step doubles it
  instead of adding one, the first double on the first success and then at most one per second (4 to 64 in
  3 s), under the same brakes (the latency valve's step stays +1); the first cut or attempt timeout
  ends the ramp for good (a tolerated throttle answer does not, where Java's first throttle answer does: a
  random background would end every ramp at 4 or 8). It applies to whatever builds the client, a resume included; the reason in Java is
  that a restart at the full width into a throttling endpoint (a resume after a stall) storms it at once.
  Counters `limiter.*`, the ramp's `slow_start.{doubles,ramp_ns,end_limit}` (feature `slow_start`: off without the
  flag, so an unengaged ramp and an absent one differ).
- *Rate cap* (`objhttp.Rate`, `--request-rate N`, off by default): after its permit every attempt takes a token
  from a bucket of one, refilled every 1/N, so attempts are spaced at least 1/N apart whatever the limit
  (`TestRateCapsAttempts`: 60 attempts with retries from 8 callers take at least 59/N, and with 2 permits never
  more than 2 in flight). A wait that the caller cancels gives its token back. Counters `rate.*` (feature
  `request_rate`).
- *Error classes* (`client.Classify`, one table test covers each): `auth`, `access_denied`, `no_such_bucket`,
  `redirect`, `throttle`, `timeout`, `server`, `transport`, `parse` (a body that does not parse),
  `invalid_page` (a page that breaks the listing contract: out of order, truncated without a token or a key
  in range), `unsupported` (a source the tool refuses, `source.ErrUnsupported`: an S3 directory bucket, an Azure
  account that is not positively flat; the run is `refused`, exit 2, its journal kept), `canceled`, `other`. `throttle`, `timeout`, `server`, `transport` and `parse` are transient
  (retried, then requeued); the rest stop the run, and the class is the run outcome's `cause`, except an
  invalid page, a protocol violation: `cause` `protocol`, the run marked unresumable
  ([durability.md](durability.md)).
- *Parsing:* a hand-rolled scanner over the whole body (read into a pooled buffer) with encoding/xml's
  semantics for what S3 sends: entities and character references, CDATA, comments, namespaces, `\r\n`
  normalisation, last occurrence wins, unknown elements (`RestoreStatus`, newer fields) skipped. It allocates
  per page, not per object: the page's strings (keys, ETags, owner, checksum fields, token) are views into one
  arena allocated for that page, repeated values share one copy, and keys and common prefixes are URL-decoded
  in place (form semantics, as the AWS SDKs decode). Rows keep pointing into the arena until encoded; the
  writer clones the only keys that outlive a part (its min and max).

**On the range engine.** S3 lists through `internal/source/keyrange` (below): its page fetch is one
ListObjectsV2 (`start-after` is exclusive, there is no end bound, so the engine trims at `hi`; probes do not
fetch owners), its description adds the client's counters (`attempts`, `retries`, `throttles`, `timeouts`,
`parse_failures`), requeues a transient failure whose retries were exhausted, and wraps contract breaks in
`client.ErrInvalidPage`. `TestRequests` (`internal/source/s3`) pins what the adapter sends: the scope
prefix, a resume's `start-after`, continuations without it, 1,000 keys a page, and the seed's delimiter
listing in its first wave, beside the first data page; a real key's `%` is sent as a start-after. Where ranges split is the engine's policy, tested in `internal/source/keyrange` against the
tiling, resume and sorted-output laws, not frozen in a hash.

## GCS (`internal/source/gcs`)

**Client** (`internal/source/gcs/client`). One `objects.list` GET of the JSON API per page:
`DEFAULT/b/BUCKET/o` with `projection=noAcl`, `prefix`, `delimiter`, `startOffset`, `endOffset`,
`pageToken`, `maxResults` and `userProject`. On Google's endpoint the request asks for the listed fields only
(`fields=nextPageToken,prefixes,items(name,size,updated,…)`); another endpoint (`--gcs-endpoint`, an emulator
or the replay server) gets the full response. A response is read up to 16 MiB; a 400 names its query in the
error. Credentials: `--gcs-anonymous` sends none; `--gcs-credentials FILE` reads a service-account or
authorized-user JSON; otherwise Application Default Credentials, and without any (no
`GOOGLE_APPLICATION_CREDENTIALS`, no gcloud file, not on Google Cloud) the requests go unauthenticated (public
buckets allow it). Credentials that are configured but do not load refuse the run (`client.CredentialError`,
class `auth`), never anonymity; on a Google Cloud machine (DMI product name `Google`/`Google Compute Engine`,
or `GCE_METADATA_HOST`) whose metadata server the client library did not detect in time, the compute metadata
token source is used anyway and its failures are retried per attempt (S3 follows the same rule). The token source lives as long as the client: it is built on a context that is
never cancelled, whose HTTP client bounds each token exchange by `--gcs-request-timeout`, and each attempt
waits for its token under its own deadline, all waiters sharing one outstanding token call (one goroutine at a
time: `tokensource.Shared`, the coalescer S3's bearer tokens use, `TestTokenWaitersCoalesce`); a token endpoint's or the metadata server's (`metadata.Error`) 5xx, 408, 429,
transport failure or timeout is
retried like a listing failure (counted in `timeouts` for the last), a refused grant stops the request
(`TestAuthorizedUserADC`, `TestSlowRefreshBounded`, `TestRefusedGrant`, `TestADCConfiguredButBroken`, against a
local token endpoint; the last injects a failed ADC search, so its unreadable-gcloud, compute-metadata and
anonymous outcomes hold on a Google Cloud host too). The scope is `devstorage.read_only`. The binary links
`golang.org/x/oauth2` and `cloud.google.com/go/compute/metadata`, not Google's storage client;
`google.golang.org/api/storage/v1` is a test dependency only (`TestClientPagesMatchGoogleClient` compares
pages against it). Retries: 429, 408, 5xx and transport errors, up to 9 attempts with exponential backoff
(100 ms to 5 s); exhaustion is requeued by the engine, as for S3. *Classification* (`client.Classify`, S3's
vocabulary): `auth` (401, a `CredentialError` that will not pass; a token endpoint outage or timeout is `transport` or `timeout`, so its exhaustion is requeued), `access_denied` (403), `no_such_bucket` (404), `redirect` (any 3xx: redirects are never followed, also through a supplied HTTP client, because the resume identity freezes the route; `TestGCSRedirectCannotChangeResumeSource`), `throttle` (429,
`rateLimitExceeded`, `userRateLimitExceeded`), `timeout` (an attempt past the request timeout, 408), `server`
(another 5xx), `transport`, `parse` (a 200 that does not parse, or whose body is not an objects.list envelope: `null`, another JSON value or
an error envelope, which would otherwise read as an empty final page; `TestInvalidSuccessEnvelopeIsRetried`,
`TestGCSInvalidEnvelopeNeverCommits`), `invalid_page` (a parsed page that breaks the
contract: `client.ErrInvalidPage`, which the engine wraps as a protocol violation: the outcome's `cause` is
`protocol` and the run is marked unresumable), `canceled`, `other`; the class is the run
outcome's `cause` (`TestClassify` drives each through the client, `TestInvalidPageClass` through the engine).

**On the range engine.** Object names list in unsigned UTF-8 byte order and `startOffset` takes any name,
so `Description.Listing` is `keyrange.Listing`. `startOffset` is inclusive (`StartInclusive`); `endOffset`
is exclusive and set to `ExclusiveEnd(Through, …)` when that bound is at most 1,024 bytes, else omitted
(`end_offset_omitted`) and the engine trims. `AcceptBound` takes any valid UTF-8 name of at most 1,024 bytes
without CR or LF: `%`, control characters and non-ASCII names are sent as start bounds (query-escaped).
Every request of a token chain repeats its `startOffset`, `endOffset`, prefix and delimiter with the
`pageToken`, and empty continuing pages are followed by the engine. The engine's policy flags are the shared
group (`keyrange.Flags`, registered once by the command); the GCS flags are prefixed `--gcs-` because one flag set serves every provider.

**Schema** `swath/gcs-objects/v1`: the key (the object name), `size`, `last_modified` (`updated`, µs),
`etag`, `generation`, `metageneration`, `storage_class`, `md5` and `crc32c` (base64, as listed),
`content_type` and, last, `time_created` (`timeCreated`, µs, the logical type of `last_modified`). `time_created`
is an optional column of the schema id: a part written before the lister listed it has no such column, and a
run resumed across that change holds parts of both kinds ([../upgrade.md](../upgrade.md#the-downstream-reader-dispatch-on-schema-id)). Two columns share
S3's names but not its meaning. `last_modified` is `updated`, which GCS also moves when only the object's metadata
changes; S3's `LastModified` moves only when the object is written. `time_created` is the generation's write
time and never changes. `etag` is the JSON API's opaque entity tag, which also changes with metadata and is not a
content hash, so compare content with `md5` or `crc32c`, never an ETag across stores. GCS omits `md5Hash` for
composite objects, whose `crc32c` remains, and omits both hashes from listings of objects encrypted with
customer-managed (CMEK) or customer-supplied (CSEK) keys. A missing hash is a null column, and when both are null
a comparison needs a hash from elsewhere
([object resource](https://cloud.google.com/storage/docs/json_api/v1/objects),
[ETags](https://cloud.google.com/storage/docs/metadata#etags)). **Identity** for resume: the source, the route (the JSON API base and the bucket, as the
client requests it; scheme and host case are the only equivalence) and the billed project the client sends
(`--gcs-user-project` for the built-in client); credentials may change. A run with `--max-keys`, or through a
supplied client that does not name its route and billed project (`gcs.Routed`), cannot resume.
Counters: the engine's, then the client's (`attempts`, `retries`, `throttles`, `timeouts`,
`parse_failures`), `end_offset_omitted` and `transport.pool_size` (the client's transports; 0 with a supplied
`Options.Client` that does not report one).

**Generations** (`--all-versions`, `NewGenerations`: the engine in group mode, `fetchGenerations`). `ListInput.Versions` sends
`versions=true` and, on Google's endpoint, the field mask with `timeDeleted` (parsed to µs, 0 when absent: a live
generation; it is not `softDeleteTime`); items stay in wire order, name ascending and generation ascending within
a name, and pages are native, continued by the API's `pageToken`. The payload `gcs.Generation` is an `Object` and
`TimeDeleted`; its schema `swath/gcs-generations/v1` is the `gcs-objects/v1` columns in the same order (the same
accessors; `generation` an exact INT64) and then `time_deleted`, null for the live generation (there is no
`is_latest`: a name may have no live generation, and the highest generation is not taken to be live), with
`Info.Groups` set; its delta spill codec codes integers as wrapping differences and repeated strings as flags.
`gcs.ExclusiveStart(name)` is the start that lists strictly after a name, `name + NUL` (checked live: it lists
none of the name's generations); for a 1,024-byte name, whose successor `AcceptBound` refuses, the provider
sends `startOffset = name` and declares it (`Description.InclusiveStart`), and the engine's inclusive drop
removes all of the name's rows, across pages. A page whose generations of one name do not ascend is invalid.

**Connections.** The built client spreads its requests round-robin over `client.PoolSize(--workers)`
transports, one per 16 workers (1 to 8), sharing one TLS session cache: Go's HTTP/2 transport opens a second
connection only at the server's stream limit (about 100), so a single transport would carry all 64 workers on
one connection, which caps throughput once the engine keeps many requests in flight
(`TestTransportPoolSpreadsConnections`). A supplied `Config.HTTPClient` is used alone (a pool of 1). There is
no flag; the pool follows `--workers`.

## Azure Blob (`internal/source/azblob`)

**Client** (`internal/source/azblob/client`): List Blobs and Get Account Information, described with the list
clients [above](#the-list-clients-hand-rolled-on-the-sdks-auth). The root is `az://ACCOUNT/CONTAINER[/PREFIX]`;
`--azblob-endpoint URL` replaces `https://ACCOUNT.blob.core.windows.net` (Azurite, the replay server), the
account then being the endpoint's first path segment (`http://127.0.0.1:10000` serves
`http://127.0.0.1:10000/ACCOUNT/CONTAINER`). Credentials, in precedence: `--azblob-anonymous`; a SAS from
`--azblob-sas`, else the root's query when it names a SAS parameter (`sig`, `sv`, `se`, `sp`, ...; found by
scanning the parameter names, whether or not the query parses; such a query must be a signed query string, or the
root is refused without quoting it; a `?` with no SAS parameter belongs to the prefix), else
`AZURE_STORAGE_SAS_TOKEN`; an account key from `AZURE_STORAGE_KEY` (Shared
Key: signed in the client with `x-ms-date` per attempt, `TestSharedKeyMatchesSDK` comparing it with the SDK's on
both endpoint forms); else Entra ID through `DefaultAzureCredential`, scope `https://storage.azure.com/.default`,
its tokens coalesced by `tokensource.Shared` and its failures classified as GCS's (no answer, 5xx, 408, 429 or a
timeout retried; a refused grant stops the request). Configured by the environment (`AZURE_CLIENT_ID` and the
others the chain reads), a credential that will not work is a `CredentialError`. With nothing configured, on an Azure VM
(its chassis asset tag) the same chain is used and its managed identity always kept. Off Azure the instance
metadata service is never asked (a proxy refuses its address, a laptop's probe hangs): only the developer logins
(the Azure CLI, the Azure Developer CLI) are, one token under the caller's context; when every one is absent the
requests go unauthenticated with the client's note as a warning, as GCS goes without Application Default
Credentials, so a public container lists without `--azblob-anonymous`; a refused grant refuses the run, and a
timeout or a transport failure keeps the login and each request asks again. A SAS never
reaches a recorded file: `--azblob-sas` is a secret flag (recorded as `[redacted]`), the root's query is recorded
as `?sig=[redacted]`, and a resume of either needs a SAS again, from `--azblob-sas` or `AZURE_STORAGE_SAS_TOKEN`
(the command's `secretEnv` lets the variable stand in for the flag). The root is redacted where the command parses
it (`registry.Factory.Redact`, `registry.Redact`), before the `-v` start line, the recorded command and the
report's `invocation.source` see it, so an admission refusal records no SAS either; the identity, the manifest
and the errors carry the root without its query. A scope prefix holding a byte in EE..F4 is accepted and listed
through the adapter like any other.

**Admission.** Before any listing request the namespace must be positively flat. (1) Get Account Information on
the container path: `x-ms-is-hns-enabled` `false` passes, `true` refuses. (2) When that call is refused (401,
which Azure answers any anonymous caller, or 403) or answers without the header, Filesystem Get Properties on the
account's Data Lake host (`client.NamespaceInfo`: `HEAD https://ACCOUNT.dfs.core.windows.net/CONTAINER?resource=filesystem`,
the blob host with its `blob` label replaced, so only for the exact default endpoint, `https://ACCOUNT.blob.core.windows.net`
with the configured account, no port and no path; with any other `--azblob-endpoint` there is no such host and
this step is skipped): `x-ms-namespace-enabled` `false`
passes, `true` refuses. Azure answers it anonymously for a public container (checked live on 2026-11-08). (3)
Anything else (a refusal or a missing header there too, another failure, no Data Lake host) is unknown and refuses,
unless `--azblob-flat` asserts a flat namespace; a reported hierarchical namespace refuses whatever is asserted.
The assertion is the identity's `flat_asserted` field and the report's `namespace` (`account information`,
`filesystem properties` or `asserted`, with a warning), so a wrong one is visible. A refusal is
`source.Unsupported` (exit 2, class `refused`, cause `unsupported`, the message naming the account and the
reason). The order defences cannot stand in: on a hierarchical-namespace account `/` sorts lowest, and after a
byte cursor `a.z` the key `a/child` is still in range, but a seek that sorts `/` lowest skips it while every page
still ascends. `--all-versions` is refused the same way (versions, snapshots and deleted blobs are a later lane).

**Order: the byte-order adapter** (`internal/source/azblob/order`). Azure lists in UTF-16 code-unit order, which
differs from byte order only between names whose first differing scalars are one in U+E000..U+FFFF (lead byte EE
or EF) and one at U+10000 and above (F0..F4) under the same prefix. Real containers hold such pairs: fullwidth
forms (U+FF01..) are common in Japanese names, emoji are at U+1F300 and up. `fetch` delegates to the adapter,
which serves each engine query in byte order: it cuts the query's scope `(After, Through]` at the bounds' scalars
in EE..F4 into spans that are intervals in both orders, each entered by `startFrom` at its minimum in both; it
reads a span by `startFrom` and markers until an item outside it ends the span, dropping the exclusive start and
the roll-up of the subtree a seek lands in; and it splits a span lazily before emitting a name at U+10000 and
above whose U+E000..U+FFFF siblings are pending, so items read once are emitted without another request when
the service has already passed them. Its seeks are its own names (a cut's first scalar, a subtree successor),
sent as they are: Azure lists from any valid-UTF-8 `startFrom` unchanged, a trailing `.` and control characters
included (probed live on 2026-11-08); `AcceptBound` governs only the engine's pivots. A chain's pages are
committed whole, so a failed read repeats the same page; tokens are the adapter's (`chain:seq`). The provider
wraps `Expand`: one adapter per range, released however `Expand` returns, holding at most `order.MaxChains`
(64) idle chains (one with a continuation in flight is never evicted). A token the adapter no longer holds
(`order.ErrToken`, which the engine never sends) is neither requeued nor a protocol violation: it fails the run,
its journal resumable. The comparator (`internal/utf16order`) is the one the client's page check and the replay's
Azure route use. The specification, with its worked examples and the facts it rests on, is
`notes/plans/2026-11-08-azblob-order-adapter-spec.md` in the notes repository.

*Cost.* A query with no name at U+E000 and above in its bounds, its prefix or its results costs nothing extra:
one span, the same requests as a plain marker chain, no split. Bounds count as well as names: the engine's
frontier splits synthesise pivots from its whole alphabet, so over names just below U+E000 (lead byte ED) a
bound at U+E000 and above can arrive and is cut like a name (`TestSynthesizedPivotListed`), which costs a seek
and possibly an empty probe. An item equal to a bounded query's `Through` ends its span, so a
range's last page does not read past its end (sound because `Through`'s path is cut at every group boundary:
nothing in its span follows it in Azure's order; an addition to the specification's §4.1). Where such names are, each cut is a seek (`order_spans`,
`order_seeks`; one per span, even an empty one: `order_probes`), each split may add one (an `E` group the read
has not reached), and a span that ends before its response does discards the rest of that response
(`order_overlap_rows`: a read asks for a full page). An adapter invariant broken (only an out-of-order page can
do it) is the client's invalid page, a protocol violation.

**On the range engine.** `Description.Listing` is `keyrange.Listing`: the adapter's pages are byte-ordered,
start after `After` (exclusive; `StartInclusive` is false) and end at `Through`, which is never sent. `AcceptBound` takes valid UTF-8 of at most 1,024 UTF-16 code units, without NUL or another
control character, not ending in `.` (Azure's name rules). `Requeue` is the client's `ExhaustedError` on a
transient class, as for S3 and GCS; `InvalidPage` is the client's. Pages are `client.MaxPageSize` (5,000, the
service's cap) unless `--page-size` lowers them. An adapter page maps `Blob` items to rows and `BlobPrefix` items
to `Prefixes`, with the adapter's `Truncated` and token.

**Schema** `swath/azblob-objects/v1`, its columns and their caveats as [upgrade.md](../upgrade.md#the-downstream-reader-dispatch-on-schema-id)
states them; `MaxKeyBytes` 3,072, `blob_type` dictionary-encoded, and a delta spill codec.

**Identity** for resume: provider `azblob`; `source` (the root without its SAS), `endpoint` (the account's blob
service URL as the client requests it, the account path segment kept on a custom endpoint; scheme and host case
are the only equivalence), `account`, `container`, `flat_asserted` (`--azblob-flat`). Credentials may change, a SAS among them. A run with
`--max-keys`, or through a supplied client that does not name its route (`azblob.Routed`), cannot resume.
**Counters**: the engine's, then the client's (`attempts`, `retries`, `throttles` (503 `ServerBusy` and 429),
`timeouts`, `parse_failures`, the admission's), `transport.pool_size`, `retry_after_capped` (a `Retry-After`
past the request timeout, cut to it) and `body_cap_shrinks` (a page past the 64 MiB body cap, asked again with
`maxresults` halved), then the adapter's: `order_spans` (spans beyond each chain's first), `order_splits`,
`order_seeks`, `order_probes` (span reads that found no member), `order_overlap_rows`, `order_empty_pages` (empty
continuing pages followed inside a span's read), `order_stack_max`, `order_buffer_rows_max`, `order_responses`,
`order_chains_evicted` and `order_chains_max`. **Connections**: the GCS pool, one
transport per 16 workers (1 to 8). The client's anonymous note goes to the run's warnings.

## Range engine (`internal/source/keyrange`)

The range model, seed, owner split, safe pivots, guards, demand, the probes, retry-and-requeue and their
counters, shared by the providers whose listing returns keys in unsigned byte order and can start after any
key (`Capabilities.RangeEngine()`). `Engine[P]` implements `source.Provider[Range, P]`; a provider supplies:
- a page fetch, `Fetch[P]`: one request (`Query`: prefix, delimiter, `After`, `Through`/`Bounded`, token,
  page size, `Probe` for requests whose objects are not emitted) to one `Page[P]` (rows in listing order,
  common prefixes, truncated, token). A continuation request repeats every field of its chain's first
  request and adds the token; the provider maps them onto its wire (S3 sends start-after on the first
  request only);
- a `Description[P]`:
  - *bound inclusivity:* the engine's ranges are `(lo, hi]`. `After` means keys strictly above it; a
    provider whose start bound is inclusive sends it as is and sets `StartInclusive`, and the engine drops
    a key equal to `After` from the first page of a request chain, and with a delimiter every common prefix
    at or below `After`: GCS returns `a/` for `startOffset` `a/` or `a/b`, S3's `start-after` returns
    neither (both measured on the live services; `TestInclusiveBoundDropsCoveredPrefixes`). `Through` is the inclusive end; a
    provider may ignore it (the engine trims) or bound the request with `ExclusiveEnd(Through, accept)`,
    the immediate successor (`Through` + NUL), when its own predicate accepts that string (a service may
    refuse NUL, or 1,025 bytes); otherwise it sends no end bound and the engine trims;
  - *bound admissibility:* the engine needs only valid UTF-8 bounds (it compares bytes); which strings a
    provider accepts as a start bound is its `AcceptBound` predicate, by default the pivot alphabet
    (`Safe`: valid UTF-8 without C0, DEL, C1, `%`, surrogates or non-characters). S3 declares `SafeKey`
    (`s3.AcceptBound`): the pivot alphabet plus `%`. `%` is out of the alphabet because an SDK that decodes a
    verbatim-echo endpoint's `StartAfter` crashes on it (I12); this client percent-encodes the query and parses
    no echo, so a real key holding `%` (a cursor, a folder) is a usable `start-after`. Under `Safe` a bucket
    whose every key holds `%` (URL-encoded names) had no usable cursor and listed on one worker: 107.9 s
    against 8.6 s on 2M keys at 50 ms ([measurements](../measurements.md#red-team-fixes-2026-11-07)).
    Synthesized scalars always come from the pivot alphabet: a pivot is a prefix copied from one of its input
    keys (cursor, bound or anchor), up to the scalar it changes or appends, plus safe scalars, so it never
    introduces a `%` its inputs do not hold at that position (`FuzzPropose` checks it). The
    predicate guards every bound the engine sends: pivots (guard clause `unsafe_pivot`), cursors
    (`unsafe_cursor`), seed and structure boundaries and a resume's start (K's longest accepted prefix);
  - `Requeue` (which failures are asked again after a backoff instead of failing the run), `InvalidPage`
    (the error contract breaks wrap), `Size` (a row's bytes) and the provider's own `Counters`, reported
    after the engine's.

The engine's options are the scope prefix, `Sequential`, `Seed`, `Prior` (set by `SeedFrom`, below), `Structure` (`StructureOff`, `StructureAuto`, `StructureOn`),
`Floors`, `Workers`, `MaxKeys`, `Resume` and `PageSize`, the nominal page size (default 1,000): data and
seed requests ask for it, structure probes for no more, and the floors count pages of it. The
engine sorts and deduplicates each page's common prefixes (`prefixes_normalized`), so a provider need not.
A continuing structure probe (a provider may cut pages short) is a partial
sample whatever its length (its furthest folder is the pivot). Its tests (`internal/source/keyrange`) run on a fake listing
API over a sorted key slice (`keyrange/keyrangetest`, with the fixture and hostile keyspaces).

**Rules** (the generic contracts are in [contracts.md](contracts.md)): a range `(a, b]`
lists `a < k ≤ b`, and its right neighbour starts after `b` (the boundary belongs to the left); a synthesized
pivot is valid UTF-8, strictly between its bounds and adds no code point from the unsafe set (safe pivots);
keys are at most 1,024 bytes (`schema.ErrKeyTooLong`).

**Paging.** A range is `(lo, hi]`: `lo = ""` is ⊥, the open frontier has `hi = ⊤`. The first page starts
after `lo`, later pages follow the continuation token. Keys past `hi` are trimmed (`overfetch_keys`); a page
whose last key equals `hi` ends the range without another request; a page that is not truncated ends it. A truncated
page without a token, a truncated page with no key in range, or a key not after its predecessor is an error.

**What is trusted.** swath takes a page that is not truncated as the true end of its range: every key the
service holds after the page's start, up to the range's end, was on that page. Nothing in one reply can show
otherwise. An empty page marked not truncated is exactly what an empty range returns, and swath never sees a
key that the service leaves out. Only a witness outside that reply can expose the omission:

- an independent sequential audit of the same scope (one continuation-token chain from the start);
- a key known to exist past the page's start, inside the range, that the listing lacks.

The durable spill campaign found such a service: MinIO answering a `start-after` inside one prefix with an
empty page ([measurements.md](../measurements.md#durable-spill), `bench/minio_start_after.sh`). Its key-file
oracle is the first kind of witness.

**Root and seed.** The root is `(⊥, ⊤]`. With the seed (on by default in the command, `swath diag list --seed=false` turns it
off; never with `--sequential`) the root is a seed that emits no rows and tiles the scope at cut points
`(⊥,c1], …, (ck, ⊤]`, all real folders found by delimiter listings (`/`, 1,000 entries a page,
`Probe` set):
- *The first data page.* The first wave also carries the scope's first data page (no delimiter, from the
  start a first tile would use; `Options.SeedFirstPage`, which the command's policy sets with the seed). When it
  is not truncated the scope fits in one page: the seed emits it, the delimiter requests still in flight are
  cancelled, and the seed stops there with no tile (`seed.stop.one_page`, `seed.first_page_keys`), one round
  trip in all, as `--seed=false` (red team: 100 and 1,000 keys took 4x the `--seed=false` wall, 70 requests per
  1,000 keys on 100). A truncated page is discarded (`seed.first_page_discarded`, one request more) and the
  tiles are the seed's alone, from the scope's start. Emitting it and starting the first tile after it left
  the 12.6M-key megascenes list with a serial tail (12 s to 20 s, mean active 57 to 35, for the same requests
  and splits; `TestSeedFirstPageLargeFirstTile` is its shape).
- *Pagination.* The top level follows its continuation token up to 4 pages (`seed.pages`), so a top level of
  up to about 4,000 folders is tiled whole; past that it is `seed.truncated`. Four pages are already four
  times the most cuts a seed keeps. A chain whose last page held no folder (objects only) is not followed
  (`seed.flat_chains`; the level then stops as `seed.stop.flat` if it found no folder): each further page of
  a flat level is a round trip that finds no cut (the red team's flat leaves spent 4-6 waves for none). The
  cost: a level with more than a page of objects before its first folder is not tiled past them, and the
  tiles split there as any range does.
- *Descent.* While a level holds fewer folders than `--workers`, the next level is listed under every one of
  them, all at once: one *wave* of independent requests, through the provider's client and admission like
  any request. Each folder's chain is paginated the same way, one wave per page. The descent stops
  (`seed.stop.<reason>`) when a level holds at least `--workers` folders (`wide`) or none (`leaf`), or at a
  budget: 4 levels below the top (`levels`; the single-root date layout
  `warehouse/events/year=Y/month=M/day=D/` needs four to reach its day folders), 64 requests (`requests`; a
  level is listed whole or not at all) or 6 round trips (`round_trips`; each wave is one request latency on the
  run's critical path before any range lists). An empty continuing page that `list` follows by its token is a
  request and, on its chain, a round trip: the seed charges each one before it is sent (a wave costs its
  longest chain), and when one would pass a budget the chain stops there, truncated, with that budget's stop
  reason; without that charge, a delimiter chain behind empty pages would run unbudgeted inside one seed page.
  Java's serial 256-probe descent spent 13.5 s of a 39 s run on its chain; here a wave sends its requests at once, so the cost is the number of waves: 1 for a wide top level, 2
  for a narrow one, 5 for the date layout.
- *Cuts.* Every folder found at every level is a cut (a descended folder stays one, and its children lie
  inside its tile), sorted, filtered by the provider's bound predicate (`seed.unsafe`) and evenly subsampled to
  at most `min(1000, 4 × workers)` (`seed.subsampled`). `seed.level.<n>.cuts` counts the folders found at each
  level, `seed.requests` and `seed.round_trips` what the seed spent, `seed.levels` how deep it went. They are
  published on every exit: a seed whose listing fails or is canceled counts `seed.stop.failed` and reports
  what it spent until then, which on a resume can be most of the run.
- *Sentinel.* When the top level was listed whole and its last page ends with a folder `pk`, the last tiles are
  `(…, prefixCeil(pk)], (prefixCeil(pk), ⊤]`, so the folder holding the end of the top level is bounded and
  not the open frontier (a page ending with an object declines it, `seed.sentinel_declined.*`). With the
  descent, a bucket whose mass sits under its last top-level folder is tiled inside that folder too.
- *A resumed scope.* After `Restart(K)` the seed tiles `(K, ⊤]` instead: every folder above `K` inside the scope
  is listed at the top, after `K`'s longest accepted prefix (each lists the siblings after `K`'s branch at its
  level, so a resume inside one day folder still tiles the rest of the month and year), and only cuts strictly
  above `K` are kept: a cut at or below `K` would list published keys again, and the drop of keys up to `K`
  covers only the first tile, `(K, c1]`. An inclusive-start provider returns the folder at or below the bound,
  which the engine drops (`bound_drops`). `seed.resumed` marks it and `resumed_ranges` counts the tiles.

**Warm start** (`--seed-from PATH`, `prior.go`; off by default). A previous listing is a cut-point set with a density
prior, so the engine can start from it instead of the seed: the root tiles the scope at the prior's cuts and lists
nothing itself, no delimiter request is sent (`seed.requests` 0, the `seed` feature off, `seed_from` on). The prior
(`source.Prior`, in `internal/source` so the pipeline fills it without importing a provider package; `source.Seeded` takes it) is source-neutral: ordered byte-exact cut keys, each with an optional count of the keys expected
in the interval it closes, and the scope they were taken under. Two sources fill it (`internal/listing/prior.go`):
- *a committed dataset directory* (SeekParquet, or an earlier build's format 2): the manifest's parts give exact bounds and row counts; the key
  column's page index (column index maxima and offset index row counts, 1,024-row pages; `parquetout.KeyPages`
  reads the footer and those two index sections, never a data page: a SeekParquet part through
  `seekparquet.InspectKeyPages`, which checks its footer and stamps first; a format 2 part by its footer) gives cuts a thousand times finer. The
  reader offers 4,096 cuts of about equal rows (four candidates per tile at the most tiles a run starts with),
  reading the index only of parts a cut falls in; a part whose index cannot be read (another writer, a key over
  the 1,024-byte index bound) is cut at its own last key (`seed.from.index_failed`). The dataset's last key closes
  the last cut;
- *a cut file*: one key per line, ascending bytewise without repeats, optionally a tab and the expected count;
  every line has a count or none does (uncounted cuts weigh one each). Its cuts are held for the run and
  merged into at most 1,000 tiles, so the file is bounded: more than 262,144 cuts or 16 MiB (counted while reading, so a
  pipe or a growing file is bounded too) is refused before any request (`ErrPrior`; `maxCutFileCuts`, `maxCutFileBytes`, about 30 MiB held at most). Thin a larger one,
  every nth line with its count the sum of the lines it replaces (`TestSeedFromCutFileBounded`).

Identity, checked before any request (exit 2, class `refused`, `ErrPrior`): a dataset's manifest must name the
run's source kind and container (the bucket), and its scope must be the run's prefix exactly: a dataset of
`s3://b/logs/` does not seed `s3://b` nor `s3://b/logs/2026/` (a prefix mismatch is refused, not clipped). The
manifest records no endpoint or region, so those are not checked: a dataset of another store's bucket of the same
name is accepted, which can make the run slower, never wrong. A cut file records no scope: every cut must carry the
run's prefix. Either way the cuts must ascend, be valid UTF-8 and at most 1,024 bytes. The prior is not the output
directory; the sequential listing and sources off the range engine (the filesystem) refuse it.

Tiling (`tilePrior`): cuts at or below a resume key are dropped (a resume tiles `(K, ⊤]`, its first tile from
K's longest accepted prefix, as the seed's); a cut the provider does not accept as a bound is replaced by its
longest accepted prefix when that still sorts after the previous cut and inside the scope, else merged into the
next (`seed.from.unsafe`); with more cuts than `min(1000, 4 × workers) − 1`, a cut is kept where the running mass
first reaches the next multiple of total/(tiles − 1), the last always. The tiles are `(⊥, c1], …, (ck−1, ck]` and
the open frontier `(ck, ⊤]` for keys the prior did not know (`seed.from.tiles` counts all). Each bounded tile
carries its expected keys as its estimate (`Range.est`), the rank's remainder until its first page.

The prior sets the initial tiles and that first estimate only; the adaptive engine is the same for every source
(owner splits on demand, the gauge, the admission gates, the override, structure probes). How a stale prior is
corrected: a tile's first page replaces the prior's estimate with its gauge's remainder, so a grown segment is
under-ranked only until its first page; from then the rank, by estimated remainder, sees it as large, and once
workers go idle (hunger) it splits inside like any range, the gates reading the page, not the prior. A gauge that
cannot read the bound (+∞) ranks it above every finite estimate; where the gauge errs, the admission override (4
refusals in a row) and hunger cover the rest. An emptied segment costs one request for its tile; a new folder
between cuts lies inside one tile and is split there. The report shows it: `seed.from.error.within_2x`, `.grown`,
`.shrunk` and `.max_grown` compare every prior tile's listed keys (its descendants included, exact once the run
ends) with the prior's count; `split.pivot.*`, `split.max_depth` and `child_empty.*` show the splits it took, as on
any run; `seed.from.expected_keys` against `objects` is the scope's growth. The run report's `invocation.prior`
names the path, the source (`dataset` or `cuts`), the cuts offered and whether they were counted. On a resume the
prior is read again; one that is gone or no longer matches is a warning, and the resume lists without it.

There are no finer cuts than the page index: the index gives every 1,024th key, more than the 1,000 tiles a run
starts with need, and a tile's own pages measure the rest.

The seed is a serial pre-phase of 1 to 6 waves; on a bucket of one page the first wave is the whole listing.
The default is measured against `swath diag list --seed=false` on production key lists of 1M objects and more
([measurements](../measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)), and on
100 to 10,000 keys ([measurements](../measurements.md#red-team-fixes-2026-11-07)).

The seed only paginates and descends narrow levels, in parallel waves, under fixed budgets. Java swath's hybrid
seed descent (`HybridSeedPlanner`: a walk of the folder hierarchy that classifies truncated levels, samples
ambiguous folders and cuts dense flat folders into radix bands) has no counterpart: measured on the replayed
recordings it gave no gain on any of them and 30-60 % more requests on the shallow ones
([measurements](../measurements.md#policy-switches)).

**Splits are owner-side.** A range's children become claimable only when its `Expand` returns, so there are
no thieves. After each full page the owner takes the provider mutex and computes demand,
`hunger = workers − active − queued`. If `hunger > 0`, the range ranks among the `hunger` largest eligible
ranges, the guards allow the pivot and admission does, it emits `Child((c, m])` and `Child((m, hi])` and
returns, where `c` is the last key emitted. `(lo, hi] = (lo, c] ∪ (c, m] ∪ (m, hi]`, disjoint and in order.
Every split feeds an idle worker and no LIST is repeated.

**Admission** (`admission.go`): demand is the only trigger (no rate limit, no feedback loop); what admission
adds is whether a split is worth its requests. Each child pays a first request, and on S3, which has no end
bound, a child's last page overfetches up to a page past its `hi`; the split saves at most the page latencies
the owner would spend on the rest alone. Without the gates a small list splits about once per page and half
the children come back empty (measured without them on os-climate-physical-risk: 674 splits, 426 empty
children, 1.82 requests per 1,000 keys). One estimate serves both the rank and the gates:
- *The gauge.* The keys in an interval `(x, y]` are estimated from the range's last page, from its run of keys
  under the prefix `x` and `y` share (a page that crosses folders would otherwise measure the density of
  folder names). Keys are read as numbers in the run's own alphabet: the distinct bytes past the run's shared
  prefix, a byte outside it rounding up to the next one in it, the end of a key below them all. The run's
  keys per unit scale the interval. The page's alphabet, not base 256, keeps decimal counters in proportion
  (base 256 reads a remainder one decimal digit above the page 25 times too long), and a synthesized pivot
  past the run's digits (`logs/p/00001l` after `logs/p/000019999`) reads as the end of them. Where the run
  cannot read the interval the estimate is +∞ and no gate engages: no run of two keys under the prefix, or a
  difference above the run's varying digits at a position whose byte in the run is not one of its digits (a
  bound in another folder). A folder gap still reads as keys (a page cannot know that a folder ended), so on
  hierarchical keys the estimate errs long, where the gates stay out of the way.
- *The gates,* on bounded ranges only, after the guards, in pages of the range's own page length (a
  provider may return fewer keys than asked): no split when the remainder `(c, hi]` is under 2 pages (the
  owner lists it in at most two more requests; `split.skipped.remainder`), or when either child, `(c, m]` or
  `(m, hi]`, is under 1 page (the owner's next request covers a small lower child anyway; a small upper child
  leaves the lower one nearly the whole remainder, and an empty one overfetches a full page;
  `split.skipped.lower_tiny`, `split.skipped.upper_tiny`). The frontier's remainder is unbounded, and a
  frontier refused for a near pivot would keep its anchor at ⊥. A refusal declines that page's split only:
  the range decides again after its next page (`TestAdmissionRedecides`), and after 4 refusals in a row, each
  with demand, the next split is taken (`split.admission.overridden`, `TestAdmissionOverride`). The policy can
  propose one refused pivot page after page: on e11bio-prism the midpoint `…/0/1>` lies in the gap past
  `…/0/19…` below `hi` `…/0/2`, its upper child is truly empty, and its lower child holds a million keys;
  refused for the range's life, those went to one worker and the run fell to 4–13 wide (66 s against 7 s on a
  6M-key slice). A page of fewer than two keys gives no estimate (`split.estimate_none`).
- *The rank* is by estimated remainder: the open frontier first, then the larger remainder `(c, hi]` by the
  range's last page, ranges whose page could not read their bound (+∞, a bound in another folder) above every
  finite estimate, and age within a class and on ties: a strict total order (`TestRank`), as a cyclic one gave
  every range a rank of one and with one idle worker none split. A child starts with its share of the parent's estimate, `(c, m]` or
  `(m, hi]`, until its own first page; without either, the keys it has listed. The keys listed alone reset in
  every child, so a rank by them is a rank by age, blind to an upper child holding most of a remainder.

The pivot is not touched by admission; the gates refuse, they do not move a pivot, and they apply to a
structure boundary as to the policy's pivot. On the 4.1M-key noaa-nws-fourcastnetgfs-pds list and synthetic
1-2M-key lists the gates cut requests per 1,000 keys by 2-15 % with no wall increase
([measurements](../measurements.md#split-admission-on-small-lists)); a flat million-key list ends in about 250
ranges where it ends in 630-810 without them (`TestAdmissionFlat`: 1,000 pages at 64 workers, 100,000 keys in
pages of 100 by default, the million-key list in pages of 1,000 with `SWATH_PERF=1`). What remains are empty upper children of pivots placed in a folder
gap (a date's `day=32`-`day=99`, a letter after a counter's digits), which only a structure probe can see.

**Slow-range splitting** (off by default, `swath diag list --slow-split`; `Options.SlowSplit`). Demand is idle
workers and the rank counts keys, so a range whose pages are slow (one folder at 2 s a page among 200 ms pages,
the red team's `hotcold.narrow.d200`) stays on one worker until the rest drains (`split.skipped.no_demand`).
With the switch each range keeps its page latency (an EWMA, weight 0.5 on the newest page; children start from
their parent's), the rank reads time left (keys left × page latency) in place of keys left, and a bounded range
splits without an idle worker (`split.reason.slow_range`) when its page latency is at least 4 times the run's
mean (a latency outlier: neither jitter of 20-500 ms nor a slow half of the keyspace reaches it) and its time
left is at least twice the running ranges' mean. Keys left are the gauge's remainder or, where that is +∞ (a
seed tile's bound is the next folder, and a page cannot see where its own folder ends), the keys the range has
listed; a slow split's child that is still an outlier halves again without that test. Bounded: one slow split's
children wait at a time (they are queued, not run, until a worker frees up), and the admission gates apply.
Counters `split.slow.max_range_latency_ns`, `split.slow.run_latency_ns`. Measured: on a scaled slow folder
(`TestSlowRangeSplits`) 9 % off the wall; on the suite ([measurements](../measurements.md#red-team-fixes-2026-11-07))
too little to adopt: the slow folder still peels a few pages per split, as the pivot policy hands out what an
idle worker can take, not half a folder whose end no page shows.

**Floors** (Java swath's owner-split gate chain; off by default, `swath diag list --floors` turns them on;
[measurements](../measurements.md#policy-switches)).
Two gates on bounded ranges, read from a position estimate rather than a request (Java swath's default
rate-anchored sensor, `RateAnchoredEstimator`): the keys left are `max(keys listed, 1 page) × g`, where `g`,
clamped to [1/4, 16], is the remaining span over the consumed span read as base-256 fractions (12 bytes) past
the byte where the cursor diverges from `lo`; where the cursor shares more with `lo` than `hi` does, the window
is that shared subtree, with ceiling 1. A cursor at `hi` leaves 0. So a range clears the remaining floor after
at most 16 pages listed, however deep its keys' shared prefix.
- *Remaining floor:* a range with at most 4 pages (4,000 keys) left is not split. Counted per full page,
  before demand (`split.skipped.remaining_floor`).
- *Child-tail floor:* the child past the pivot must realize more than 2 pages. It is credited with
  `remaining × max(1/16, min(1, ρ) − f)`, where `f` is the pivot's fraction of the remaining span (same frame) and `ρ`
  the ratio of the trailing page density (EWMA, weight 0.3 on the newest page) to the range's average; a
  thinning keyspace (`ρ < 1`) shrinks the credited tail (`split.skipped.tail_floor`).
The open frontier's remainder is unbounded, so neither gate applies to it.

**Structure probes** (on their evidence by default; `swath diag list --structure=on` probes every range in a
folder, `--structure=off`, or `=false`, none; a library caller's zero `Options` has none). A page's density is
leaf-local and the anchor reflection overshoots into gaps between sparse folder names, so neither can place a
pivot on a distant folder boundary; a delimiter listing can. A probe is one delimiter request per folder level
(`max-keys` 32, start-after the cursor), all levels at once, alongside the range's next page, only while a
worker is idle and with at most one probe in flight per range; it takes its admission permit from the same pool
as pages. Who probes, and why (`structure.sent.*`):
- *the open frontier* (`frontier`), while its cursor has a folder below the scope: the scope root and the two
  folders above the cursor's own (its own folder lists the cursor's siblings, rarely a folder). The furthest
  boundary of the coarsest level that has any is the frontier's pivot when it lies further than the policy's
  pivot (`structure.frontier_pivots`); the lower child inherits the other boundaries and halves on them. One
  probe moves a date-partitioned frontier from `month=01/day=02` to `month=12/`;
- *a bounded range on evidence of a stall* (`stall`): the split that made it kept its pivot inside the cursor's
  own folder while `hi` lies beyond that folder, the shape where a chain peels one folder per round trip. Its
  levels are the folder where the cursor and `hi` diverge, then the next folder below it on the cursor's path.
  A flat keyspace inside one folder never shows that evidence, since its `hi` shares the cursor's folder;
- *every bounded range in a folder* with `--structure=on` (`forced`), a diagnostic.
The coarsest level with a folder inside `(c, hi)` gives the range its boundaries (`structure.boundaries`). At a
later split decision, if the result has arrived, a bounded range pivots on the median boundary inside
`(c, hi)`, or the furthest when that level's page was full; the policy's pivot is used otherwise. A split never
waits for a probe (`structure.late` counts decisions taken while one was in flight). A probe still in flight
when its range's `Expand` returns is cancelled. Children inherit the boundaries inside their bounds, so one
probe serves several generations of splits, and they inherit the count of consecutive probes that found no
folder at any level: eight in a row along a range's ancestry stop probing there and below
(`structure.suppressed`; `structure.suppressed_inherited` when the eight were all an ancestor's), so a flat
keyspace pays at most eight probes along any chain.

**Guards**, checked before any child and counted per clause (a page that ended the range never reaches
them: the loop ends before the decision): `no_pivot` (the owner keeps listing), `unsafe_cursor` (`c` must be a usable start-after), `cursor`
(`m > c`), `unsafe_pivot` (the provider's `AcceptBound` vetoes `m`), `bound` (`m < hi`, or below
`prefixCeil(scope)` on the frontier). Each declines only that page's
split, and the next continuing page is evaluated afresh, except `no_pivot` after a safe cursor: a later cursor only
narrows `(c, hi]`, so the range stops considering splits (`TestSingleUnsafeCursorDoesNotDisableLaterSplits`: one
unsafe or vetoed page-ending key among 100,000 still leaves 98 splits). That `no_pivot` is reached only when the
midpoint `between(c, ceiling)` finds neither a safe scalar nor a sliver, so it is a fact about the interval, never
a synthesis step that could not run on its inputs (`TestInvalidScopeCeiling`).

**Pivots.** A split waits for a page, so a chain of `d` dependent splits costs at least `d` page latencies:
the pivot policy is judged by the depth of the split tree it builds (`split.max_depth`), which a balanced tree
keeps near `log2(pages)`. A pivot that peels a few pages off the cursor and leaves the remainder in the upper
child makes the depth linear in the range's size. So the pivot is the further of two steps (`propose`):
1. **The anchor reflection.** Each chain of upper children carries an anchor: on the open frontier a seed or
   resume tile's `lo`, or, for a chain that started at ⊥, its first pivot (⊥ itself measures nothing); on a
   bounded chain the first key its first range listed. The cursor is reflected from the anchor at the scalar
   where they diverge, by `idx(c[i]) − idx(anchor[i])` (`forwardReflect`). The covered distance then doubles
   per split (δ, 3δ, 7δ, …) and climbs folder levels by itself: from `day=01` the frontier reaches `day=03`,
   `05`, `09`, then `day=A`, which sorts after `day=31`, and the next month; reflecting from the previous
   pivot instead would cover one page per split, since the upper child's cursor is one page past it. A
   reflection at or past the range's ceiling (`hi`, or `prefixCeil(scope)` on the frontier) falls back to the
   safe midpoint `between(c, ceiling)`, and the upper child starts a new chain measured from its own keys
   (`split.reflect.ceiling`): the remainder is then shorter than the distance already covered. A ceiling
   that is not UTF-8 (`prefixCeil("ÿ")` is `c3 c0`) is replaced by the least valid string at or above it
   (`Ā`), which bounds exactly the same valid keys, so a scope ending in a multi-byte scalar still has a
   midpoint. An anchor that is not UTF-8 (a first key) measures nothing, and the reflection is taken from ⊥.
2. **The galloped density step.** Keys are read as numbers in the alphabet seen on the last page (safe ASCII
   and `%`; the target's first new character is synthesized, so a `%` there is snapped up to `&`); the target is `c + page span × (max(4, hunger) << g)`, at most halfway to `hi`, cut after its first
   character that differs from `c`. `g`, the gallop generation, counts the consecutive upper children along the
   chain (a lower child is 0, at most 16), so a remainder that keeps splitting hands out twice as much per
   round trip. A target past the representable keys, with a hole, or outside `(c, hi)` is halved within the
   decision, down to 4 page spans (`split.gallop.halvings`). The step is leaf-local: one page's span lives in
   the last folder's digits, so it cannot reach a parent folder; that is the reflection's job. It may move the
   pivot further than the reflection, never nearer (`split.density.shrink_prevented`), on the frontier and in
   bounded ranges alike: a nearer pivot would make the frontier crawl one folder per round trip.
A folder boundary known from a structure probe inside `(c, hi)` takes precedence over both in a bounded range; on
the frontier it is taken when it lies further. A sliver, the
only pivot when nothing safe lies between the cursor and the ceiling and the density step is refused
(`cursor + " "`, or the 1,024-byte cap's bump), leaves the lower child all but certain to be empty: it is
refused (`split.sliver_refused`). The range is not made unsplittable; it keeps listing and decides again after
its next page. The split's demand slot is reserved in the same decision, so concurrent owners do not split for
the same idle worker. Every pivot is safe (I12); `FuzzPropose` checks it over the policy's inputs.

The pivot is placed outside the decision mutex, from the page and a snapshot of the demand taken with the
page's bookkeeping: the density step's big-number arithmetic, up to one evaluation per halving, is the costly
part of a decision. The decision itself (demand, rank, a structure boundary, the guards, the reservation) stays
under the mutex; a page whose snapshot saw no demand is not split even if demand arose since
(`split.skipped.no_demand`). Counters are atomic and are not a reason to take the mutex: a path that only
counts (requests, pages, overfetch, structure probes) does so without it, and `SWATH_SPLIT_TRACE` prints after
the split is committed and the mutex released, so `mutex.wait_ns` measures decisions. Generation and anchor live on the immutable `Range`; nothing persists them (a
resume starts new chains).

There is no 1-key pivot probe (Java's `max-keys=1` request after the pivot, moving an empty upper child's
pivot toward the cursor): on the deep replayed recordings ([measurements](../measurements.md#policy-switches))
it cost more than the empty children it saved, a serial round trip before each split that lowered
concurrency: 27–31 % more requests and 2.2–2.3× the wall.

**Counters** (in the run summary): objects, bytes, pages, requests, retries, `overfetch_keys`; seed
(prefixes, pages, requests, round trips, levels, cuts per level, why it stopped, cuts, unsafe, subsampled,
sentinel and why declined, resumed); warm start (`seed.from.*`: parts, indexed and failed parts, cuts, unsafe
cuts, tiles, expected keys, the prior's error per tile); splits (considered, accepted bounded and
frontier, rejected per guard clause, skipped for no demand, outranked, or by an admission
gate), each accepted
split's pivot source, the density refinement's refusals by reason; `child_empty.{lower,upper}` and by pivot
source and kind of `hi`; small children; depth; unsplittable ranges; the floors' skips; structure probes and
their pivots; `max_active`, `max_queued`, `mean_active`; busy, idle-with-demand and sink-wait time.

**Reading a split chain from the report.** Every accepted split counts one pivot source:
`split.pivot.reflect` (the anchor reflection), `split.pivot.midpoint` (a safe scalar between the cursor and the
ceiling, after a reflection reached it), `split.pivot.density.bounded` and `.frontier` (the galloped density
step lay further), and `split.pivot.structure`. `split.frontier.anchored` counts frontier splits whose chain had
an anchor, `split.gallop.max_generation` the longest run of upper children that split, and
`split.reflect.ceiling`, `split.density.shrink_prevented`, `split.gallop.halvings` and `split.sliver_refused` the
decisions where those rules engaged. When the density step is refused, `split.density.refused.*` says why
(`alphabet` is a page with a non-ASCII or control byte: such keys turn density off, and only the reflection and the
midpoint remain). A child carries its parent's pivot source, so an empty child is attributed
(`child_empty.by.<source>.<lower|upper>`) and classed by its `hi` (`child_empty.hi.pivot`: a pivot; `.bound`: a
seed cut, the scope's end or ⊤). The chain's length is `split.max_depth`, its mean over the leaves
`split.leaf_depth_sum` / `split.leaves`; at one page latency per step, depth times `inflight.p50_ns` is a floor
under the wall time. Children ending under one page (`child_small.*`) and the keys a frontier split handed to
its lower side (`split.frontier_handout_keys` / `split.accepted_frontier`) show the balance of the splits;
`idle_demand_ns` is worker time idle while work was wanted, the cost of a policy that cannot hand work out fast
enough; `split.unsplittable` and `split.unsplittable_keys` are ranges a guard made unsplittable and the keys one
worker then drained alone. `SWATH_SPLIT_TRACE=1` prints each split with its pivot source, depth, gallop
generation and the anchor its upper child carries.

**Sequential oracle.** `swath diag list --sequential` lists the scope as one continuation-token chain; its dataset is
byte-identical to the parallel core's. It is the only mode where `-max-keys` stops early (the dataset is
then marked incomplete).

**Resume.** `Restart(K)` makes the scope `(K, ⊤]`: with a prior or the seed it is tiled as above, without them the root is
one range `(K, ⊤]`, split as on a fresh run; S3 is `source.Resumable` through it ([durability.md](durability.md)).

### Group mode (`groups.go`)

A versions listing (`Description.Groups`, which must agree with the bound schema's `Info.Groups`, and
`Description.Detach`, the schema's) returns a key once per version, consecutive, and a page may end inside a
key's rows. Every consumer treats the last key it saw as finished (a split's lower child starts after it, the
journal records it as published, a resume lists after it), so within one chain (an `expandRange` call or the
sequential chain) the engine emits a row only once its key's rows are proven complete:

- *Hold-back.* A continuing page's trailing key group is held and prepended to the next page; a later key or
  the chain's end (EOF, or a row above `hi`) completes it. A page that is all one new key is held whole; a page
  that repeats the held or emitted key cannot be held (a key can span any number of pages), so it is emitted as
  it arrives and the chain's group is *open*. Held rows come from at most one page; the emitted and the held
  sides of a partial page are both copied out of the page (`Detach`), so a few rows never pin
  a page the order stage did not charge.
- *Splits.* A range splits only at its last emitted key, which is complete: never while its group is open, and
  not before it has emitted a key (a split at `lo` would hand the whole range on again). The veto is in
  `wanted`, in `decide` (before the coin, structure boundaries and the gates) and asserted before `commit`;
  such a range is left out of the split rank. Both children are ordinary fresh ranges; held rows are dropped
  and listed again by whichever child owns their key (`groups.split_relisted_rows`). Nothing crosses an
  `Expand` call: a retry, a requeue or the drainer's expansion starts fresh.
- *Bounds.* A bounded range lists until a key above `hi` or EOF, not until `c == hi` (hi's rows may continue).
  A resume drops every row at or below its key, across pages, without moving the cursor. The inclusive-start
  drop removes the bound key's whole group across pages.
- *Order and progress.* Rows are nondecreasing; a row equal to the previous page's last key is allowed only
  when that key's group may continue. A continuing page that shows no key above the chain's last must carry a
  token outside the window of such pages' tokens (1,024), or it is an invalid page; ever-fresh tokens over one
  key earn watchdog credit only within the first 1,024 pages of a no-advance run. Raw keys are
  validated before bound and resume suppression; suppressed rows cannot reset that window.
- *Units.* `st.keys` counts rows (work); density, the gauge and the pivot take distinct keys.
- *Counters:* `groups.open_pages`, `groups.held_rows`, `groups.split_relisted_rows`, `groups.max_rows`, `groups.overfetch_rows`,
  `split.skipped.group_open`.

Group page fragments are detached on both sides before emission or retention: their row arrays and
string arenas cannot retain excluded rows outside the order budget. Native chains validate raw keys
before inclusive or resume suppression. Only a strictly greater observed key resets the token window;
same-key pages earn watchdog credit for at most the first 1,024 pages of a run. GCS also validates
strictly ascending generation ordinals across page seams, including empty and suppressed pages,
with state owned by each chain. `groups.max_rows` scans every group, including interior groups,
sequential listings and seed EOF. `groups.overfetch_rows` counts only rows above a range's upper bound
read to close its last group; resume suppression and split relisting have separate accounting.

## Registry and optional interfaces

A provider registers a `registry.Factory` from `init`: its URL scheme (`s3`; the empty scheme takes local
paths), its flag group (`Flags(fs, diag, shared) any` returns the provider's options value, filled by the
parse), `Defaults`, and `Open(ctx, Request) (Source, error)`. A provider on the range engine names the
engine's shared flag group (`Shared: keyrange.FlagGroup`, `SharedFlags`: the policy flags `--seed`,
`--structure`, `--floors`, `--sequential`, `--max-keys` and the admission flags
`--limiter`, `--limiter-slow-start`, `--request-rate`; all but `--limiter` and `--request-rate` diagnostic,
registered with `diag` only): the command registers each named group once and
passes its value (`*keyrange.Common`: a `keyrange.Policy` and an `objhttp.Admission`) to every provider that
names it. The command imports providers only for that
registration and passes the options value for the root's scheme as `listing.Config.Source.Options`, opaque to the
pipeline. `Open` validates the root without touching the output.

A `registry.Source` is the provider bound to its schema (`registry.Bind[R, P]`: the range type erased by
`source.Erase`, ranges boxed once each, rows untouched; `Run` instantiates the order stage and the part router
for `P`), its schema `Info`, its `Locator` (kind, root, container, local path) and `Listed` (rows emitted, checked
against the rows ordered). Optional interfaces, each found by a type assertion:

| Interface | Meaning | S3 | Azure Blob | fs |
|---|---|---|---|---|
| `Identified` | its part of the resume identity: named fields, each with a canonical form for comparison | source, endpoint (canonical: `NormalizeEndpoint`), region, fetch owner | source, endpoint (canonical: `CanonicalRoute`), account, container, flat_asserted | – |
| `Resumable` | `Restart(after)` before `Root`: list only keys after the last published key | `(K, ⊤]` | `(K, ⊤]` | – |
| `Capable` | per-instance capabilities (`Resume`): an instance may decline what its type implements | `Resume` | `Resume` | – |
| `Excluder` | leaves an output directory inside its root out, after the output is prepared | – | – | yes |
| `Reporter` | its stats under its name in the summary, a text line, error count, metadata tier, and for a remote source its `Coverage` (target, filters, complete, stop reason) and `Activity` for swath's run summary | `s3`, coverage | `azblob`, coverage | `listing` |

A source that is not resumable (or declines `Resume`) runs without a journal; it still takes the output lock
like every run with an output. `swath resume` on it is an error.

## Provider capabilities

What a provider must state about itself (built: `source.Capable` with `Resume`, `Seek` and `RankOracle`,
`source.Resumable`, `source.Disposer`):
- **Applicability:** its seek kind and whether it has a rank oracle (above).
- **Order:** output is unsigned byte order; a provider whose native order differs sorts or partitions inside
  itself; there are no per-range comparators.
- **Bounds:** explicit bound inclusivity, as S3's `(lo, hi]`.
- **Bounded cost:** one `Rows` call has a bounded cost; no unbounded in-memory directory or page.
- **Restart:** start after a key; the journal stores no provider state. A provider that can only replay
  tokens must re-list to the key or declare `Capabilities.Resume` false.
- **Disposal:** a cancelled run releases every queued range's resources: the order stage disposes ranges it
  never expands, including children an `Expand` emitted before failing (`Child` takes ownership even when it
  returns an error). A range a worker has taken is the provider's: its `Expand` releases it however it ends,
  a return on an already-cancelled context included. Filesystem chunks release their shared directory
  descriptor both ways (`TestCancelReleasesChunkFDs`, `TestCancelledChunkReleasesItsFD`).
