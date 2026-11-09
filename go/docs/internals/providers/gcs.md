# GCS (`internal/source/gcs`)

## Support and differences

`gs://bucket[/prefix]` uses the native JSON API for current objects, or live and noncurrent
generations with `--all-versions`. Soft-deleted objects are outside its listing scope.
[Shared object-store controls](../../operating.md#object-store-requests) cover authentication,
request timeout, retries and admission.

An anchored literal `--include` can narrow the listing prefix. Google's own endpoint has a
reference Class A request-cost estimate; custom endpoints have none. GCS does not honor
`Retry-After`. `--anonymous` overrides ambient and explicit credentials silently: it prints no
ignored-credentials note. Endpoint validation rejects user information and URI-template braces
in the path, but does not reject queries or fragments.

## Provider implementation

**Client** (`internal/source/gcs/client`). Google's generated JSON API client
(`google.golang.org/api/storage/v1`) sends one `objects.list` GET per attempt: `ENDPOINT/b/BUCKET/o` with
`projection=noAcl`, `prefix`, `delimiter`, `startOffset`, `endOffset`, `pageToken`, `maxResults` and
`userProject` when set (`TestRequests`; the rest of the query is the generated client's). **Endpoint**
(the shared `--endpoint`, default `https://storage.googleapis.com/storage/v1/`): an `http` or `https` URL with a host,
no user information (a password would be recorded; `source` `TestRegisteredEndpointRefusals`) and no URI-template brace (`{`, `}`, `%7B`, `%7D`) in its path, which the generated client would expand as a
template (an empty expression panics, a named one is dropped). `client.Endpoint` normalizes it once, for the
client and the resume identity alike: a literal `/` is added when the escaped path does not end in one, and
escapes are kept, so `…/tenant%2F` is the base `…/tenant%2F/`. The generated client then resolves `b/BUCKET/o`
against that base and re-escapes the decoded path, and swath accepts its reading: dot segments are removed
(`/a/../storage/v1/` lists `/storage/v1/b/BUCKET/o`, `/a/./storage/v1/` lists `/a/storage/v1/b/BUCKET/o`) and
an escaped separator is sent as a separator (`/proxy%2Ftenant/storage/v1/` lists
`/proxy/tenant/storage/v1/b/BUCKET/o`, `/tenant%2F` lists `/tenant//b/BUCKET/o`); other escapes are sent as given
(`/custom%20base/v1`). Two such spellings of one destination record different identities, which refuses a resume
that was safe; two endpoints with one identity always list one path (`TestEndpoint`,
`TestEndpointIdentityIsDestination`). The generated client is built with a logger that emits nothing: its
default one, switched on by `GOOGLE_SDK_GO_LOGGING_LEVEL`, writes request headers, `Authorization` included,
and response bodies to stderr (`TestSDKLogsNothing`). On Google's endpoint the request asks for
the listed fields only (`fields=kind,nextPageToken,prefixes,items(name,size,updated,…)`); another endpoint (an
emulator or the replay server) gets the full response. The generated client makes one request per call and never
retries: `objhttp.Do` retries, backs off and admits requests, and an attempt is counted from just before the call
until it returns. Below it, every response passes `objhttp.Guard`, which reads a success body up to 16 MiB (a
longer one fails the attempt, class `other`) and an error body up to 64 KiB, closes the original body on every
path, and rejects a 200 that carries a top-level `error` member (`"error": null` included), which the generated
decoder would drop; the body is parsed for that only when it can hold one (a backslash, or `"error"` in any
letter case). The decoded page must name `kind: storage#objects` (so `{}`, `null`, an empty body or another JSON
value is a `parse` failure, retried, never an empty final page), every item must be present, a size must fit
`int64` and a time must be RFC 3339. Any 2xx other than 200 is an answer, not a page (class `other`, or
`throttle` by its reason). An answer is a `client.Error` by the HTTP status sent (never the body's `code`), with
the first reason and the message; a 400 names its query. The User-Agent is swath's product token
(`swath-go/<version>`) followed by the generated client's. `TestAnswers` drives each scripted answer through
the client: class, retry decision, attempts and counters, the answer, every body closed once, no redirect
followed. Credentials: `--anonymous` (shared with S3 and Azure Blob) sends none, and is the only way a request goes
unauthenticated (`TestAnonymousPrecedence`); `--gcs-credentials FILE` loads Google credential JSON through the pinned OAuth library; otherwise Application Default Credentials, found by the client library's own search
(`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud file, the metadata server on Google Cloud). Credentials that
cannot be found or do not load refuse the run before any listing request (`objhttp.CredentialError`, class
`auth`), never anonymity. The token source lives as long as the client, built on a context that is never
canceled. Each attempt bounds its own wait through [the shared token waiter](../clients.md#token-waits).
Exchanges that use the supplied OAuth HTTP client have `--request-timeout`; metadata refresh uses the
metadata SDK's HTTP timeout and retries, with no total refresh deadline. Ending a waiter does not cancel
that refresh. A token endpoint's or the metadata server's (`metadata.Error`) 5xx, 408, 429, transport failure
or timeout is retried; a refused grant stops the request (`TestAuthorizedUserADC`, `TestSlowRefreshBounded`,
`TestRefusedGrant`, `TestADCConfiguredButBroken`). `TestConcurrentBearerRequests` checks concurrent client
wiring; coalescing mechanics are tested in `source/tokensource`. The token is fetched before the attempt is
counted and set as `Authorization`. The supplied HTTP client bypasses the generated client's authentication
construction, so swath owns bearer injection; a credential failure is an `objhttp.CredentialError` and not
an attempt. The scope is `devstorage.read_only`. The client is Google's
generated `google.golang.org/api/storage/v1` client, with `golang.org/x/oauth2` credentials ([clients.md](../clients.md));
`gcs` `TestClientPagesMatchGoogleClient` compares the client's pages, through
the production fields selector too, with the generated client's own full responses. Retries: 429, 408, 5xx and transport errors, up to 9 attempts with exponential backoff
(100 ms to 5 s); exhaustion is requeued by the engine, as for S3. *Failures* are normalized once into `objhttp.Failure` with transient/throttle/timeout signals,
HTTP status, the first reason, and a safe cause. A 403 with `rateLimitExceeded` or
`userRateLimitExceeded` is a throttle; another 403 is a terminal permission refusal. A token endpoint
outage or timeout remains transient, independently of report labels. Diagnostics retain `auth`,
`access_denied`, `throttle`, `timeout`, `parse` and `unsupported`
([Run report](../../cli.md#run-report)); other errors keep status/reason without a separate label.
A 200 outside the objects.list envelope is a retried parse failure, never an empty final page
(`listing` `TestGCSInvalidEnvelopeNeverCommits`). A parsed invalid page preserves `objhttp.ErrInvalidPage`;
the engine wraps it as a protocol violation (`cause` `protocol`, unresumable). Redirects are never
followed, including with a supplied HTTP client, so they cannot change the resume source
(`listing` `TestGCSRedirectCannotChangeResumeSource`).

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
run resumed across that change holds parts of both kinds ([../dataset.md](../../dataset.md#schema-registry)). Two columns share
S3's names but not its meaning. `last_modified` is `updated`, which GCS also moves when only the object's metadata
changes; S3's `LastModified` moves only when the object is written. `time_created` is the generation's write
time and never changes. `etag` is the JSON API's opaque entity tag, which also changes with metadata and is not a
content hash, so compare content with `md5` or `crc32c`, never an ETag across stores. GCS omits `md5Hash` for
composite objects, whose `crc32c` remains, and omits both hashes from listings of objects encrypted with
customer-managed (CMEK) or customer-supplied (CSEK) keys. A missing hash is a null column, and when both are null
a comparison needs a hash from elsewhere
([object resource](https://cloud.google.com/storage/docs/json_api/v1/objects),
[ETags](https://cloud.google.com/storage/docs/metadata#etags)). **Identity** for resume is the recorded inputs: the source (bucket and prefix), the endpoint (as
`client.Endpoint` normalizes it for the client, above; scheme and host case are the only other equivalence) and the billed project
(`--gcs-user-project`), as given (`TestIdentity`); credentials may change. A run with `--max-keys`, or through a
supplied `Options.Client` (whose endpoint and project are not those inputs), cannot resume. A journal from a
build that recorded the resolved route instead of the endpoint is refused (`endpoint` not recorded;
[upgrade.md](../../upgrade.md), "Changes between Go builds"; `TestOldJournalRefused`).
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
`Info.Groups` set. Its [version roles](../parquet-output.md#version-roles) declare `generation` as ID and
`time_deleted` as deleted-at. Its spill codec uses the [shared payload delta encoding](../ordering.md#payload-delta-codec).
`gcs.ExclusiveStart(name)` is the start that lists strictly after a name, `name + NUL` (checked live: it lists
none of the name's generations); for a 1,024-byte name, whose successor `AcceptBound` refuses, the provider
sends `startOffset = name` and declares it (`Description.InclusiveStart`), and the engine's inclusive drop
removes all of the name's rows, across pages. A page whose generations of one name do not ascend is invalid.

**Connections.** The built client rotates one generated service per [pooled transport](../clients.md#pool-construction).
`TestServicePool` checks service count and supplied-client wiring; `TestServicePoolRoundRobin` observes
actual requests through each service. Transport construction and HTTP/2 connection spread are tested in
`source/objhttp`.

**Overload signals** (`client.Error.Failure`):

| Answer | Shared limiter input |
|---|---|
| 429, `rateLimitExceeded` or `userRateLimitExceeded` (including a 403) | Throttle |
| 408 or an attempt deadline | Timeout; holds growth, does not cut |
| Another 5xx, including a plain 503 | Transient; no throttle signal |

[Request admission](../clients.md#request-admission) describes the shared controller and its tuning limits.
