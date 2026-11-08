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
rank oracle (`token_only` with `--sequential`); the filesystem has no seek and no order (a directory is a
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

## The list clients: hand-rolled on the SDK's auth

Each object-store provider makes one call, its list request, through a client of its own, and takes
authentication from the vendor's SDK modules.

**The SDK's.** S3: the credential chain (environment, shared config and profiles, SSO, web identity and STS,
IMDS) and the SigV4 signer (`aws-sdk-go-v2/config`, `credentials`, `aws/signer/v4`). GCS: Google's
credential loading (service-account and authorized-user files, Application Default Credentials, the metadata
server) and token refresh (`golang.org/x/oauth2`, `cloud.google.com/go/compute/metadata`).

**Ours.** The one list call per provider (ListObjectsV2; `objects.list`), built and sent by
`internal/source/s3/client` and `internal/source/gcs/client`; S3's response parser, a hand-written scanner
over the body that allocates per page, not per object; GCS's decoding of only the listed fields with
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

The SDK service clients (`service/s3`, `google.golang.org/api/storage/v1`) are test-only oracles
(`internal/source/s3/sdkoracle`, `TestClientPagesMatchSDK`, `TestClientPagesMatchGoogleClient`); the binary
links neither ([testing.md](../testing.md)).

## S3 (`internal/source/s3`)

**Client** (`internal/source/s3/client`). The ListObjectsV2 request, the retry policy and the response parser
are this package's. Credentials and SigV4 come from the AWS SDK's auth modules (`aws-sdk-go-v2/config`,
`credentials`, `aws/signer/v4`). The SDK's S3 service client (`service/s3`) is not linked into the binary:
`go version -m swath` lists no `service/s3`.

- *Request:* one signed GET with `list-type=2`, `prefix`, `start-after`, `continuation-token`, `max-keys`,
  `delimiter`, `fetch-owner` and `encoding-type=url`. The query is built sorted and SigV4-encoded, so the URL
  sent is the canonical query that is signed.
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
  for up to 30 s (a web-identity token file not yet mounted is polled for); a lookup that fails for good is a
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
  when its answer is read, so in-flight requests never exceed the limit. The limit starts at the worker count;
  each throttle answer (S3 429/503/`SlowDown`; GCS 429 and rate-limit reasons) multiplies it by 0.7 (floor 1;
  an answer at the floor removes nothing and does not restart the window, `limiter.floor_throttles`);
  after 10 s without a decrease it grows by one per second back to the worker count. Growth (never the limit)
  is held while three or more attempt timeouts fell in the last 10 s, or while the EWMA of successful attempts'
  latency exceeds twice its baseline (a rolling minimum re-floored every minute), with one step per 30 s let
  through when there is progress. Its only inputs are the attempts' own latency and answers; no permit is held
  during a backoff, and admission waits are not latency samples. A permit is returned however the attempt ends,
  a panic included. The rules and constants are Java swath's
  `ConcurrencyGauge`. Java's slow start is behind `--limiter-slow-start` (off by default until measured,
  `objhttp.NewSlowStartLimiter`): the limit starts at min(4, workers) and each paced growth step doubles it
  instead of adding one, the first double on the first success and then at most one per second (4 to 64 in
  3 s), under the same brakes (the latency valve's step stays +1); the first throttle answer or attempt timeout
  ends the ramp for good. It applies to whatever builds the client, a resume included; the reason in Java is
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
  in range), `canceled`, `other`. `throttle`, `timeout`, `server`, `transport` and `parse` are transient
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
`client.ErrInvalidPage`. `TestDecisionsGolden` (`internal/source/s3`) pins the engine's decisions through
the S3 adapter: for every fixture and hostile keyspace and four deterministic configurations, a hash of the
covered intervals, the requests sent, the output and the non-zero counters, against
`testdata/decisions.golden`.

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
time, `TestTokenWaitersCoalesce`); a token endpoint's or the metadata server's (`metadata.Error`) 5xx, 408, 429,
transport failure or timeout is
retried like a listing failure (counted in `timeouts` for the last), a refused grant stops the request
(`TestAuthorizedUserADC`, `TestSlowRefreshBounded`, `TestRefusedGrant`, `TestADCConfiguredButBroken`, against a
local token endpoint). The scope is `devstorage.read_only`. The binary links
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
`etag`, `generation`, `metageneration`, `storage_class`, `md5` and `crc32c` (base64, as listed) and
`content_type`. **Identity** for resume: the source, the route (the JSON API base and the bucket, as the
client requests it; scheme and host case are the only equivalence) and the billed project the client sends
(`--gcs-user-project` for the built-in client); credentials may change. A run with `--max-keys`, or through a
supplied client that does not name its route and billed project (`gcs.Routed`), cannot resume.
Counters: the engine's, then the client's (`attempts`, `retries`, `throttles`, `timeouts`,
`parse_failures`), `end_offset_omitted` and `transport.pool_size` (the client's transports; 0 with a supplied
`Options.Client` that does not report one).

**Connections.** The built client spreads its requests round-robin over `client.PoolSize(--workers)`
transports, one per 16 workers (1 to 8), sharing one TLS session cache: Go's HTTP/2 transport opens a second
connection only at the server's stream limit (about 100), so a single transport would carry all 64 workers on
one connection, which caps throughput once the engine keeps many requests in flight
(`TestTransportPoolSpreadsConnections`). A supplied `Config.HTTPClient` is used alone (a pool of 1). There is
no flag; the pool follows `--workers`.

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
    (`Safe`: valid UTF-8 without C0, DEL, C1, `%`, surrogates or non-characters), which S3 declares
    explicitly (`s3.AcceptBound`). Synthesized pivots always use the pivot alphabet. The predicate guards
    every bound the engine sends: pivots (guard clause `unsafe_pivot`), cursors (`unsafe_cursor`), seed and structure
    boundaries and a resume's start (K's longest accepted prefix);
  - `Requeue` (which failures are asked again after a backoff instead of failing the run), `InvalidPage`
    (the error contract breaks wrap), `Size` (a row's bytes) and the provider's own `Counters`, reported
    after the engine's.

The engine's options are the scope prefix, `Sequential`, `Seed`, `Structure` (`StructureOff`, `StructureAuto`, `StructureOn`),
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

**Root and seed.** The root is `(⊥, ⊤]`. With the seed (on by default in the command, `--seed=false` turns it
off; never with `--sequential`) the root is a seed that emits no rows and tiles the scope at cut points
`(⊥,c1], …, (ck, ⊤]`, all real folders found by delimiter listings (`/`, 1,000 entries a page,
`Probe` set):
- *Pagination.* The top level follows its continuation token up to 4 pages (`seed.pages`), so a top level of
  up to about 4,000 folders is tiled whole; past that it is `seed.truncated`. Four pages are already four
  times the most cuts a seed keeps.
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

The seed is a serial pre-phase of 1 to 6 waves: on a bucket of a few pages it is paid on the critical path
for little gain, which is why `--seed=false` remains. The default is measured against `--seed=false` on
production key lists of 1M objects and more ([measurements](../measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms));
its cost on a bucket of a few pages is not measured.

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
ranges where it ends in 630-810 without them (`TestAdmissionMillionFlat`). What remains are empty upper children of pivots placed in a folder
gap (a date's `day=32`-`day=99`, a letter after a counter's digits), which only a structure probe can see.

**Floors** (Swath's owner-split gate chain; off by default, `swath diag list --floors` turns them on;
[measurements](../measurements.md#policy-switches)).
Two gates on bounded ranges, read from a position estimate rather than a request (Swath's default
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

**Guards**, checked before any child and counted per clause: `done` (never after the page that ended the
range), `no_pivot` (the owner keeps listing), `unsafe_cursor` (`c` must be a usable start-after), `cursor`
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
   only); the target is `c + page span × (max(4, hunger) << g)`, at most halfway to `hi`, cut after its first
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
(`split.skipped.no_demand`). Generation and anchor live on the immutable `Range`; nothing persists them (a
resume starts new chains).

There is no 1-key pivot probe (Java's `max-keys=1` request after the pivot, moving an empty upper child's
pivot toward the cursor): on the deep replayed recordings ([measurements](../measurements.md#policy-switches))
it cost more than the empty children it saved, a serial round trip before each split that lowered
concurrency: 27–31 % more requests and 2.2–2.3× the wall.

**Counters** (in the run summary): objects, bytes, pages, requests, retries, `overfetch_keys`; seed
(prefixes, pages, requests, round trips, levels, cuts per level, why it stopped, cuts, unsafe, subsampled,
sentinel and why declined, resumed); splits (considered, accepted bounded and
frontier, rejected per guard clause, skipped for no demand, outranked, not continuing, or by an admission
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
(`alphabet` is a non-ASCII or `%` page: issue-shaped keys turn density off, and only the reflection and the
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

**Resume.** `Restart(K)` makes the scope `(K, ⊤]`: with the seed it is tiled as above, without it the root is
one range `(K, ⊤]`, split as on a fresh run; S3 is `source.Resumable` through it ([durability.md](durability.md)).

## Registry and optional interfaces

A provider registers a `registry.Factory` from `init`: its URL scheme (`s3`; the empty scheme takes local
paths), its flag group (`Flags(fs, diag, shared) any` returns the provider's options value, filled by the
parse), `Defaults`, and `Open(ctx, Request) (Source, error)`. A provider on the range engine names the
engine's shared flag group (`Shared: keyrange.FlagGroup`, `SharedFlags`: the policy flags `--seed`,
`--structure`, `--floors`, `--sequential`, `--max-keys` and the admission flags
`--limiter`, `--limiter-slow-start`, `--request-rate`): the command registers each named group once and
passes its value (`*keyrange.Common`: a `keyrange.Policy` and an `objhttp.Admission`) to every provider that
names it. The command imports providers only for that
registration and passes the options value for the root's scheme as `listing.Config.Source.Options`, opaque to the
pipeline. `Open` validates the root without touching the output.

A `registry.Source` is the provider bound to its schema (`registry.Bind[R, P]`: the range type erased by
`source.Erase`, ranges boxed once each, rows untouched; `Run` instantiates the order stage and the part router
for `P`), its schema `Info`, its `Locator` (kind, root, container, local path) and `Listed` (rows emitted, checked
against the rows ordered). Optional interfaces, each found by a type assertion:

| Interface | Meaning | S3 | fs |
|---|---|---|---|
| `Identified` | its part of the resume identity: named fields, each with a canonical form for comparison | source, endpoint (canonical: `NormalizeEndpoint`), region, fetch owner | – |
| `Resumable` | `Restart(after)` before `Root`: list only keys after the last published key | `(K, ⊤]` | – |
| `Capable` | per-instance capabilities (`Resume`): an instance may decline what its type implements | `Resume` | – |
| `Excluder` | leaves an output directory inside its root out, after the output is prepared | – | yes |
| `Reporter` | its stats under its name in the summary, a text line, error count, metadata tier, and for a remote source its `Coverage` (target, filters, complete, stop reason) and `Activity` for swath's run summary | `s3`, coverage | `listing` |

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
  never expands, including children an `Expand` emitted before failing. Filesystem chunks release their shared
  directory descriptor this way (`TestCancelReleasesChunkFDs`).
