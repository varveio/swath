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

**Why.** CPU per listed object, measured on fake buckets (synthetic; [REPORT.md](../../REPORT.md)): S3, the
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
  buckets, Outposts, the GovCloud-specific and ISO partitions beyond the plain regional host,
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
  progress and no answered request for `--stall-timeout`: exit 75, resumable; `objhttp.Counters.Answered`,
  through `source.Live`, counts the attempts that returned a usable answer, never a failed one). Every request, including probes and the seed page,
  goes through the same path.
- *Shared with GCS* (`internal/source/objhttp`): the retry loop (`objhttp.Do`: attempts, backoff, exhaustion),
  the class vocabulary, transport classification, `CredentialError` (an S3 credential refresh that fails is one:
  a credential service's 5xx, 408, 429 or throttling code, a transport failure or a timeout is transient, retried
  and requeued, `TestCredentialOutageRecovers`; a refused grant is `auth`; `Classify` reads a credential failure
  before a wrapped deadline, so a token exchange's own timeout is never taken for the caller's cancellation), the client counters and route canonicalization; `objhttptest.Contract` is the retry and
  classification contract both clients' tests run with their own wire answers (`TestRetryContract`). The
  report brief, coverage and capabilities come from `keyrange.Report` and `keyrange.Capabilities`. Each client
  keeps its answers' meaning (status, codes, reasons), its parser and its authentication.
- *Admission* (`objhttp.Limiter`, both clients; on by default in the command, `--limiter=false` turns it off; a
  library caller's zero `Policy` leaves it off): every HTTP attempt (pages, seed,
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
  `ConcurrencyGauge`; Java's slow start (from 4, doubling per second) is not ported. Counters `limiter.*`.
- *Rate cap* (`objhttp.Rate`, `--request-rate N`, off by default): after its permit every attempt takes a token
  from a bucket of one, refilled every 1/N, so attempts are spaced at least 1/N apart whatever the limit
  (`TestRateCapsAttempts`: 60 attempts with retries from 8 callers take at least 59/N, and with 2 permits never
  more than 2 in flight). A wait that the caller cancels gives its token back. Counters `rate.*` (feature
  `request_rate`).
- *Error classes* (`client.Classify`, one table test covers each): `auth`, `access_denied`, `no_such_bucket`,
  `redirect`, `throttle`, `timeout`, `server`, `transport`, `parse` (a body that does not parse),
  `invalid_page` (a page that breaks the listing contract: out of order, truncated without a token or a key
  in range), `canceled`, `other`. `throttle`, `timeout`, `server`, `transport` and `parse` are transient
  (retried, then requeued); the rest stop the run, and the class is the run outcome's `cause`.
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
the S3 adapter: for every fixture and hostile keyspace and six deterministic configurations, a hash of the
covered intervals, the requests sent, the output and the non-zero counters, against
`testdata/decisions.golden`, which was generated before the engine left the S3 package.

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
contract: `client.ErrInvalidPage`, which the engine wraps), `canceled`, `other`; the class is the run
outcome's `cause` (`TestClassify` drives each through the client, `TestInvalidPageClass` through the engine).

**On the range engine.** Object names list in unsigned UTF-8 byte order and `startOffset` takes any name,
so `Description.Listing` is `keyrange.Listing`. `startOffset` is inclusive (`StartInclusive`); `endOffset`
is exclusive and set to `ExclusiveEnd(Through, …)` when that bound is at most 1,024 bytes, else omitted
(`end_offset_omitted`) and the engine trims. `AcceptBound` takes any valid UTF-8 name of at most 1,024 bytes
without CR or LF: `%`, control characters and non-ASCII names are sent as start bounds (query-escaped).
Every request of a token chain repeats its `startOffset`, `endOffset`, prefix and delimiter with the
`pageToken`, and empty continuing pages are followed by the engine. The engine's policy flags are the shared
ones (`keyrange.Flags`); the GCS flags are prefixed `--gcs-` because one flag set serves every provider.

**Schema** `swath/gcs-objects/v1`: the key (the object name), `size`, `last_modified` (`updated`, µs),
`etag`, `generation`, `metageneration`, `storage_class`, `md5` and `crc32c` (base64, as listed) and
`content_type`. **Identity** for resume: the source, the route (the JSON API base and the bucket, as the
client requests it; scheme and host case are the only equivalence) and the billed project the client sends
(`--gcs-user-project` for the built-in client); credentials may change. A run with `--max-keys`, or through a
supplied client that does not name its route and billed project (`gcs.Routed`), cannot resume.
Counters: the engine's, then the client's (`attempts`, `retries`, `throttles`, `timeouts`,
`parse_failures`) and `end_offset_omitted`.

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
    boundaries, the seed's top-up after the top page's last entry (not sent when vetoed,
    `seed.top_paginated_declined.unsafe`) and a resume's start (K's longest accepted prefix);
  - `Requeue` (which failures are asked again after a backoff instead of failing the run), `InvalidPage`
    (the error contract breaks wrap), `Size` (a row's bytes) and the provider's own `Counters`, reported
    after the engine's.

The engine's options are the scope prefix, `Sequential`, `Seed`, `SeedDescent`, `Structure`, `Probe`,
`Floors`, `Workers`, `MaxKeys`, `Resume` and `PageSize`, the nominal page size (default 1,000): data and
seed requests ask for it, structure and 1-key probes for no more, and the floors count pages of it. The
engine sorts and deduplicates each page's common prefixes (`prefixes_normalized`), so a provider need not.
The seed reads a probe page as two facts, what it showed (prefixes and objects) and whether it continues: a
sample is weighed and classed by what it showed, and a continuing sample that showed less than a nominal page
(a provider may cut pages short) is taken as a full page, an estimate counted in
`seed.short_continuing_estimated` (`TestSeedObservedVsContinuing` in the planner, `TestSeedShortContinuingThroughClient` through the GCS client); a continuing structure probe is a partial
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

**Root and seed.** The root is `(⊥, ⊤]`. With `-seed` the root is a seed that emits no rows and tiles the
scope at cut points `(⊥,c1], …, (ck, ⊤]`. The seed is Swath's hybrid seed descent (`HybridSeedPlanner` at
its defaults: mass-aware, fan-out tiling, radix bands), a walk of the folder hierarchy with delimiter
pages (`/`, one page of 1,000 each, never paginated):
- *Budget:* aims at `min(1000, 4 × workers)` tiles with at most `min(256, that)` probes, of which up to 32
  are samples; the walk itself stops at the budget less the sample share.
- *Top level:* its folders are cuts. A truncated top page is read once more from its last item; a top level
  that still holds more than a page of folders is left whole (no descent). No folders and a truncated page:
  the scope is one dense flat folder, cut into radix bands (below).
- *Descent:* queued folders are probed shallowest first, then by the widest byte gap to the next cut. A
  folder listed in one page adds its subfolders as cuts and queues them (`narrow`). A truncated folder is
  classified: objects only (`flat_wide`, the first one is cut into radix bands); mostly `key=value`
  subfolders (`fanout_tiled`: an even subsample of up to `workers` of them become cuts); otherwise 3
  subfolders are sampled, and if at least half hold 8 or more objects, subfolders or a full page
  (`heavy_cut_banded`), up to `workers` subfolders become cuts, else it is a one-object-per-folder explosion
  left whole. Past the sample budget, the earlier samples' majority decides.
- *Cuts:* more cuts than the target are subsampled evenly, or by mass when at least 8 cuts were sampled
  for their weight (one sample probe each, a full page weighing 1,000). Radix bands: after a dense flat
  folder `d`, cuts `d·b` for up to `min(93, max(8, target))` bytes `b` spread over `!`..`~` without `%`.
  The closing sentinel `prefixCeil(pk)` is added when the top level was complete, ended with its greatest
  folder `pk` and a tile slot is spare. Unusable start-after values are dropped (`seed.unsafe`).
- *Probes in parallel:* the walk is a state machine fed one probe result at a time, so its plan is the one a
  serial walk makes (`TestSeedDescentConcurrentEqualsSerial`). Where its next probes are known (the queued
  folders of the depth being walked, the rest of a sample) they are fetched together, up to one per
  worker; a probe fetched ahead and never asked for is counted (`seed.probes_unused`) and dropped.

`swath diag list --seed-descent=false` keeps the single delimiter page instead: its common prefixes are the
cuts, at most `min(1000, 4 × workers)` evenly subsampled, with the closing sentinel
`(pk, prefixCeil(pk)], (prefixCeil(pk), ⊤]` when the page was complete and ended with a prefix.

**Splits are owner-side.** A range's children become claimable only when its `Expand` returns, so there are
no thieves. After each full page the owner takes the provider mutex and computes demand,
`hunger = workers − active − queued`. If `hunger > 0` and the range ranks among the `hunger` largest eligible
ranges (open frontier first, then keys listed, then age), it emits `Child((c, m])` and `Child((m, hi])` and
returns, where `c` is the last key emitted. `(lo, hi] = (lo, c] ∪ (c, m] ∪ (m, hi]`, disjoint and in order.
Every split feeds an idle worker and no LIST is repeated.

**Floors** (Swath's owner-split gate chain; off by default, `swath diag list --floors` turns them on; REPORT.md
"Gate-chain floors").
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

**Structure probes** (after Swath's thief structure probes; behind `swath diag list --structure`, off by
default). A bounded range whose cursor has a folder below the scope prefix sends, alongside its next
page and only while a worker is idle, one delimiter request (`max-keys` 32, start-after the cursor) per
folder level: the folder where the cursor and `hi` diverge, then the next folder below it on the cursor's
path. The coarsest level with a folder inside `(c, hi)` gives the range its boundaries. At a later split
decision, if the result has arrived, the pivot is the median boundary inside `(c, hi)`, or the furthest when
that level's page was full; the byte pivot is used otherwise. A split never waits for a probe
(`structure.late` counts decisions taken while one was in flight). A probe still in flight when its range's `Expand` returns is cancelled. Children inherit the boundaries inside
their bounds, so one probe serves several generations of splits. Eight probes in a row that find no folder
at any level stop probing in that range (`structure.suppressed`). Swath sends these probes serially, coarse
to fine, after a 1-key probe found the upper child empty; here the levels go at once and nothing waits.

**Guards**, checked before any child and counted per clause: `done` (never after the page that ended the
range), `unsafe_cursor` (`c` must be a usable start-after), `cursor` (`m > c`), `bound` (`m < hi`, or below
`prefixCeil(scope)` on the frontier), `no_pivot` (the owner keeps listing).

**Pivots.** Three steps:
1. **The spec's pivot is the default:** swath's `ByteMidpoint`, i.e. the safe midpoint of `(c, hi]`, or
   `ForwardReflect` on the open frontier, with the 1,024-byte cap.
2. **A density refinement from the last page.** Keys are read as numbers in the alphabet seen on that page.
   The target is `c + page span × max(4, hunger)`, at most halfway to `hi`, cut after its first character
   that differs from `c`. It is taken only when it lies between the cursor and the spec pivot (nearer the
   cursor), or when the spec pivot is within one page span of the cursor. In that second case the spec
   pivot would leave the lower child empty: reflection from ⊥ moves only the last character.
3. **The 1-key probe (Java's pivot probe; off by default, `-probe` turns it on).** Before the split is published,
   one ListObjectsV2 with `max-keys=1` after the pivot checks that the upper child `(m, hi]` holds a key. If
   it does not, the pivot moves to the safe midpoint of `(c, m]` and is probed again, up to 3 probes. Then
   the split is skipped, and the range does not try again for 2, 4, … 64 pages.

The split's demand slot is reserved while it is probed, so concurrent owners do not split for the same idle
worker. Every pivot is safe (I12). Without `-probe` the provider lists with the kernel alone.

Measured on five replayed recordings (REPORT.md, "Step 5a"), the probe costs more than the empty children
it saves: it is a serial round trip before each split, so it lowers concurrency, and runs were 1.4–2.2× slower
than without it. Without it the refined pivot matches the old one. So the probe is off by default.

**Why.** Before the probe, the density estimate was the first choice and could reach far past the data on
skewed shapes. On a replay of noaa-nws-fourcastnetgfs-pds, 1,362 of 2,927 splits left an empty upper child,
and the run used about 1 of 64 workers (REPORT.md "Step 5a").

**Counters** (in the run summary): objects, bytes, pages, requests, retries, `overfetch_keys`; seed
(prefixes, cuts, unsafe, subsampled, sentinel and why declined); splits (considered, extrapolated, accepted
bounded and frontier, rejected per guard clause, skipped for no demand, outranked or not continuing);
`child_empty.{lower,upper}`; the floors' skips; the seed descent's probes, classifications and
sentinel; structure probes and their pivots; with `-probe`, `probe.{requests,upper_empty,moved}` and `split.rejected.probe_empty`; `max_active`, `max_queued`, `mean_active`; busy and
sink-wait time.

**Sequential oracle.** `swath diag list --sequential` lists the scope as one continuation-token chain; its dataset is
byte-identical to the parallel core's. It is the only mode where `-max-keys` stops early (the dataset is
then marked incomplete).

**Resume.** `Restart(K)` makes the root one range `(K, ⊤]`, split as on a fresh run; S3 is
`source.Resumable` through it ([durability.md](durability.md)).

## Registry and optional interfaces

A provider registers a `registry.Factory` from `init`: its URL scheme (`s3`; the empty scheme takes local
paths), its flag group (`Flags(*flag.FlagSet) any` returns the provider's options value, filled by the
parse), `Defaults`, and `Open(ctx, Request) (Source, error)`. A provider on the range engine registers the
engine's policy flags (`--seed`, `--seed-descent`, `--structure`, `--floors`, `--probe`, `--sequential`,
`--max-keys`) through `keyrange.Flags` from its flag group: the group is registered once per flag set and
every such provider gets the same `keyrange.Policy`. The command imports providers only for that
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
