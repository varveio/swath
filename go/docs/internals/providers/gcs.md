# GCS (`internal/source/gcs`)

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
run resumed across that change holds parts of both kinds ([../upgrade.md](../../upgrade.md#the-downstream-reader-dispatch-on-schema-id)). Two columns share
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
