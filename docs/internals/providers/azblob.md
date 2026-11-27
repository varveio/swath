# Azure Blob (`internal/source/azblob`)

## Support and differences

`az://account/container[/prefix]` lists current blobs in flat namespaces, and with `--all-versions` every
entry (versions, snapshots, soft-deleted blobs; `NewVersions`, below). Current listings of hierarchical
namespaces use the [tree core](../tree-sources.md); grouped listings require a flat namespace. Namespace
admission is required even for a public container; `--azure-assume-flat` overrides an unknown result,
never an account reported as hierarchical.
[Shared object-store controls](../../sources/README.md#object-store-requests) cover authentication,
request timeout, retries and admission.

An anchored literal `--include` narrows the listing prefix (`withPrefix`). An `az://` root is not an
escaped URL (`%` and `+` are a name's own bytes; a `?` is the prefix's unless the query names a SAS
parameter), so the narrowed root keeps the SAS byte for byte and is taken only if `ParseURL` gives back the
same account, container, SAS and the prefix asked for; otherwise the root is kept
(`provider_declined_prefix`), and an include's text is never read as a credential. Namespace admission
still asks about the container (`TestNarrowedAdmission`); the oracle that the narrowed listing filtered
equals the root's listing filtered is `TestNarrowedListingIsTheFilteredListing`.
There is no tariff estimate, including on Azure's default endpoint. Only this provider honors
`Retry-After` (capped at `--request-timeout`, 30 s by default, counted by `retry_after_capped`).
It applies the shared [endpoint validation](../clients.md#endpoint-validation) to `--endpoint-url`; supply SAS
credentials separately. `--anonymous` with
`--azure-sas-token` or a SAS in the root is refused (exit 2), as every provider refuses it beside an explicit
credential; Azure credentials in the environment (`AZURE_STORAGE_SAS_TOKEN`, `AZURE_STORAGE_KEY`, the Entra ID
variables) are ignored with the shared one-line warning naming them. `--storage-class` filters on the access
tier (`storage_class`, the schema's declared storage-class column).

Root parse errors keep clean roots, redact SAS queries and hide credential-bearing authorities.

## Provider implementation

**Client** (`internal/source/azblob/client`): List Blobs and Get Account Information, described with the list
clients ([clients.md](../clients.md)), and Get Blob Properties for the [live source](#the-live-source)'s lookups. Every
attempt of List Blobs and Get Blob Properties, retries included, is reported to the `objfile.Observer` on its context, as
a list request of the prefix or a metadata request of the name; the command puts none there. Both are sent through the Azure
SDK's service and container clients (`sdk/storage/azblob` v1.8.3), with the SDK's retries off: swath's retry loop
(`objhttp.Do`) repeats an attempt, `objhttp.Guard` bounds every body and checks a listing's envelope before the SDK
decodes it, in the format its `Content-Type` names. A per-call policy pins `x-ms-version` to `2026-06-06`, the oldest version with the Arrow format, which the live service, swath-replay and Azurite all answer. Every
listing outside HNS inventory mode asks for the Arrow response format, then XML (`Accept: application/vnd.apache.arrow.stream,application/xml`),
and the service picks: an account or endpoint without Arrow listings (a hierarchical namespace, Azurite, the replay
server) answers XML, and so may an account whose eligibility changes mid-run. There is no option: XML is the
service's fallback, not swath's. An XML page must have an `EnumerationResults` root, nothing after it and no `Error`
element in it, and must hold `Blobs`; an Arrow page must read without error, with `NextMarker` and
`NumberOfRecords` in its schema metadata, that many rows, and each column behind what swath writes in a type the SDK reads; other columns, which swath
never reads, may have any type ([clients.md](../clients.md#azure-decoding-and-requests)).
Both convert to the same page (`TestArrowEquivalence`, recorded live). A missing or zero SDK timestamp
stays null in the dataset (`TestZeroSDKTimesAbsent`). Besides the first eight columns'
fields an item carries the entry fields of a grouped listing (below) and the properties every listing returns
(`client.Props`: access tier inferred and change time, archive status, rehydrate priority, last access time,
content encoding, server encryption, encryption scope, customer-provided key hash, lease state, tag count),
and, when a request's `client.Include` asks (`--azure-fetch-metadata`, `--azure-fetch-tags`,
`--azure-fetch-immutability`), the user metadata and index tags as JSON objects with sorted keys, the
immutability policy and the legal hold. Arrow and XML carry each of them (`TestGroupedPage`); each column
behind one is held to a type the SDK reads. `Content-CRC64` is not read: azblob 1.8.3's Arrow decoder drops
it, so it would be null or not by format. The zero `Include` sends no `include`. The provider sets an empty
`metadata` or `tags` asked for to `{}` and an absent legal hold to false, so null means not asked; metadata
stays null for a blob with a customer-provided key (List Blobs cannot decrypt it). A refused listing that
asked for tags (401, or 403 `AuthorizationPermissionMismatch` for a SAS without `t`: the service refuses the
whole request) keeps its class and gains the permission it needs in its message (`TestFetchTagsRefused`). The `list_format.*` counters say which format
each valid page came in. A hierarchy page's blobs and prefixes, which the SDK returns
apart, are merged back into wire order (UTF-16 code-unit order, `utf16order.Compare`). Filesystem Get Properties (`NamespaceInfo`, below) has no SDK call and is one plain HEAD.
The accepted success-body limit is 64 MiB; a listing page past it fails the request
(`client.ErrBodyCap`, not retried, not asked again with fewer entries). This does not bound buffer
capacity or decoded-page allocations; see [shared body handling](../clients.md#what-swath-keeps).
The root is `az://ACCOUNT/CONTAINER[/PREFIX]`;
the shared `--endpoint-url URL` replaces `https://ACCOUNT.blob.core.windows.net` (Azurite, the replay server), the
account then being the endpoint's first path segment (`http://127.0.0.1:10000` serves
`http://127.0.0.1:10000/ACCOUNT/CONTAINER`). Credentials, in precedence: `--anonymous` (shared with S3 and GCS, and the only way a request goes
unauthenticated); a SAS from
`--azure-sas-token`, else the root's query when it names a SAS parameter (`sig`, `sv`, `se`, `sp`, ...; found by
scanning the parameter names, whether or not the query parses; such a query must be a signed query string, or the
root is refused without quoting it; a `?` with no SAS parameter belongs to the prefix), else
`AZURE_STORAGE_SAS_TOKEN`; an account key from `AZURE_STORAGE_KEY` (Shared
Key: signed by the SDK's own policy, after swath's per-call policy, so the pinned version is signed with the rest;
the client's tests check the signature independently of the SDK); else Entra ID through `DefaultAzureCredential`, scope `https://storage.azure.com/.default`,
its tokens coalesced by `tokensource.Shared`, each fetched before the attempt and sent as the request's
`Authorization` header (the SDK's bearer policy and its 401 challenge re-send are never in the pipeline), and its
failures classified as GCS's (no answer, 5xx, 408, 429 or a
timeout retried; a refused grant stops the request). The chain is the SDK's, built as it
builds it (environment, workload identity, managed identity, the Azure CLI and Developer CLI logins), with no
discovery of swath's own: a chain that will not build is an `objhttp.CredentialError` at construction; one that finds
no credential (azidentity's own error) or whose grant is refused fails before sending the request.
A credential failure, during namespace admission as during engine listing, has class `auth` (`Azure Blob
client: credentials (...)`, exit 1), as S3's and GCS's: admission returns it as it is rather than asking the
Data Lake host, which would need the same token (`TestAdmissionKeepsCredentialFailure`); transient token
failures are retried per call
(`TestEntraDefaultChain`). A public container needs `--anonymous`; the command's error says so
when no credentials were found. Endpoint admission follows the [shared check](../clients.md#endpoint-validation). A SAS never
reaches a recorded file: `--azure-sas-token` is a secret flag (recorded as `[redacted]`), the root's query is recorded
as `?sig=[redacted]`, and a resume of either needs a SAS again, from `--azure-sas-token` or `AZURE_STORAGE_SAS_TOKEN`
(the command's `secretEnv` lets the variable stand in for the flag). The SAS is appended to every request, and
since azblob v1.8 the SDK puts the operation selectors (`restype`, `comp`) in the request path rather than
setting them on the query, so a SAS that held one would send it twice. The client refuses, when it is built and
before any request, a SAS naming any parameter of swath's requests (`operationParams`: the List Blobs and Get
Account Information parameters and the Data Lake host's `resource`), by decoded name in any case and whatever
its value; the refusal is `source.Unsupported` (exit 2) and names the parameter, never a value. With
`--anonymous` the SAS is ignored, unchecked (`TestSASReservedParameters`, `TestAzblobSASReservedParameter`).
Errors carry no SAS: answers that
quote the query, redirect targets, transport errors and the SDK decoder's errors are scrubbed of every parameter
the SAS names, found by name wherever the SDK sorted it into the query and however it is encoded (as sent,
decoded, escaped again), its value replaced and the rest of the text kept; a 400's parsed query drops the SAS's
parameters and keeps the request's (`TestSASRedacted`, `TestScrubSASParameters`). The Azure SDK's own
log, which quotes request URLs, is off in swath: the command clears azcore's log listener at startup, so
`AZURE_SDK_GO_LOGGING=all` prints nothing. A library host that leaves that log on gets each failed answer without its
body: the client keeps the body from the SDK and reads the code itself (`TestSDKLogWithheld`). The root is redacted where the command parses
it (`registry.Factory.Redact`, `registry.Redact`), before the `-v` start line, the recorded command and the
report's `invocation.source` see it, so an admission refusal records no SAS either; the identity, the manifest
and the errors carry the root without its query. A scope prefix holding a byte in EE..F4 is accepted and listed
through the adapter like any other.

**Admission.** Before choosing a listing engine, admission establishes the namespace. (1) Get Account Information on
the container path: `x-ms-is-hns-enabled` `false` selects flat listing, `true` selects tree listing for current blobs. (2) When that call is refused (401,
which Azure answers any anonymous caller, or 403) or answers without the header, Filesystem Get Properties on the
account's Data Lake host (`client.NamespaceInfo`: `HEAD https://ACCOUNT.dfs.core.windows.net/CONTAINER?resource=filesystem`,
the blob host with its `blob` label replaced, so only for the exact default endpoint, `https://ACCOUNT.blob.core.windows.net`
with the configured account, no port and no path; with any other `--endpoint-url` there is no such host and
this step is skipped, as it is with Shared Key, which this request is not signed with): `x-ms-namespace-enabled` `false`
selects flat listing, `true` selects tree listing for current blobs. Azure answers it anonymously for a public container (checked live on 2026-11-08). A 409
`EndpointUnsupportedAccountFeatures` there ("This endpoint does not support BlobStorageEvents or SoftDelete") also
passes (`flatFeatures`): Azure documents that blob soft delete blocks the Data Lake endpoint only on an account
without a hierarchical namespace, while on one with it that endpoint is the account's own API and keeps working
([soft delete overview](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview)). Most
public open-data accounts answer it: 60 of the 64 Planetary Computer containers, and NOAA's GOES, HRRR, GFS and NWM and
NAIP (checked live on 2026-11-11). The host gives that 409 before it checks credentials, so a signed caller whose
credential Get Account Information rejected (class auth) keeps that failure; a container SAS that Get Account
Information only refuses (403 `AuthorizationPermissionMismatch`) is admitted by the 409. One report attributes the
same message to an account reached through a private endpoint for `blob` without one for `dfs`; whether a
hierarchical account can answer it that way is unverified. A 403 `AuthorizationPermissionMismatch` there proves
nothing: a container SAS met it on a flat account and on a hierarchical one alike. A call whose retries were spent
on transient failures (5xx, throttling, timeouts: `objhttp.Requeue`) decides nothing: the round is asked again
after the engine's requeue backoff (`keyrange.RequeueBackoff`), as a listing page is, for as long as the run
lives, so a service that never answers stops as stuck (exit 75), never refused; an endpoint nothing ever
answered at (objhttp's fail-fast) stops as unreachable (exit 75); with `--azure-assume-flat` the listing reaches
the same stop. (3) Anything else (a refusal, another code or a missing header
there too, another failure, no Data Lake host) is unknown. For current blobs, a directory listing can
establish HNS; otherwise the source refuses unless `--azure-assume-flat` asserts a flat namespace.
A reported hierarchical namespace always selects tree listing, even beside that assertion. The probe
requeues transient failures with the admission backoff and preserves cancellation;
`namespace_probe_requests` counts probe attempts and `namespace_probe_requeues` reports outer retries
when nonzero. An assertion still permits probing an unknown namespace; an affirmative HNS answer
always wins. Signed authentication failures on the Blob endpoint retain their class without a Data Lake fallback
or listing probe. A Data Lake authentication failure after a silent Blob header does not preclude
probing the Blob endpoint.
The assertion is the identity's `flat_asserted` field and the report's `namespace` (`account information`,
`filesystem properties`, `filesystem endpoint blocked`, `listing`, or `asserted`, with a warning for the assertion), so a wrong one is visible. A refusal is
`source.Unsupported` (exit 2, class `refused`, cause `unsupported`, the message naming the account and the
reason). The order defenses cannot stand in: on a hierarchical-namespace account `/` sorts lowest, and after a
byte cursor `a.z` the key `a/child` is still in range, but a seek that sorts `/` lowest skips it while every page
still ascends. `--all-versions` requires a flat namespace. Admission runs before the range engine is constructed, so exhausted admission
calls do not reach engine requeue.

## Hierarchical namespace inventory

`Open` selects `OpenedTree` for a current HNS listing. Its directory reader uses the byte-order adapter
for name pages, then `tree.Ordered` for slash-suffixed directory keys and directory-local splits.
Tree requests use XML: the SDK Arrow hierarchy decoder does not expose the directory properties needed
for this mode. Include narrowing uses a directory boundary inside the original scope and retains a
matching directory row by starting from its parent. Directory rows retain their listing properties but leave optional metadata, tags and immutability fields
null. The root directory is omitted. Tree capabilities refuse resume, seeds and range traces; `--max-keys`, sequential
and grouped listings require a flat namespace. See [tree sources](../tree-sources.md) for ownership,
pruning, continuation and bounded lookup evidence. Include narrowing skips subtrees disjoint from the
inferred prefix (`tree.include_prefix_pruned`), separately from recursive-exclude pruning. Candidate
lookups follow empty continuation pages (`tree.lookup_empty_pages`) and retry exhausted transient reads
at the current marker (`tree.lookup_requeues`).

## Order: the byte-order adapter

`internal/source/azblob/order` adapts Azure's UTF-16 code-unit order, which
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
wraps `Expand`: it creates one adapter per expansion, installs it in that call's context, and defers
`Release` on every return, including cancellation and error. `fetch` requires that context adapter; a
missing one is a wrapped `source.ErrProtocol`, tested by `TestFetchRequiresExpandAdapter`, rather than a provider-wide
fallback cache. The adapter holds at most `order.MaxChains` (64) idle chains; one with a continuation in
flight is never evicted. Direct-fetch tests create and release their own adapter. A token the adapter no longer holds
(`order.ErrToken`, which the engine never sends) is neither requeued nor a protocol violation: it fails the run,
its journal resumable. The comparator (`internal/utf16order`) is the one the client's page check and the replay's
Azure route use. The specification, with its worked examples and the facts it rests on, is
`notes/plans/2026-11-08-azblob-order-adapter-spec.md` in the notes repository.

*Cost.* A query with no name at U+E000 and above in its bounds, its prefix or its results costs nothing extra:
one span, the same requests as a plain marker chain, no split. Bounds count as well as names: the engine's
frontier splits synthesize pivots from its whole alphabet, so over names just below U+E000 (lead byte ED) a
bound at U+E000 and above can arrive and is cut like a name (`TestSynthesizedPivotListed`), which costs a seek
and possibly an empty probe. An item equal to a bounded query's `Through` ends its span, so a
range's last page does not read past its end (sound because `Through`'s path is cut at every group boundary:
nothing in its span follows it in Azure's order; an addition to the specification's §4.1). Where such names are, each cut is a seek (`order_spans`,
`order_seeks`; one per span, even an empty one: `order_probes`), each split may add one (an `E` group the read
has not reached), and a span that ends before its response does discards the rest of that response
(`order_overlap_rows`: a read asks for a full page). An adapter invariant broken (only an out-of-order page can
do it) is the client's invalid page, a protocol violation.

*One read per page.* A page that holds an item ends where its span needs another read, short of `Max` if need
be (`order_short_pages`, and the items they lacked, `order_short_rows`); reads that bring it no member (empty
continuing pages, empty spans) do not count. A seek after a listed name, the lower child of every split, returns
that name first and rule 1 drops it, so its read holds one member fewer than the page. Filling the page with a
second read would leave the rest of that read buffered in the chain, and the engine abandons a chain at every
split: its children would read those rows again. With one read per page a chain whose names lie below U+E000
holds nothing between pages (`TestPropertySplitsAndResume`), and every item the service returns is listed once,
a dropped split cursor or past a range's end (`TestAzureSplitsReadNothingTwice`).

**On the range engine.** `Description.Listing` is `keyrange.Listing`: the adapter's pages are byte-ordered,
start after `After` (exclusive; `StartInclusive` is false) and end at `Through`, which is never sent. `AcceptBound` takes valid UTF-8 of at most 1,024 UTF-16 code units, without NUL or another
control character, not ending in `.` (Azure's name rules). `Requeue` is `objhttp.Requeue`; `InvalidPage` is `objhttp.ErrInvalidPage`
([shared failure vocabulary](../clients.md)). Pages are 1,000 entries (`defaultPageSize`; the
service's cap is 5,000, `client.MaxPageSize`) unless the diagnostic `--page-size` (1 to 1,000) sets another size: a range reads to its first non-member, so the rest of its last page is discarded unless a sibling span takes it over, and at 5,000 that was 62% of the rows read on the measured listing ([measurements](../measurements.md#azure-page-size-2026-11-10)). An adapter page maps `Blob` items to rows and `BlobPrefix` items
to `Prefixes`, with the adapter's `Truncated` and token.
With a counted `--seed-from` prior the pages are 5,000 entries, as the prior's tiles hold whole pages
([warm start](../range-engine.md)); `--page-size` still sets another size.

**Every entry** (`--all-versions`: `NewVersions`, rows `Version`, schema `swath/azure-versions/v1`). Every
listing request sends `include=versions,snapshots,deleted` (`client.Include`), so a name is listed once per entry:
its snapshots, its previous versions, its soft-deleted blobs and its current blob. Not `deletedwithversions`:
it adds, to a listing without `versions`, one placeholder (`HasVersionsOnly`) per deleted name that still has
versions; with `versions` those versions are rows themselves. The facts the design rests on:

- *A name's entries are consecutive, keyed by their time within the name.* List Blobs documents names in order
  and snapshots oldest to newest; the SDKs' recorded responses (azure-sdk-for-js, -python, -java session
  records) list snapshots oldest first then the base blob, versions oldest first then the current version, and a
  deleted base blob after its snapshots. Azure's continuation marker decodes to the next entry's name and a
  timestamp, the base blob's `9999-12-31T23:59:59.9999999Z` (decoded live on 2026-11-11). No column is taken as
  an ordinal (`GroupOrdinal` is unset): rows keep the wire order within a name, as on S3.
- *A marker continues inside a name.* It names an entry, not only a name, so a chain's next page resumes at the
  entry after the last one returned; the adapter's chains, like every chain, keep their marker only within one
  range's read.
- *`startFrom` enters a name at its first entry.* It is a name bound; the adapter's exclusive start drops every
  entry equal to it (rule 1 drops by name, over as many pages as the name fills). Checked live for soft-deleted
  entries on 2026-11-11 (NOAA GFS: `startFrom` a deleted name lists it first); not checked live for versions or
  snapshots, for want of a public container with any. `azblobtest.NewEntryStore` models it so.
- *Soft-deleted entries interleave with live ones in name order* (NOAA GFS, 2026-11-11; the recorded page
  `client/testdata/deleted.*`).

The engine runs in group mode (`Description.Groups`, keyrange's `groups.go`): it holds a page's trailing name
until a later name or the chain's end shows it complete, never splits inside a name, and cuts parts and records
the journal only between complete names; a resume restarts after the last complete name, whose entries the
adapter's exclusive start drops. The adapter serves grouped pages unchanged but for two rules: it may emit the
last name again, and a range bounded at a name no longer ends its span at that name's first entry (its later
entries follow it; the span ends at the first item past them, rule 3). Probes send no `metadata`, `tags` or
immutability include, and delimiter queries (probes only) no `snapshots`: a snapshot never holds a name alone, so
no roll-up needs them (List Blobs also refused `delimiter` with `include=snapshots` before service version
2021-06-08). `latest` sets `is_latest` on the base blob that is neither a
snapshot nor soft-deleted and is the current version, or lists no version id (a blob written before versioning
was enabled, or an account without versioning). `TestVersionsListing` and `TestVersionsRestart` list names with
up to five entries at page sizes that put seams inside names, with splits, and resume after names;
`order`'s `TestGroupedProperty` walks random grouped queries. swath-replay refuses `include=` (its conformance
profile); `command`'s `TestAzblobAllVersions` runs `--all-versions` end to end through a proxy that answers
it as Azure answers it over a container of current blobs only: the plain listing (identical bytes, live).

**Schema** `swath/azure-objects/v1`: [the schema binding](../parquet-output.md#schemas) takes its descriptor
from the reader. [dataset.md](../../dataset.md#schema-registry) owns column semantics; the provider uses
`MaxKeyBytes` 3,072, dictionary encoding for `blob_type` and the other short strings, and `DELTA_BYTE_ARRAY`
for `etag`, `metadata` and `tags`. A row's logical size counts a property's integer or boolean only when it
has a value. Its
spill codec uses [the shared payload delta encoding](../ordering.md#payload-delta-codec) in three groups
(eight strings at most each: the first columns, the properties, the tags) and one byte packing the three
optional booleans (`TestDeltaHistoricalBytes` pins the first group's bytes, `TestCodecProperties` the rest).

**Identity** for resume: provider `azure`; `source` (the root without its SAS), `endpoint` (the account's blob
service URL as the client requests it, the account path segment kept on a custom endpoint; scheme and host case
are the only equivalence), `account`, `container`, `flat_asserted` (`--azure-assume-flat`), and `fetch_metadata`, `fetch_tags`, `fetch_immutability` only when
given (a default run's identity is as before they existed). Credentials may change, a SAS among them. A run with
`--max-keys`, or through a supplied client that does not name its route (`azblob.Routed`), cannot resume.
**Counters**: the engine's, then the client's (`attempts`, `retries`, `throttles`,
`timeouts`, `parse_failures`, the admission's), `transport.pool_size`, `retry_after_capped` (a `Retry-After`
past the request timeout, cut to it) and `list_format.arrow_pages`, `.arrow_items`, `.xml_pages`, `.xml_items` (the
valid listing pages each format carried and their blobs and prefixes; an XML page is the service declining Arrow), then the adapter's: `order_spans` (spans beyond each chain's first), `order_splits`,
`order_seeks`, `order_probes` (span reads that found no member), `order_overlap_rows`, `order_empty_pages` (empty
continuing pages followed inside a span's read), `order_peak_stack`, `order_peak_buffer_rows`, `order_responses`,
`order_chains_evicted`, `order_peak_chains`, `order_short_pages` and `order_short_rows` (one read per page). **Connections**: the [shared pool](../clients.md#pool-construction), with provider-owned rotation of the
service and HTTP-client slices (`TestServicePool`, `TestServicePoolRoundRobin`). The client's anonymous note goes to the run's warnings.

**Overload signals** (`client.Error.Failure`):

| Answer | Shared limiter input |
|---|---|
| `ServerBusy`, or 429 without a recognized overriding code | Throttle |
| `OperationTimedOut`, or 408 without a recognized overriding code | Timeout; holds growth, does not cut |
| 500 `InternalError`, or another retryable status (502, 503, 504) without a throttle/timeout code | Transient; no throttle signal |

The code takes precedence over the status. A generic 500 or 501 is terminal; 500 is retried only for
`OperationTimedOut` or `InternalError`. [Request admission](../clients.md#request-admission) owns the shared
controller and the limits of its tuning evidence. Namespace admission above remains provider-owned.

## The live source

`OpenLive` builds the client as `New` does and sends nothing: no namespace admission, no range engine. `Live` is the
`live.Store` of the library's `github.com/varveio/swath/source/azblob` ([library](../../library.md#azure-blob-storage)); its
location is the account, the container and the canonical route.

**Pages.** `List` is one List Blobs with the delimiter `/` and `include=metadata`, its names held to no order
(`ListInput.AnyOrder`: a hierarchical namespace lists in its own order, `/` below every other character), and it
serves the level in byte order. A blob whose metadata says `hdi_isfolder=true` (`client.Directory`), as a
hierarchical namespace's directory's blob does, and whose name does not end in `/`, is the directory's prefix in a
hierarchical namespace only, so a directory listed both as that blob and as its prefix in one answer is one prefix;
any other key listed twice is served twice, which `source.Source`'s page check refuses (`listing.ErrInvalidPage`).
User metadata can mark a flat namespace's blob the same way, so the first answer or Get Blob Properties holding a
marked blob makes `Live` ask the namespace with admission's calls (`namespaceOf`: Get Account Information, then the
Data Lake host), once per source. A flat namespace's marked blob is an object. Credentials refused or not had and a
failing or unreachable service keep their class; a namespace those calls cannot tell is `listing.ErrUnsupported` for
a page and unknown for a lookup. The first page of a level asks for 5,000 entries (`client.MaxPageSize`). When that
answer holds the whole level, the level is sorted, unless it is in byte order already and holds no directory's blob; a sorted level's later pages read it whole again
(the cursor `s`, its last key), and one that no longer fits one answer is `listing.ErrUnsupported`. Otherwise the level
is paged as a chain in Azure's order (the cursor `c`: its `startFrom`, next marker and last key): from the marker once
an answer is served whole, else from a new chain whose `startFrom` is the last key served, whose answer lists that key
again and drops it. Each answer must ascend in byte order after the last key; one that does not, or that holds a
directory's blob, is `listing.ErrUnsupported`. The pages served before it stand, and may have missed that key: the
refusal says the level cannot be had in byte order, not where it went wrong. A chain is sound while every answer
ascends because the merge of an answer's blobs and prefixes (UTF-16) is then Azure's own sequence, so a new chain at
the last key misses nothing listed after it. In a hierarchical namespace that merge is not the namespace's sequence
(`a/` comes before `a-b` there), so a new chain could skip a key while every answer still ascends: a directory's blob,
which such a level lists beside every directory, ends the chain.
`TestPageOrderAtLimitOne`, `TestUnorderedLevel` and `TestHierarchicalDirectories` (package `azblob`) cover the three
paths over `azblobtest.Handler`, the `azblobtest.Store` served over HTTP; `TestFlatDirectoryMetadata` the flat and
unknown namespaces, and `TestInvalidPages` a key repeated in an answer and across answers.

**Lookups.** `Lookup` is Get Blob Properties of the key: present with its size, last-modified time and access tier, or,
for a hierarchical namespace's directory's blob, as the directory's prefix. A 404 whose code is `BlobNotFound` (or
absent) is absent only once the probe, a List Blobs of the key's prefix for one entry, succeeds. Any other failure of
either request leaves the key unknown with its class (`objhttp.Classed`, Azure's code before its status): a 403 is
`listing.ErrAuth`, `ContainerNotFound` `listing.ErrNotFound`, and an authentication code that comes with a 404
`listing.ErrAuth`. `TestLookupUnknown` covers each.

**Walks.** `Live.Walk` (`walk.go`) builds a provider of its own over the live client (`New` with it as
`Options.Client`, 64 workers unless the walk sets them, `Sequential` at one), so each walk runs admission: a
hierarchical namespace, or one admission cannot establish as flat, is `listing.ErrUnsupported` before any List
Blobs (`TestWalkHierarchicalRefused`). The byte-order adapter serves each range ascending, so `live.Count` checks
each range's keys ascending and apart from the others' (`TestWalkEachEntryOnce`, over `azblobtest.Keys`).
