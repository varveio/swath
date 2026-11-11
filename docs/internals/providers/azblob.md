# Azure Blob (`internal/source/azblob`)

## Support and differences

`az://account/container[/prefix]` lists current blobs: a flat namespace on the range engine, a
hierarchical namespace (HNS) on the tree core ([tree mode](#hierarchical-namespaces-the-tree-mode)).
With `--all-versions` a flat namespace lists every entry (versions, snapshots, soft-deleted blobs;
`NewVersions`, below); a hierarchical one is refused, and its tree listings send no `include`, so the
`--azure-fetch-*` flags fill nothing there. Namespace admission is required even for a public
container; `--azure-assume-flat` only overrides an unknown result, never an account reported as
hierarchical.
[Shared object-store controls](../../sources.md#object-store-requests) cover authentication,
request timeout, retries and admission.

Azure applies anchored `--include` filters to rows without narrowing the listing prefix.
There is no tariff estimate, including on Azure's default endpoint. Only this provider honors
`Retry-After` (capped at `--request-timeout`, 30 s by default, counted by `retry_after_capped`).
It applies the shared [endpoint validation](../clients.md#endpoint-validation) to `--endpoint-url`; supply SAS
credentials separately. `--anonymous` with
`--azure-sas-token` or a SAS in the root is refused (exit 2), as every provider refuses it beside an explicit
credential; Azure credentials in the environment (`AZURE_STORAGE_SAS_TOKEN`, `AZURE_STORAGE_KEY`, the Entra ID
variables) are ignored with the shared one-line warning naming them. `--storage-class` filters on the access
tier (`access_tier`, the schema's declared storage-class column).

Root parse errors keep clean roots, redact SAS queries and hide credential-bearing authorities.

## Provider implementation

**Client** (`internal/source/azblob/client`): List Blobs and Get Account Information, described with the list
clients ([clients.md](../clients.md)). Both are sent through the Azure
SDK's service and container clients (`sdk/storage/azblob` v1.8.3), with the SDK's retries off: swath's retry loop
(`objhttp.Do`) repeats an attempt, `objhttp.Guard` bounds every body and checks a listing's envelope before the SDK
decodes it, in the format its `Content-Type` names. A per-call policy pins `x-ms-version` to `2026-06-06`, the oldest version with the Arrow format, which the live service, swath-replay and Azurite all answer. Every
listing asks for the Arrow response format, then XML (`Accept: application/vnd.apache.arrow.stream,application/xml`),
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
`AZURE_SDK_GO_LOGGING=all` prints nothing. The root is redacted where the command parses
it (`registry.Factory.Redact`, `registry.Redact`), before the `-v` start line, the recorded command and the
report's `invocation.source` see it, so an admission refusal records no SAS either; the identity, the manifest
and the errors carry the root without its query. A scope prefix holding a byte in EE..F4 is accepted and listed
through the adapter like any other.

**Admission.** Before any listing request the namespace must be known: flat lists on the range engine,
hierarchical on the tree core (`open`; `New`, the flat provider's constructor, still refuses it). (1) Get
Account Information on the container path: `x-ms-is-hns-enabled` `false` is flat, `true` hierarchical. (2)
When that call is refused (401,
which Azure answers any anonymous caller, or 403) or answers without the header, Filesystem Get Properties on the
account's Data Lake host (`client.NamespaceInfo`: `HEAD https://ACCOUNT.dfs.core.windows.net/CONTAINER?resource=filesystem`,
the blob host with its `blob` label replaced, so only for the exact default endpoint, `https://ACCOUNT.blob.core.windows.net`
with the configured account, no port and no path; with any other `--endpoint-url` there is no such host and
this step is skipped, as it is with Shared Key, which this request is not signed with): `x-ms-namespace-enabled` `false`
is flat, `true` hierarchical. Azure answers it anonymously for a public container (checked live on 2026-11-08). A 409
`EndpointUnsupportedAccountFeatures` there ("This endpoint does not support BlobStorageEvents or SoftDelete") is
flat too (`flatFeatures`): Azure documents that blob soft delete blocks the Data Lake endpoint only on an account
without a hierarchical namespace, while on one with it that endpoint is the account's own API and keeps working
([soft delete overview](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview)). Most
public open-data accounts answer it (checked live on 2026-11-11). The host gives that 409 before it checks
credentials, so a signed caller whose credential Get Account Information rejected (class auth) keeps that failure;
a container SAS that Get Account Information only refuses (403 `AuthorizationPermissionMismatch`) is admitted by
the 409. One report attributes the same message to an account reached through a private endpoint for `blob`
without one for `dfs`; whether a hierarchical account can answer it that way is unverified.
(3) When both calls were refused (a container SAS gets 403 from both: `bingmlbuildings`, 2026-11-11) and no
flat namespace is asserted, one delimiter listing of the scope's directory (`maxresults=1`,
`ListInput.Tree`, `listedHNS`): an entry whose `ResourceType` is `file` or `directory` is a hierarchical
namespace's (a flat account's XML entries carry none; Arrow's `blob` and `blobprefix` do not count). (4)
Anything else (a refusal or a missing header there too, another failure, no Data Lake host) is unknown and refuses,
unless `--azure-assume-flat` asserts a flat namespace (an asserted run sends no step-3 request); a reported
hierarchical namespace is listed as one whatever is asserted.
The assertion is the identity's `flat_asserted` field and the report's `namespace` (`account information`,
`filesystem properties`, `filesystem endpoint blocked`, `listing` or `asserted`, with a warning), so a wrong one is visible. A refusal is
`source.Unsupported` (exit 2, class `refused`, cause `unsupported`, the message naming the account and the
reason). The order defenses cannot stand in: on a hierarchical-namespace account `/` sorts lowest, and after a
byte cursor `a.z` the key `a/child` is still in range, but a seek that sorts `/` lowest skips it while every page
still ascends. `--all-versions` is admitted as any listing. Admission runs before the range engine is constructed, so exhausted admission
calls do not reach engine requeue.

## Hierarchical namespaces: the tree mode

Spike (issue #263; design in [tree-sources.md](../tree-sources.md)). On an HNS account List Blobs lists in
directory-tree order (`/` lowest: all of `Guinea/…` before `Guinea-Bissau`), so no byte-order seek covers the
container. A flat run forced on one may fail its order check (it did with `--azure-assume-flat` on
`bingmlbuildings`, 2026-11-11) but need not: a seek can skip keys while every page still ascends (see
Admission), so the check is no guard against a wrong assertion. Within one directory, names hold no `/` and the service's order is UTF-16 by name, and
`startFrom` seeks by name (live, `overturemapswestus2`). `Tree` (`tree.go`) is the tree core's ordered mode
(`tree.Ordered`) over that:

- **Listing.** A directory is one delimiter listing (`prefix=DIR/`, `delimiter=/`) with `ListInput.Tree`:
  the client merges blobs and `BlobPrefix` items by name and makes each prefix a directory entry
  (`Item.Dir`) with the properties HNS returns on it at version 2026-06-06 (`ResourceType` directory, Etag,
  Last-Modified, Creation-Time, BlockBlob, the tier). `include=metadata` (`hdi_isfolder`) is not needed. The
  byte-order adapter serves the names in byte order, one per `Expand` as for flat accounts; `Lookup` (a
  candidate directory) is one `maxresults=1` listing of `DIR/NAME`.
- **Why Blob, not Data Lake `Path - List`.** List Blobs seeks by name (`startFrom`), so a large directory
  splits on the range engine; `Path - List` continues by token only. Both answer anonymously and with a
  container SAS (the spike's oracle walks with `Path - List`), but List Blobs returns the flat schema's
  columns (content type, MD5, tier) and reuses the client, its retries, classification, SAS scrubbing and
  the adapter. `Path - List` returns POSIX owner, group and permissions, which the schema
  has no columns for (open).
- **Rows.** A directory is a row keyed with its name plus `/` in `swath/azblob-objects/v1` (size 0, its Etag,
  times, BlockBlob and tier as listed), followed by its subtree; a file is its blob name. No schema change:
  in a tree-mode dataset a key ending in `/` is a directory (an HNS path cannot end in `/`); in a flat
  account's dataset it may be a blob (a folder marker), so the report's `listing: tree` tells them apart.
  Directory rows count as objects in `du`. Flat-account output is unchanged.
- **Large directories** go to one shared range engine as segments (`keyrange.Engine.Within`), seed and
  structure probes off; the engine's `AcceptBound`, requeue and counters are the flat provider's.
- **Recursive excludes.** The registered `treeOpened.Run` installs the filter package's descendant
  proof in `Walker.SetPruneDirectories` before workers start. Both whole directories and range-engine
  segments keep the directory row for ordinary row filtering but submit no child for a matching
  recursive exclude. This avoids requests below that directory; its parent is still listed and returns
  its metadata. `source.stats.pruning` records the rule classification (as for
  [local trees](local.md)); `source.stats.pruned_dirs` counts prevented child traversals, absent when
  zero. Includes, metadata filters and directory-row rejection alone never prevent descent.
- **Scope and options.** The prefix must name a directory (end in `/`); `--sequential` is refused; no
  resume yet. Identity: the flat fields plus `listing` `tree` (absent for flat accounts, so neither journal
  resumes as the other). Report: `listing` `tree`, the walk's `files`, `dirs`, `bytes` in the brief, the
  engine's, client's, adapter's and the tree core's counters (`tree.*`).
- **Cost.** At least one request per directory: a Hive layout with one file per partition directory costs
  about one request per file, where a flat container costs one per 1,000.
- **Tests.** `TestTreeListsHNS` (`azblobtest.TreeStore`: tree order, directory entries, UTF-16 siblings,
  non-ASCII Hive names, a 3,000-file directory with a directory candidate; 1/4/16 workers, pages 1 to
  1,000), `TestTreeScopedToDirectory`, `TestTreeCancelMidDirectory`, `TestTreeIdentityAndReport`,
  `TestTreeListedNamespace`, `TestTreeNotNewFlat`, `client` `TestTreeListing`; admission's HNS rows in
  `TestAdmissionFallback`. The replay server's `--azure-hns` still changes only Get Account Information.

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

**Every entry** (`--all-versions`: `NewVersions`, rows `Version`, schema `swath/azblob-versions/v1`). Every
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
profile); `cmd/swath`'s `TestAzblobAllVersions` runs `--all-versions` end to end through a proxy that answers
it as Azure answers it over a container of current blobs only: the plain listing (identical bytes, live).

**Schema** `swath/azblob-objects/v1`: [the schema binding](../parquet-output.md#schemas) takes its descriptor
from the reader. [dataset.md](../../dataset.md#schema-registry) owns column semantics; the provider uses
`MaxKeyBytes` 3,072, dictionary encoding for `blob_type` and the other short strings, and `DELTA_BYTE_ARRAY`
for `etag`, `metadata` and `tags`. The sixteen columns after `creation_time` were added to the id later
(`MayBeAbsent`), as dataset.md's evolution policy allows. A row's logical size counts an added integer or
boolean only when it has a value, so a listing whose blobs carry none of them cuts its parts as before. Its
spill codec uses [the shared payload delta encoding](../ordering.md#payload-delta-codec) in three groups
(eight strings at most each: the first columns, the properties, the tags) and one byte packing the three
optional booleans (`TestDeltaHistoricalBytes` pins the first group's bytes, `TestCodecProperties` the rest).

**Identity** for resume: provider `azblob`; `source` (the root without its SAS), `endpoint` (the account's blob
service URL as the client requests it, the account path segment kept on a custom endpoint; scheme and host case
are the only equivalence), `account`, `container`, `flat_asserted` (`--azure-assume-flat`), `fetch_metadata`, `fetch_tags`, `fetch_immutability` only when
given (a default run's identity is as before they existed), and for a hierarchical namespace `listing` `tree` (absent for a flat account). Credentials may change, a SAS among them. A run with
`--max-keys`, or through a supplied client that does not name its route (`azblob.Routed`), cannot resume.
**Counters**: the engine's, then the client's (`attempts`, `retries`, `throttles`,
`timeouts`, `parse_failures`, the admission's), `transport.pool_size`, `retry_after_capped` (a `Retry-After`
past the request timeout, cut to it) and `list_format.arrow_pages`, `.arrow_items`, `.xml_pages`, `.xml_items` (the
valid listing pages each format carried and their blobs and prefixes; an XML page is the service declining Arrow), then the adapter's: `order_spans` (spans beyond each chain's first), `order_splits`,
`order_seeks`, `order_probes` (span reads that found no member), `order_overlap_rows`, `order_empty_pages` (empty
continuing pages followed inside a span's read), `order_stack_max`, `order_buffer_rows_max`, `order_responses`,
`order_chains_evicted`, `order_chains_max`, `order_short_pages` and `order_short_rows` (one read per page). **Connections**: the [shared pool](../clients.md#pool-construction), with provider-owned rotation of the
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
