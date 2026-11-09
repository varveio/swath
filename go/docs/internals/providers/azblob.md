# Azure Blob (`internal/source/azblob`)

**Client** (`internal/source/azblob/client`): List Blobs and Get Account Information, described with the list
clients ([clients.md](../clients.md#the-list-clients-hand-rolled-on-the-sdks-auth)). The root is `az://ACCOUNT/CONTAINER[/PREFIX]`;
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

**Schema** `swath/azblob-objects/v1`, its columns and their caveats as [upgrade.md](../../upgrade.md#the-downstream-reader-dispatch-on-schema-id)
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
