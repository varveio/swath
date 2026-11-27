# Tree sources (`internal/source/tree`)

The tree core shares ordered child splicing between local filesystems, Azure hierarchical namespaces
(HNS), S3's folder walk and Vercel Blob. Azure uses `Walker` and its seekable-directory reader, `Ordered`.
S3's `--listing tree` and Vercel use `Walker` and the token-only reader, `Delimited`. localfs uses
`EmitRows` with its own ranges, retaining descriptor ownership, unread-entry reporting, platform readers
and count paths.

## Ordering and ownership

A reader must supply rows and child subtrees in unsigned byte order. A file is keyed `prefix + name`, a
directory `prefix + name + "/"`. The directory row followed by its subtree forms one contiguous interval:
`a-b < a.b < a/ < a/x < a0`. This requires each subtree's keys to extend its raw byte prefix. The core does
not establish that property for a provider whose routing uses normalized or otherwise transformed names.
Azure validates directory names before creating children, and the ordered output stage checks increasing
keys. Those checks detect malformed output, but cannot prove that a provider omitted nothing.

`Reader.Read` receives a `Range` describing a directory or one of its segments, and writes to `Out`:

- `Rows` emits rows and splices child work at its ordered position.
- `Seg` emits a continuation or chunk of the same directory at its ordered position.
- `Leaf` emits one row, regardless of a trailing slash in its key.
- `Descend` emits a directory row followed by its child; `NoDirs` suppresses the row only.
- `Prune` emits the directory row without walking it; `Omit` emits neither.
- `Folder` descends into a virtual folder: a child with no row of its own (an object store's common
  prefix). Its key extends the range's by one segment without `/`, possibly empty (`a//` below `a/`), and a
  `/`; anything else is a protocol violation and nothing is emitted. No path cleaning applies.

Rows transfer ownership to the emitter. A child transfers ownership even when `Child` returns an error.
`EmitRows` constructs a child only after the preceding rows were accepted: localfs retains a descriptor
at that point, so rejection of the rows cannot leak the child's reference. localfs keeps its own disposal
and unread callbacks; the generic walker disposes queued resource-bearing segments through its reader.

Children become available for work when an expansion returns. A cursor reader should emit one page and
its continuation segment, then return, rather than drain the directory before publishing child work.
This bounds that reader's page buffer; it does not itself bound pending work or ordered-output buffering.
The order stage's own admission and memory policies still apply. `Out` belongs to one goroutine.
A reader owns a segment once `Read` is entered and releases it on every return; the walker disposes it
only when it never enters `Read`. Directory handles in `Dir` must not own resources.

## Recursive exclusions and markers

The walker's recursive-exclude predicate proves that every descendant is excluded. It can suppress a
`Descend` child while the directory row still goes through ordinary filtering. Reader-owned `Prune` and
`Omit` take precedence. A virtual folder (`Folder`) keeps traversal apart from emitted rows: it can contain
a real marker object whose key equals its prefix, and excluding descendants does not exclude that marker,
so the predicate never applies to it and a virtual folder is always walked. A provider would have to prove
exclusion of both before omitting such a branch; `Delimited` does not try, and S3's folder walk installs
no predicate (ordinary filters still apply to the rows). A real marker returned inside its own prefix is a
`Leaf`, emitted once. `Delimited` rejects self-referential folder responses.

## Azure ordered directories

`Ordered` adapts a seekable directory listing in byte order of names into byte order of keys. Azure's
UTF-16 name order is adapted by its existing byte-order adapter first. A directory name `part` becomes
`part/`, so it must be held behind names such as `part-001`. Held directory entries are emitted when their
key belongs next.

A directory fitting in its first page is emitted whole. A larger directory emits its first page and a
remaining interval through `keyrange.Engine.Within`. That interval is local to the directory, not a
namespace-wide recovery fence. Splits can start between a directory's name and its slash-suffixed key;
`Lookup` resolves candidate directories behind the name seek that still belong ahead of the key cursor.
The cache keeps up to eight directory records, each with up to 1,024 entries of evidence. Active
readers fall back to lookups when their record is evicted.

The reader retains held rows and continuation state while retrying a later name page. Restarting the whole
logical page would replay already-consumed adapter tokens. First-page and resolution requeues use the
provider's requeue classification and cancellable backoff. The tree counters report first pages, whole
and split directories, held directories, candidate lookups, retries, and evidence resets and evictions;
range-engine counters describe segment work. Azure reports requests for the whole walk and shares HTTP
activity with the watchdog.

Neither the generic walker, Azure HNS nor the token-only folder walks declares durable coverage fences
or resume support. Tree resume and unordered-directory spill are not implemented here.

## Token-only delimited folders

`Delimited` reads a store that lists one folder at a time with delimiter `/` and continuation tokens only, and
whose pages can arrive in any order. A provider supplies a `FolderLister`, which takes the folder prefix
and cursor. It returns the direct objects as rows, the child prefixes as spelled by the service, and the
next cursor. The first call for a folder uses an empty cursor. S3 uses `ListObjectsV2` with delimiter `/`, never `StartAfter`,
and maps objects with the range listing's `rows`. Vercel uses folded mode and maps `pathname` to the row key
and `uploadedAt` to `created_at`, the current blob's reported upload time. The same validation and sorting serve Vercel and Cloudflare R2.
See [Vercel Blob](../sources/vercel.md) and [S3](../sources/s3.md#cloudflare-r2).
GCS (`delimiter` and `pageToken`) and flat Azure (`delimiter` and `marker`) adapters are not wired.

A range is a folder (`FolderRange`). `Read` lists it whole, follows tokens only, and checks every page before
anything is emitted; a violation stops the run (`protocol`) rather than drop a row:

- every object key and child prefix starts with the folder's raw prefix bytes: a store that matched the
  prefix in another form (R2 matches NFC) returns keys that do not;
- an object key is not empty and holds no `/` after the prefix: with delimiter `/` every key that does is
  rolled into a child prefix, and the folder's own marker (its key the prefix) has an empty remainder;
- a child prefix is the folder's prefix, one segment without `/` (possibly empty) and one `/`, and is not the
  folder itself;
- no entry is listed twice in a folder;
- a truncated page carries a token the folder has not returned before; a terminal page needs none.

The folder's entries are then sorted by raw bytes, a child by its prefix (which ends in `/`, so it sorts where
its subtree belongs), and handed on: runs of objects as `Leaf` rows, each child as a `Folder`, listed by its
prefix exactly as returned, never normalized. That is the localfs argument: no sibling sorts between a
folder's key and the keys that extend it, so emitting each folder's sorted entries with its children spliced
in place is global byte order, whatever order the service sent. It relies on what the service's own
definition of a complete listing gives (continuation tokens and the delimiter's grouping), not on its order
or a seek. The checks cannot see what a service never sends: a common prefix whose hidden members belong
elsewhere is caught only when they arrive, under the folder that does not hold them.

**The scope.** `Select` is a raw-byte prefix. The walk starts at its deepest folder (`Select` up to its last
`/`) and keeps from that folder, after validation, only the objects and child prefixes whose keys start with
`Select` (counted as `tree.out_of_scope` otherwise); below a kept child every key does. Validation, the
duplicate check included, covers every returned entry before the scope is applied. Leaving a child prefix out
is not safe by its spelling alone on a store that groups names in Unicode NFC: R2 merges `e\u0301/` and
`\u00e9/` into one common prefix spelled as one of them, so `\u00e9/` can hold `e\u0301/x`, a key of the scope
`e`. So a child `F` outside the scope is probed (`tree.probes`) only when the NFD form of `F`'s segment begins
with the stem of the scope's remainder `r` (`Select` after the start folder's prefix): `NFD(r)` up to and
including its last starter (canonical combining class 0), or "" when `r` has none. Any other `F` is left out
without a request. A key `m` of the scope placed under `F` has a segment `s` that starts with `r` and is
canonically equivalent to `F`'s (`NFC(s) == NFC(seg F)`, so `NFD(s) == NFD(seg F)`); `NFD(s)` is the canonical
reordering of `NFD(r) + NFD(rest)`, and reordering only permutes runs of non-starters, so the stem is a prefix
of `NFD(s)`, hence of `NFD(seg F)`. So scope `a` never lists `\u00e9/` (`e\u0301`), scope `e` does, scope
`\u00e9` lists `\u1eb9\u0301/` (NFD e, dot below, acute: where `\u00e9\u0323` belongs), scope `K` lists
`\u212a/` (NFD `K`), and a remainder of combining marks alone lists every folder outside it. A probe lists the
subtree folder by folder, checked as any folder is, emitting nothing, and holds no more than each folder's
listing and its children's prefixes. A key of the scope hidden in it reaches, at some depth, a listing whose
raw prefix it does not start with, and stops the run (`protocol`); listing only `F`'s own folder would not do,
since a deeper common prefix can be spelled as the other spelling's key. The argument needs two things this
reader has: every folder prefix ends in `/`, and NFC and NFD are segmentable at `/` (no canonical
decomposition holds `/`, and `/` composes and reorders with nothing). With them a scope that does not end in
`/` (`logs`, covering `logs/` and `logs-old/`) and an `--include` narrowing are exact in raw bytes, or the run
stops. A probe costs a request per folder of the probed subtree.

**Memory.** A folder is held whole while it is read. Each page is charged what its strings retain
(`FolderPage.Retained`: S3's decoded page keeps one arena at its URL-encoded size, so a key of decomposed
characters costs three times its length) or, where the provider says nothing, their lengths and its
payload's (`Bytes`); twice each entry's staging slot (the slot and the row it becomes); and each token in the
history its length and a map slot (the history keeps a copy, not the page's arena). One folder may stage
`FolderBytes` (64 MiB by default) and every folder being read at once `StagingBytes` (256 MiB). A page that
would pass either stops the run with `FolderLimitError` (cause `folder_limit`), naming the folder; staging
never waits, and is released on every return, cancellation included. Once a folder is handed on its rows are
the order stage's, under its own budget. A folder is not spilled: one larger than the bound cannot be listed
this way.

A page in flight is a second, separate bound: from its request until it is decoded and charged it reserves
`PageHeadroom` from `HeadroomBytes`, waiting while other pages hold the rest (`tree.headroom_waits`,
`tree.headroom_wait_ns`). S3 reserves 4 KiB an entry asked for (a URL-encoded 1,024-byte key and its
metadata), or 4,000 KiB for a page of 1,000. Vercel reserves 64 MiB per response within the shared 256 MiB
headroom budget, allowing four concurrent page requests. The response body is capped at 16 MiB.
The reservation is an admission allowance, not a measured heap or RSS bound: decoder buffers,
temporary strings and garbage collection can add memory. S3's default `HeadroomBytes` is one page for
each page the walk can have in flight (`InFlight`: the order stage's workers and its drainer, which lists a queued folder itself, each reading
one folder a page at a time), so the bound states the memory rather than limiting the walk: at `--workers 64`,
254 MiB; at 256, 1,004 MiB. The worst case of a walk is then the staging bound plus that: 510 MiB at 64
workers, 1.23 GiB at 256, beside the order stage's own budgets. The range engine holds the same response memory, a body of up to 16 MiB for each request in flight
(`--workers`, plus seed listings and probes beside them), under no bound. It cannot deadlock: the headroom is
the only thing anyone waits for; a holder takes one reservation at a time and releases it once its page is
charged or has failed, waiting on nothing but its own request; and staging, which a waiter may hold, is never
waited for. One reservation never exceeds the bound, and a cancelled wait returns.

**Coverage.** A walk's report claims its scope only when every folder it handed out was read whole (the start
folder and each child spliced in); a walk that stopped says it is not complete.

**Progress and counters.** `Frontier` counts pages that held an entry or ended a folder, and empty continuing
pages within `keyrange.EmptyChainLive` of each other, for the watchdog. A page whose failure the provider's
`Requeue` accepts is asked again with the same token after `keyrange.RequeueBackoff`. `DelimitedCounters` are
the reader's counters (folders started and completed, pages, empty pages, requeues, objects, prefixes, markers,
empty segments, non-NFC names, out-of-scope entries, the largest folder's entries and bytes, peak staging,
the bounds, budget refusals, sort time and each kind of violation), placed in the provider's table
([Counters](counters.md#the-s3-folder-walk---listing-tree)).

**Resume, not yet.** No delimited listing declares resume. The intended shape, once a published key `K` is
journaled: skip the objects at or below `K`, and the folders whose whole subtree sorts at or below `K`,
without listing them; re-list only the folders on `K`'s path (each of `K`'s ancestors), keeping their entries
above `K`. Since a folder is emitted whole and in order, a sorted folder is the unit a resume re-reads; nothing
here keeps state that would block it.

## Other directory APIs

A token-only ordered API can reuse the walker and continuation segments without `Ordered` or
`keyrange.Engine`, or `Delimited` and its `Folder` operation, at the cost of sorting what is already in order.
A reader using native ordering would need a byte-order guarantee across pages, including the merge of direct
objects and virtual folders. Vercel's folded pages use opaque cursors and swath sorts each folder in full.

Cloudflare R2's observed listing order differs from raw key order even within a directory, and normalization
can change routing and subtree placement, so its pages cannot feed `Ordered`. `Delimited` sorts each whole
folder instead (sorting each page is insufficient), and validates placement by raw bytes. A global external
sort, a spilled folder, key-range parallelism within one folder and a model of R2's comparator are not
implemented: the measured comparator is not a service contract, and checking page order alone cannot prove
that a seek or range bound omitted nothing.
