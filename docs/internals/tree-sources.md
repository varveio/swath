# Tree sources (`internal/source/tree`)

The tree core shares ordered child splicing between local filesystems and Azure hierarchical namespaces
(HNS). Azure uses `Walker` and its seekable-directory reader, `Ordered`. localfs uses `EmitRows` with its
own ranges, retaining descriptor ownership, unread-entry reporting, platform readers and count paths.

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
`Omit` take precedence. A future virtual-folder API must distinguish traversal from emitted rows: a virtual folder can contain
a real marker object whose key equals its prefix, and excluding descendants does not exclude that marker.
A provider must prove exclusion of both before omitting such a branch. A real marker returned inside its
own prefix is a `Leaf`, emitted once. Such an API must also reject self-referential folder responses.

## Azure ordered directories

`Ordered` adapts a seekable directory listing in byte order of names into byte order of keys. Azure's
UTF-16 name order is adapted by its existing byte-order adapter first. A directory name `part` becomes
`part/`, so it must be held behind names such as `part-001`. Held directory entries are emitted when their
key belongs next.

A directory fitting in its first page is emitted whole. A larger directory emits its first page and a
remaining interval through `keyrange.Engine.Within`. That interval is local to the directory, not a
namespace-wide recovery fence. Splits can start between a directory's name and its slash-suffixed key;
`Lookup` resolves candidate directories behind the name seek that still belong ahead of the key cursor.
Bounded per-directory evidence saves repeated lookups. Dropping evidence changes request cost, not output.

The reader retains held rows and continuation state while retrying a later name page. Restarting the whole
logical page would replay already-consumed adapter tokens. First-page and resolution requeues use the
provider's requeue classification and cancellable backoff. The tree counters report first pages, whole
and split directories, held directories, candidate lookups, retries, and evidence resets and evictions;
range-engine counters describe segment work. Azure reports requests for the whole walk and shares HTTP
activity with the watchdog.

Neither the generic walker nor Azure HNS declares durable coverage fences or resume support. Tree resume,
unordered-directory spill, Vercel Blob and Cloudflare R2 providers are not implemented here.

## Other directory APIs

A token-only ordered API can reuse the walker and continuation segments without `Ordered` or
`keyrange.Engine`. A virtual-folder operation that emits no row can be added when such a provider is built.
The Vercel investigation observed ordered folded pages with separate blobs and virtual folders; an adapter
would have to merge them, preserve real markers and validate order across pages. One large flat directory
would remain a serial cursor chain; tiny folders can amplify requests. Those service contracts and a full
recursive walker need independent validation before a provider is added.

Cloudflare R2's observed listing order differs from raw key order even within a directory, and normalization
can change routing and subtree placement. It cannot feed such pages directly into this ordered splicer.
A future R2 adapter needs a separately validated sorting stage or order conversion before using ordered
emission. Sorting each page is insufficient. The order stage's spill stores already-ordered work; it is not
a global external sorter. The measured R2 comparator is not a service contract, and checking page order
alone cannot prove that a seek or range bound omitted nothing.
