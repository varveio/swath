# Tree sources (`internal/source/tree`)

Status: spike design (issue #263). The walker, the ordered mode and Azure HNS listing are built; resume, the
unordered-mode spill and the S3 directory-bucket and Drive readers are designed here only.

A *tree source* is a store with no single seekable key order across the whole namespace: a local
filesystem, an Azure Blob account with a hierarchical namespace (HNS), an S3 directory bucket, Google Drive.
Each lists one directory at a time. The tree core turns directory listings into one stream in unsigned byte
order of keys, the order every swath dataset has, and hands it to the order stage like any other provider.

## The order

A directory is the unit of work. Its entries' keys are `prefix + name` for a leaf and `prefix + name + "/"`
for a directory, where `prefix` is the directory's own key (`""` at the root, `a/b/` below it). Names hold no
`/`. Sorting a directory's entries by key in byte order and emitting a directory's row, then its subtree,
before the next sibling is global byte order: every key of the subtree starts with the directory's key, and
no sibling sorts between the directory's key and the keys that extend it. So `a-b` < `a.b` < `a/` < `a/…` <
`a0`.

The store's own order is not this order. A filesystem has none. Azure HNS lists a directory by *name*
(UTF-16, tree order: `/` lowest), so `Guinea/` comes before `Guinea-Bissau`, while the keys sort the other
way (`Guinea-Bissau` < `Guinea/`). The core owns the conversion.

## Package layout and seam

```
internal/source/tree        the walker (source.Provider over Range), the order helpers,
                            the ordered mode (a Reader over keyrange)
internal/source/localfs     a Reader: getdents/portable engines, chunks, count slabs
internal/source/azblob      flat accounts on keyrange (unchanged); HNS accounts on tree.Ordered
```

```go
// Range is the tree's unit of work: a directory, or a segment of one.
type Range[D, S any] struct {
	Dir    D      // the directory as its reader names it (a path; nothing for Azure)
	Prefix string // its key prefix
	Seg    S      // a segment of the directory (a chunk, a keyrange interval)
	InSeg  bool
}

// Reader is the provider half of a tree source.
type Reader[D, S, P any] interface {
	Read(ctx context.Context, r Range[D, S], out *Out[D, S, P]) error
}

// Out is where a Reader puts a range's content, in key order.
func (o *Out[D, S, P]) Rows(rows []schema.Row[P], walk func(*schema.Row[P]) (D, Walk)) error
func (o *Out[D, S, P]) Seg(s S) error

// Walk is what the walker does after a row: Leaf, Descend (its subtree follows), Prune (listed, not
// walked: another device), Omit (neither: the run's own output, a file or a directory with its subtree).
```

`Walker[D, S, P]` implements `source.Provider[Range[D, S], P]` and `source.Disposer`. `Expand` checks the
context and calls `Read`. `Out.Rows` splices: it hands rows to the order stage's emitter up to each row whose
walk is `Descend`, then emits that directory as a child range (prefix = the row's key, cloned: keys may be
views of a reader's arena), and continues. `Out.Seg` emits a segment of the same directory at its position.
The walker counts what it emitted (`files`, `dirs`, `bytes`, `pruned`, `excluded`, `omitted`, `segments`) and applies
`Options.NoDirs` (directories walked, their rows dropped). The reader owns everything about reading:
handles, metadata calls, its error classes (counted per directory, never fatal unless the run must stop),
sorting, chunking. The core imports no provider; a reader imports the core.

Recursive excludes are installed before workers start with `Walker.SetPruneDirectories`, using the
filter package's proof that every descendant is excluded. `Out.Rows` checks that predicate only for
`Descend`: reader-owned `Omit` and `Prune` take precedence. `Walker.Excludes` exposes the same predicate to
the reader: localfs drops an entry whose stat failed when the key-only filters leave it out (a directory
only when its subtree is proved excluded too), counted in `filtered_stat_errors` rather than as unreadable. A match retains the directory row for the
ordinary row filter, emits no child, and increments `Stats.Excluded`, separately from reader-owned
`Stats.Pruned`. It applies equally to whole directories and segments. localfs reports these as
`pruned_dirs` and `other_device_dirs`. Azure HNS also installs the predicate in its registered
`Run` and reports `pruned_dirs` (absent when zero); both sources report the rule classification in
`pruning`. This avoids child listing requests on HNS; the service still lists the parent and returns
the directory metadata. Row rejection
alone never prevents descent, and no counts or bytes are estimated for unvisited descendants.

Two ways to read a directory:

1. **Unordered pages** (filesystem; S3 directory buckets, Drive, probably Azure Files). The reader reads the
   directory whole, keys its entries (`tree.Key`), sorts them (`tree.SortRows`, or its own sort that
   produces the same order), and either hands the rows to `Out.Rows` or cuts them into chunk segments
   (`Out.Seg`) that other workers complete (localfs: chunks of 2,048 sorted names statted in parallel).
   localfs keeps its getdents engine (offset arenas, parallel sort, presizing, count slabs, READDIRPLUS
   hints) behind this seam; its sort produces the order of `tree.Key` and `SortRows` (`TestSortRunsEqualsSerial`).
2. **Ordered and seekable within a directory** (`tree.Ordered`: Azure HNS). The provider supplies a
   `Lister`: one page of a directory in byte order of *names* (Azure's UTF-16 order made byte order by the
   existing `azblob/order` adapter), after a name, continuing by token; and `Lookup`, whether a name is a
   directory. `Ordered` reads a directory's first page itself; a directory that fits is emitted whole. A
   larger one emits the rows of that page and then one segment, a `keyrange` range `(c, ceil(prefix)]`
   starting after the last key emitted (`Engine.Within`), which the range engine splits and reads in
   parallel like a flat container. No directory is buffered whole.

### The ordered mode: names to keys

The engine works on keys; the store seeks by name. `Ordered`'s fetch translates each engine query
`(A, T]` (keys) into a name listing and back:

- **Reorder.** Within a directory a file's key is its name, a directory's key its name + `/`. A directory
  entry is held until an entry whose key passes its key arrives (`a`, then `a-b`: `a-b` is emitted, then
  `a/`; `tree.held_dirs`). Held entries form a stack whose top is the smallest key.
- **Page ends.** A page whose name page ended with nothing held continues by the lister's token (the mode's
  token carries the chain's start key with it). One that ended with directories held ends at its last
  emitted key `c`, and its token restarts the name listing after `c`: each held directory is one of `c`'s
  candidates (below) or a name after `c`'s, read again. A directory named like the start of thousands of
  its siblings (`part` beside `part-00000…`) is held across all of them, so it must not force one huge
  page: that is why a page may end with rows held. A page that emitted nothing yet reads on
  (`tree.resolve_pages`).
- **Start.** Names after `n` (the start key's part below the directory, cut at its first `/`) are listed;
  rows with key ≤ the start key are dropped (`tree.dropped_rows`: the low-byte siblings read again after a
  directory key).
- **Candidates.** A directory `d` sorts before a start key A by name and after it by key exactly when A starts
  with `d` and the next byte is below `/` (or A is `d`): `part` for a pivot `part-0015`. Such a directory
  belongs to the range but lies behind its seek. The fetch resolves A's candidates from what was read of the
  directory (the name intervals listed and the directories seen there) and asks `Lookup` only for a
  candidate outside them (`tree.lookups`); a present one starts the chain held (`tree.candidate_dirs`). A
  cursor's candidates were all read; a pivot's may not be, and then a split costs one lookup (`part-00150` lying
  beyond the parent's last name). At the default page of
  1,000 a split already costs at least two pages (the admission gates), so a lookup per split adds at most a
  third; skipping lookups whose candidate the lower child is certain to read is open.
- **Bounds.** `T` is not sent: the engine trims keys past it. A directory held past `T` belongs to the next
  range, whose candidates find it.

Every directory costs at least one request; a directory larger than one page costs its pages plus the
engine's splits and their lookups. The first page is never read twice: the segment seeks after it.

## Errors, engagement and cancellation

A reader returns an error only when the run must stop. localfs counts open, read and stat errors per
directory with samples (unchanged); Azure's request failures are the client's (retries, requeue through the
engine for segments and, the same way, for a directory's first page: `tree.first_page_requeues`, since tree
mode cannot resume). When one logical page needs several name pages to resolve held directories, a later
name page is requeued in place (`tree.resolve_requeues`), preserving the held rows and current continuation:
earlier successful reads have consumed their adapter tokens and cannot be replayed. The same provider
requeue predicate and tree backoff bounds apply; permanent failures return and cancellation interrupts the wait.
The walker checks the context before every
`Read`; readers check it in long loops (localfs as before; `Ordered` per name page). Engagement counters:
the walker's (`tree.Stats`: files, dirs, bytes, pruned, excluded, omitted, segments; localfs reports them in its own
fields), the ordered mode's (`tree.first_pages`, `tree.dirs_whole`,
`tree.dirs_split`, `tree.held_dirs`, `tree.resolve_pages`, `tree.dropped_rows`, `tree.candidate_dirs`,
`tree.lookups`, `tree.first_page_requeues`, `tree.resolve_requeues`, `tree.record_resets`,
`tree.record_evictions`), and the engine's own for segments. Directory records only save lookups. Each
record holds at most 4,096 combined name intervals, directory entries and negative lookup results; exceeding
that limit drops all its evidence together (`tree.record_resets`). The shared cache holds at most 256
directory records, evicting one at capacity (`tree.record_evictions`); an active reader can retain its own
bounded record until it finishes. Candidates whose evidence was dropped use `Lookup`, preserving ordering
without retaining the listing's entire page or directory history.

## Composition with the order stage and counts

The walker is an ordinary `source.Provider`: children are claimable when `Expand` returns, a range's
children are placed at the position they were emitted, segments are children like any other. A count
(`order.Options.Unordered`) needs no order: localfs skips its sort and reads slabs (unchanged); `Ordered`
needs no change (its rows are ordered anyway). Segments do not declare coverage fences, so durable spill does
not engage for tree sources (as for localfs today).

## Resume (designed, not built)

Neither tree source implements resume or declares durable coverage fences. Pruning suppresses child
submission; it does not advance a range-engine cursor or certify a subtree for recovery. A future
restart must apply the same frozen filters while replaying the path to K: a pruned directory creates
no child to resume, whether its own row is retained or filtered out.

The journal stays key-only: the last published key K. A tree source restarts by walking down to K:

- the root is listed again; entries with key ≤ K are dropped, except a directory whose key is a prefix of K
  (K lies in its subtree), whose row is dropped (≤ K) but which is descended with the same K;
- every directory not on K's path either sorts wholly before K (dropped with its subtree, never listed) or
  wholly after it (listed as on a fresh run);
- in the ordered mode the directory on K's path is not re-read: its segment starts after K (its candidates
  looked up once).

So only the directories on K's path are listed again. The seam is `Walker.Restart(K)` setting an `after` on
the root range that `Out.Rows` applies and hands to the child on K's path; `Ordered` maps it to the
segment's start. The listing mode (`flat` or `tree`) is frozen into the resume identity, so a journal of one
never resumes as the other (Azure: identity field `listing`, present only for `tree`). localfs gets resume
from the same seam.

## Other stores on this core

**S3 directory buckets** (Express One Zone): `ListObjectsV2` with `delimiter=/`, prefixes ending in `/`,
unordered pages and continuation tokens only, `CreateSession` auth on zonal endpoints. A reader of the
unordered kind: list a directory's pages to the end, key files and `CommonPrefixes` (directories; no
metadata of their own, so a directory row has only its key, or directories emit no row), `SortRows`, emit.
A directory of 100M objects under one prefix cannot be held: the reader must spill sorted runs and merge
them, emitting the merge as segments (bounded memory, the issue's spill item); it cannot be split, since
there is no seek.

**Google Drive** (#264): folders by ID, keys built from names on the walk (`Range.Dir` is the folder ID).
Unordered pages (`files.list` with `q='ID' in parents`). Duplicate names in a folder break key uniqueness
(a tiebreak such as the file ID appended to the key, before sorting); shortcuts and multiple parents need a
visited set of folder IDs in the reader (a `Prune` for a folder already walked); native documents have no
size (the payload's `Size` reports none).

## Risks

- **Spill.** The unordered mode holds a directory whole; S3 directory buckets need the spill before they
  can list large prefixes.
- **Demand across engines.** `Ordered` shares one `keyrange.Engine` for all segments; its demand counts only
  segments, not other directories being read, so it may split a large directory more eagerly than idle
  workers justify (bounded by the admission gates; extra requests, never wrong output).
- **Many tiny directories.** One request per directory: a Hive layout with one file per partition costs one
  request per file, where a flat account would cost one per 1,000.
- **Tree-order assumptions.** Azure's within-directory order is assumed to be UTF-16 by name (checked live on
  ASCII and on bingmlbuildings; the adapter's byte-order conversion is reused unchanged).
