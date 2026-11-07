# Ordering

`order.Run(ctx, provider, sink, Options)` turns a provider's range tree into one ascending stream.
`order.Count` walks the same tree without ordering it, for a dry run (below).

## Ranges and nodes

Each range becomes a node holding an item list: row segments and child nodes, in key order, as `Expand`
emits them. Workers pop queued nodes from a LIFO stack; children are pushed in reverse so the next-needed one
is on top. Emitters batch items locally and publish them under the lock at once when the drainer is on that
node, every 1024 rows, or when `Expand` returns: one lock round trip per directory, while long streaming
ranges stay visible as they grow.

The drainer walks the tree depth-first: it hands the sink each row segment in order (in batches of at least
4096 rows, and whatever is batched before it waits on the frontier or lists a range itself, so a part's age bound
and its durability see every row the walk has reached; `TestTimeCutPartDoesNotCalibrate` holds the source after a
few rows and needs them in a part), descends into each child, and frees a node once it is drained and its `Expand` has returned. It
streams the frontier: rows of the node it is draining are consumed as they arrive, and once the consumed items of
a node still being listed are at least 256 and half of its items, they are dropped with their storage, so a long
streaming range retains only what is not yet drained (`TestRunningNodeStorageIsBounded`).

## The order guard

Every row the drainer hands on passes a stream-wide guard: its key must sort strictly after the previous
row's, and on a resume after the last published key (`Options.After`). A provider that breaks its ordering
contract (two ranges each ascending whose boundary keys repeat or descend) fails the run with a
`*dataset.InvariantError` naming the row, before any sink sees it (`TestOrderGuard`), so text sinks get the
same protection as Parquet. The stage also counts the rows its emitters received and refuses to report success
unless it handed on exactly as many (spill, compaction and replay conserve rows); the pipeline compares both
counts with the provider's and the router's.

A failed range stops the stream at its node: the failure is latched under the stage's mutex before the node is
marked done, so the drainer never passes it and hands on a later sibling's rows. What a sink received is then
a prefix of the keyspace without a gap, the prefix a resume continues after. The context's stop callback
also sets the flag, but on its own goroutine and later: left to it alone, the drainer could pass the failed
node and publish a later tile's rows past the gap its dropped children leave, and a resume would then skip
those children's keys (`TestFailedRangeStopsTheStream` forces that window with `hookStopCallback`).

## The budget

`Options.Budget` is a byte budget, `--row-budget`, for rows in flight. A row counts its value (`schema.Row[P]`, the
key and the payload), its key bytes and the payload's strings (`Schema.RowBytes`, with the producer's `Heap`). The strings are views of the page arena they came from, so
counting them approximates the arena they keep alive. That holds only while most of a page is handed on: a row
filter (`registry/filtered.go`) that keeps at most half of a page's rows or charged bytes detaches the survivors
(`Schema.Detach`: a row slice and one string buffer sized to them, through the payload codec), so a sparse filter
cannot hold whole pages for a few charged rows (`TestSparseFilterPipelinePlateau`). The encoders' queues add the bytes of every queued
batch to the same budget (`order.Shared`, fed by the router's `Account` hook) and release them once
written. A slow encoder therefore slows producers instead of growing a second, unaccounted buffer. Pages being
parsed (each capped at 16 MiB), codecs and the garbage collector are outside it.

The budget bounds the bytes of rows read but not yet drained. A worker waits before claiming a range, and before
publishing rows, while the budget is used up. Two exceptions keep the walk live and both were forced by S3,
where one range can hold a whole bucket:

1. **Only the awaited node may exceed the budget.** The node the drainer is waiting for publishes regardless;
   other nodes, even ones on the drain path, wait. (Exempting the whole drain path lets a bucket-wide range
   buffer every row: measured at 971,927 rows, 621 MiB RSS.)
2. **The drainer streams what it expands.** When the drainer expands the range it needs itself, rows up to
   that range's first child are next in global order and go straight to the sink.

**Metadata is charged too.** A child node counts its node and item (`childUnits`, about 150 bytes) and a
spill marker its item (`markerUnits`) against the same budget, as metadata: a publish of only children and
markers waits once the metadata in memory would fill the budget by itself, so a provider emitting many
children, or a deep speculative tree behind a slow frontier, is bounded (`TestDelayedFrontierPlateaus`:
2,201 ranges, outstanding nodes peak at 929 under a 128 KiB budget; without the charge all 2,201 are held).

**Batches are bounded.** An emitter publishes after `publishRows` (1,024) rows or 1,024 items, and a single
`Rows` call larger than that is published in pieces of at most 1,024 rows, so one producer batch never exceeds
about 2,048 rows. The awaited node's lead is one such batch: the drainer clears the exemption once it wakes and
takes what was published. A range the drainer expands itself cannot be drained until its `Expand` returns;
past a full budget its published lead is capped at a quarter of the budget and its rows spill
(`TestDrainerExpandedLeadIsBounded`). Without spill (off, or the disk budget refusing) that one range is the
remaining exception: it proceeds, since the drainer cannot wait for itself. Object-store ranges return right
after their children, so they never take it.

Beyond the budget the walk holds the rows pending on the root-to-frontier path (the O(depth × fan-out) of any
depth-first walk, the awaited root's children included) and one publish batch per worker.

### What a waiting range retains

A queued range, or one finished ahead of the drain, waits with its `Range` value for as long as the frontier
takes, and the budget charges it as a node (`childUnits`). Its strings must therefore hold only their own
bytes. A page's strings are views of one buffer: the S3 parser copies a page into one arena and every key,
ETag and token is a view of it (`s3/client/xml.go`, `finish`), and the local provider slices a directory's
keys from one arena (`localfs/getdents_linux.go`, `buildKeys`). A bound that is such a view keeps the whole
page alive: about 1,000 keys and ETags, some 130 KB at 100-byte keys, for a range charged 150 bytes. A split's
lower child is the case that matters (`lo` is the cursor, the last key of its parent's page): on a 3M-key
reproduction with a held-up first range (4,800 outstanding ranges at a 128 MiB budget) the peak RSS is
808 MiB with the cursor as a view and 333 MiB with it copied ([measurements](../measurements.md#memory-behind-a-held-up-frontier)).

So every string the engine keeps past its page is copied, once per page, where it is retained:

| Retained | Where it comes from | Copied |
|---|---|---|
| the cursor `c` (the next page's order check, a structure probe's start, a lower child's `lo`) | the page's last key, or the last resume-skipped key | `expandRange`, after the page's rows are checked: one `strings.Clone` per page |
| the range's first key `head` (an upper child's reflection `anchor`) | the first page's first key | `expandRange`, once per range |
| structure-probe boundaries (`sb`, inherited by children) | a probe page's common prefixes | `probeStructure`, into one buffer per probe (`detach`) |
| seed cuts (tile bounds) | the seed's delimiter pages | `expandSeed`, into one buffer once the cuts are chosen (`detach`) |
| a local subdirectory's `prefix` | its row's key in the directory arena | `localfs.Provider.emit` |

Not retained, so not copied: the density observer's first and last keys (it keeps only a rate), the per-page
key list a split decision reads (dropped when it returns), the continuation token (replaced by the next
page's), a resume's `after` (read from the journal, never from a page), and pivots (the pivot functions
encode fresh strings). GCS pages are decoded by `encoding/json`, which allocates each string on its own: a
retained GCS key pins only itself, and the engine's copy covers it anyway. The order stage itself keeps only
rows (charged as rows, `Schema.RowBytes`) and the drainer's last key, which aliases the page of a row it has
already handed on. `TestRetainedBoundsDoNotPinPages` (listing; real XML through the production parser, a
held-up first range) bounds the peak live heap by the budget and the pages in flight and fails on the leak.

**What `--row-budget` is not.** It is not a cap on the process's resident memory. It bounds rows and
metadata in flight; outside it are response bodies being parsed (up to 16 MiB each, one per worker in the
worst case), arena slack (a surviving string keeps its page's allocation alive, which the row count only
approximates), compression scratch, open spill readers (at most 8 × 64 KiB), the encoders' writers (budgeted
at `parquetout.EncoderBytes`, 24 MiB each, apart from it: below) and the garbage collector's
headroom. The envelope that has been measured: `TestDelayedFrontierPlateaus` (synthetic, above) grows the heap
by about 15 MiB at the smallest `--row-budget`; RSS at 512 workers with 1 KiB keys has not been measured.

**Encoder admission.** The default encoder count is `GOMAXPROCS`, admitted against the run's resources with
a floor of one: as many writers as fit in an allowance equal to the row budget, kept apart from it (24 MiB each,
measured at about 20 MiB of heap for one writer) and as many descriptors as the soft limit leaves after 8
spill readers, two per worker and a reserve of 32 (`listing.admitEncoders`; the default 128 MiB admits 5). An
explicit `--encoders` above that is honoured with a warning.

## The waits

Every wait is on one of two condition variables under the stage's mutex, and every event that can make a
waiter eligible broadcasts to both; each waiter rechecks its own condition. Each wait is driven to its wait
and released by one event in a test with a deterministic barrier (`hookWait`):

| Wait | Who | Released by | Test |
|---|---|---|---|
| idle (`work`) | a worker, nothing queued | a range completing with children (pushed then), the drain ending, a stop | `TestIdleWorkerWakesOnChildren` |
| admission (`work`) | a worker, ranges queued, the budget used and spill unavailable (off, or the disk budget refused) | drained bytes released, bytes released downstream, spill credit returned (a drained range's file deleted), a stop | `TestAdmissionWaitWakes` |
| publish (`drain`) | an emitter of rows facing a full budget; of markers, while the markers alone would fill it; after a refused frame | the same releases, a marker replayed while the total stays full, spill credit, the drainer awaiting its node, a stop | `TestPublishWaitWakes`, `TestMarkerSaturationWakes`, `TestRefusedSpillWakesOnCredit` |
| frontier (`drain`) | the drainer, the range it needs being listed | that range publishing, completing (with rows, children only, or nothing), a stop | `TestFrontierWaitWakes` |

A release of drained bytes wakes every waiter, not only when it ends a full budget: an emitter of markers
waits for the marker bytes alone, which can fall while the total stays full. A deleted spill file returns
its credit and wakes every waiter, so a worker held at admission and an emitter whose frame was refused retry
at once. Publishing only children never waits (`TestChildOnlyPublishIsAdmitted`). The run's own `WaitGroup`
waits for providers and spill I/O to return: a stop wakes every condition variable but cannot interrupt
arbitrary I/O, which the command's forced-exit grace covers.

## Counting without order

A dry run (`--count-only`: no dataset, no text) consumes no row in order, so the pipeline runs `order.Count`
instead of `Run` (`registry.Env.Count`). Workers pop ranges from a LIFO stack and push a range's children when
its `Expand` returns, as in `Run`; rows are counted as they arrive and dropped. There is no tree, no budget,
no drainer and no spill: memory is the pages in flight and the ranges queued for a worker, and the run
creates no file (`TestCountOnlyHoldsNothing`). A stop reaches the providers through the emitter: every
`Rows` call, empty ones included, and every `Child` returns the cause once the run has stopped, and a child
handed over then is disposed, never expanded, so a range emitting only children (a wide local directory
with `--no-dirs`) stops at its next child (`TestCountStopsAChildOnlyRange`). The report says so: `features.order` false, `spill` absent,
`max_buffered_bytes`, `peak_ahead_rows` and `frontier_stuck_max_ns` 0.

It keeps conservation (rows received equal rows counted, then the pipeline's `reconcile` against the
provider), the progress file's `ordered` counter, and a per-range order check: each range's keys strictly
ascending across its `Rows` calls, and after `Options.After` (`TestCountGuardsEachRange`). What it does not
verify is the stream-wide guard across ranges: two ranges that overlap, each ascending, are counted
twice where `Run` would fail on the repeated key. Coverage and disjointness are the provider's tiling in
either case, and the object-store providers check it per range: the range engine sends each range's first
request after its `lo`, refuses any key at or below the last one (`lo` first) and trims keys past its `hi`
(`keyrange` `expandRange`), so a service answering out of bounds cannot make two ranges overlap; only
intervals that overlap from the engine's own splits or seed cuts could, which T1, the metamorphic laws and
`TestSeedListsEveryKey` test. A local directory's children are its distinct subdirectories. The guard
across ranges was defence in depth against a provider that breaks its tiling, and it still runs on every
run that writes. `Count` cannot apply it cheaply: the stream order is a tree walk, and checking it means
keeping each finished range's first and last key until its neighbours finish, the tree `Count` exists to
avoid.

## Cancellation

A stop (sink error, provider error, or context cancellation) sets an atomic flag and broadcasts to every
waiter; `ctx.Err()` is not polled on the hot path because it takes a mutex. Workers return, the drainer
returns the first error.

## Spill

When the budget is used up, a range that is not the one being drained spills instead of waiting. Its
emitter writes the rows it would have published to the range's spill file and publishes markers in their
place, so the range keeps listing while a slow frontier holds the output back. A range that ends with
nothing left to publish (its last batch went out with its last page), or with only children, never waits
either. Waiting at a range's end would free nothing and keep the node from being done; on a deep bucket, where
rows far ahead of the frontier keep the budget full, it would leave the drainer listing the frontier alone (one
range in flight of 64, as measured with that wait). Spill does not remove every wait: a worker still waits when the disk budget refuses a frame,
and an emitter of markers waits once the markers in memory would fill the budget by themselves, so at
sufficient scale (hundreds of finished ranges ahead of a stalled frontier) producers wait even with unlimited
disk. On the finite fixture of `TestSpillKeepsWorkersBusy` no worker waits on the budget:
- **Order.** The rows of one range are already in key order, so the drainer replays a range's frames front to
  back when it reaches the markers: no index, no merge, no dictionaries. Frames are written before their
  markers are published, so the drainer never reads ahead of the writer.
- **Active ranges spill too.** The unit is a publish batch of a running range, not a finished range. A
  range larger than the remaining budget therefore never blocks; this is what the slow-frontier shape needs.
- **Files.** One file per range under `DIR/.swath/spill/spill-*/` (or the temp directory for a text output; a dry run never spills).
  Frames are length-prefixed, carry a CRC-32C, and are typed: rows, zstd rows (`swath diag list --spill-compress`, level 1),
  or a trailer with the row count. A rows frame holds each row's key and the producer's payload encoded by
  its `schema.Codec[P]` (only the producer's fields: `fs.Entry` writes its present values, `s3.Object` its
  size, time and six strings); the encoding is private to one run. The trailer is written when the range ends and checked after replay.
  Children have their own files, so a parent's trailer says nothing about them.
- **Defensive reads.** A frame length is checked against a bound (256 MiB) before anything is allocated, and
  the reader is closed on every error path.
- **Readers are capped.** A deep walk leaves a partly replayed file per spilled ancestor. At most 8 readers
  (a descriptor and a 64 KiB buffer each, `order.MaxSpillReaders`) are open; past that the least recently
  used one is closed and reopened at its position when the drainer comes back to it
  (`TestSpillReadersAreCapped`).
- **Disposable.** A drained range's file is deleted. Run removes the whole spill directory when it returns,
  on success or failure. A crash leaves at most an orphan directory. The next run on the same output removes it (spill on or off; a failed removal is a warning), under the output
  lock, and the resume relists from the durable frontier.
- **Memory.** In-memory buffered rows stay within the budget plus one publish batch per worker plus the
  frontier path, with or without spill (`TestSpillKeepsWorkersBusy`). Workers also claim new ranges while
  the budget is full, since their rows will spill, unless the disk budget has refused a frame.
- **Disk budget.** A frame is written only if the live spill bytes stay within `-spill-max` (default: half
  the space free on the spill filesystem when the run starts, after the reserve; a library caller's negative
  value means no cap; the cap in effect is the report's `spill.max_bytes`) and the spill filesystem keeps a reserve free afterwards (`listing.spillReserve`: one part per encoder at
  full size, at least 1 GiB, so the parts being written, which share the filesystem with the spill directory
  under the output, and the journal and manifest have room; `swath diag list --spill-reserve` sets it; the
  reserve in effect is the report's `spill.reserve_bytes`). The two protect different things and are both
  kept: the cap bounds spill's own use, the reserve what spill may take from the parts and from other
  processes on the filesystem, whose use the cap cannot see. A refused
  frame is not an error: the emitter waits on the reorder budget, as with `swath diag list --no-spill`, and retries after the
  drainer moves. An ENOSPC on a frame of rows is handled the same way after truncating the torn frame; one
  that cannot be undone, or on a trailer, stops the run (resumable: nothing durable is lost). A
  range's bytes return to the budget when its file is deleted after replay, not frame by frame: a consumed
  frame's bytes stay on disk until the file goes (punching holes is not portable, and a range's file is
  deleted at its trailer), so live spill is what the disk really holds. Trailers (a few bytes) are
  always admitted, and concurrent emitters can each pass the free-space check before either writes, so the
  reserve can be undershot by at most one frame per worker; the reserve is checked, not held, so parts
  being published can use it too. A frame read back is refused (before allocating) past 256 MiB, compressed
  or decoded, and a rows frame claiming more rows than the largest frame its file was given is refused
  before its rows are allocated. A spill marker (the item a node keeps per frame on disk) counts its own
  size against the row budget: after a spill, markers and children are admitted without a wait unless the
  markers in memory would by themselves fill the budget, and a marker's bytes are released when its frame
  is replayed (`TestSpillMarkersInByteMode`, the one order test in byte mode). A failed run closes every
  open spill reader. The report's `resources` carries `peak_nodes`, `peak_markers`, `peak_ahead_rows` (rows received and
  not yet handed on, in memory or spilled), `frontier_stuck_max_ns` (the longest interval in which the
  drainer handed nothing on, closed on every drain exit, so a cancelled or failed run reports its last
  stall: `TestInterruptedRunReportsStall`; the interval is closed after the final flush, so a last sink write that
  blocks counts: `TestFinalFlushReportsStall`) and, under `spill`, `peak_bytes`, `peak_readers`, `max_bytes`,
  `reserve_bytes`, `dir` and `fs_type` (the spill filesystem: on `tmpfs` spill is memory, and the run warns
  at start); `progress.json` carries the same as live values with their peaks
  under `order` (`nodes`, `markers`, `spill_readers`, `spill_live_bytes`, `ahead_rows`, each with a `peak_` twin). The summary
  reports `spill_peak_bytes` and
  `spill_refused` (`TestSpillDiskBudget`: a cap at a quarter of the uncapped peak is held with identical
  output; a reserve larger than the disk spills nothing).
- `swath diag list --no-spill` restores waiting on the budget.
