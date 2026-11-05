# Ordering

`order.Run(ctx, provider, sink, Options)` turns a provider's range tree into one ascending stream.

## Ranges and nodes

Each range becomes a node holding an item list: row segments and child nodes, in key order, as `Expand`
emits them. Workers pop queued nodes from a LIFO stack; children are pushed in reverse so the next-needed one
is on top. Emitters batch items locally and publish them under the lock at once when the drainer is on that
node, every 1024 rows, or when `Expand` returns: one lock round trip per directory, while long streaming
ranges stay visible as they grow.

The drainer walks the tree depth-first: it hands the sink each row segment in order (in batches of at least
4096 rows), descends into each child, and frees a node once it is drained and its `Expand` has returned. It
streams the frontier: rows of the node it is draining are consumed as they arrive.

## The budget

`Options.Budget` is a byte budget: half of `-memory`, for rows in flight. A row counts its struct, its object
metadata and its strings (`schema.RowBytes`). The strings are views of the page arena they came from, so
counting them approximates the arena they keep alive. The encoders' queues add the bytes of every queued
batch to the same budget (`order.Shared`, fed by the router's `Account` hook) and release them once
written. A slow encoder therefore slows producers instead of growing a second, unaccounted buffer. The other
half of `-memory` is headroom for pages being parsed (each capped at 16 MiB), codecs and the garbage
collector.

The budget bounds the bytes of rows read but not yet drained. A worker waits before claiming a range, and before
publishing rows, while the budget is used up. Two exceptions keep the walk live and both were forced by S3,
where one range can hold a whole bucket:

1. **Only the awaited node may exceed the budget.** The node the drainer is waiting for publishes regardless;
   other nodes, even ones on the drain path, wait. (Exempting the whole drain path let a bucket-wide range
   buffer every row: 971,927 rows, 621 MiB RSS.)
2. **The drainer streams what it expands.** When the drainer expands the range it needs itself, rows up to
   that range's first child are next in global order and go straight to the sink.

Beyond the budget the walk holds the rows pending on the root-to-frontier path (the O(depth × fan-out) of any
depth-first walk) and one publish batch per worker.

## Cancellation

A stop (sink error, provider error, or context cancellation) sets an atomic flag and broadcasts to every
waiter; `ctx.Err()` is not polled on the hot path because it takes a mutex. Workers return, the drainer
returns the first error.

## Spill

When the budget is used up, a range that is not the one being drained spills instead of waiting. Its
emitter writes the rows it would have published to the range's spill file and publishes markers in their
place, so the range keeps listing while a slow frontier holds the output back:
- **Order.** The rows of one range are already in key order, so the drainer replays a range's frames front to
  back when it reaches the markers: no index, no merge, no dictionaries. Frames are written before their
  markers are published, so the drainer never reads ahead of the writer.
- **Active ranges spill too.** The unit is a publish batch of a running range, not a finished range. A
  range larger than the remaining budget therefore never blocks; this is what the slow-frontier shape needs.
- **Files.** One file per range under `-out/.swath/spill/spill-*/` (or the temp directory without `-out`).
  Frames are length-prefixed, carry a CRC-32C, and are typed: rows, zstd rows (`-spill-compress`, level 1),
  or a trailer with the row count. The trailer is written when the range ends and checked after replay.
  Children have their own files, so a parent's trailer says nothing about them.
- **Defensive reads.** A frame length is checked against a bound (256 MiB) before anything is allocated, and
  the reader is closed on every error path.
- **Disposable.** A drained range's file is deleted. Run removes the whole spill directory when it returns,
  on success or failure. A crash leaves at most an orphan directory. The next run on the same output removes it, under the output
  lock, and the resume relists from the durable frontier.
- **Memory.** In-memory buffered rows stay within the budget plus one publish batch per worker plus the
  frontier path, with or without spill (`TestSpillKeepsWorkersBusy`). Workers also claim new ranges while
  the budget is full, since their rows will spill, unless the disk budget has refused a frame.
- **Disk budget.** A frame is written only if the live spill bytes stay within `-spill-max` (default: no cap)
  and the spill filesystem keeps `-spill-reserve` bytes free afterwards (default 1 GiB, room for the parts
  still to be published; the spill directory is under `-out`, so they share the filesystem). A refused
  frame is not an error: the emitter waits on the reorder budget, as with `-no-spill`, and retries after the
  drainer moves. An ENOSPC on a spill write is handled the same way after truncating the torn frame. A
  range's bytes return to the budget when its file is deleted after replay. Trailers (a few bytes) are
  always admitted, and concurrent emitters can each pass the free-space check before either writes, so the
  reserve can be undershot by at most one frame per worker. The summary reports `spill_peak_bytes` and
  `spill_refused` (`TestSpillDiskBudget`: a cap at a quarter of the uncapped peak is held with identical
  output; a reserve larger than the disk spills nothing).
- `-no-spill` restores waiting on the budget.
