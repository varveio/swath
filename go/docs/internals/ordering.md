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

## The reorder budget

`Options.Budget` bounds rows read but not yet drained. A worker waits before claiming a range, and before
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

## Spill (planned, step 3)

When the budget is exhausted, finished ranges ahead of the frontier will spill to sequential files and be
replayed in order when the drainer reaches them. They are already sorted, so this is no merge. Workers then
keep listing while a slow frontier range holds the output back, instead of waiting on the budget. Output is
unchanged.
