# Invariants

The invariants the Go tool keeps, stated for its ordered design. Java numbers refer to
[`docs/internals/contracts.md`](../../../docs/internals/contracts.md) §0; where Go keeps the same rule by a
different mechanism, the mechanism is named. Tests that hold each one: [../testing.md](../testing.md).

| Go invariant | Java | Mechanism in Go | State |
|---|---|---|---|
| **No gap, no overlap.** The ranges of a run partition the keyspace: every key in scope is listed by exactly one range. | I2 | A split of `(lo, hi]` at cursor `c` and pivot `m` yields `(lo, c]` (already listed), `(c, m]`, `(m, hi]`; seed tiles `(⊥,p1], …, (pk, ⊤]`. | built |
| **The boundary belongs to the left.** `(a, b]` lists `a < k ≤ b`; the right neighbour starts after `b`. | I3 | `StartAfter = lo`; keys `> hi` are trimmed; a page ending at `hi` ends the range. | built |
| **Unsigned byte order.** Keys are compared, split and written as bytes, never as UTF-16 or by locale. | I10 | Go string comparison is bytewise; every stage checks strict ascent (provider, writer, gate). | built |
| **Safe pivots.** A synthesized pivot is valid UTF-8, strictly between its bounds, and adds no code point from the unsafe set E. | I12 | `ByteMidpoint` port; extrapolated pivots pass `safeString` and the same betweenness check. | built |
| **A split is atomic and has one owner.** | I4 | Only the range's owner splits it, after a full page, under the provider mutex; nobody else holds its cursor. Splits are never journaled; resume correctness depends only on the last published key ([durability.md](durability.md)). | built |
| **No infinite loop.** A truncated page without a token, or without a key in range, is an error. | I9 | Checked per page in `expandRange` and `expandSequential`. | built |
| **No wait on child work.** A worker never blocks on work it created. | I7 | Children become claimable; the drainer expands what it awaits itself. | built |
| **Bounded buffers.** Buffered rows are a function of `-memory`, not of object count, apart from the awaited node and the root-to-frontier path. | I11 | `order.Options.Budget`; bounded encoder channels; per-part metadata is O(parts). | built |
| **Commit before emit.** Progress is durable before rows past it are published. | I1 | Per part: the journal record (`PartInfo`, frozen limit, row total) is fsynced before the part's rename. | built |
| **Resume never relists published keys and never skips one.** | I5, I6 | Resume lists `(K, ⊤]` after the last recorded part's max key K, as one range split as on a fresh run; a part is published iff its record is durable and it is renamed (a recorded but unrenamed part is renamed on resume). | built |
| **Publication order.** Parts are complete and fsynced before rename; renames go in ordinal order; `manifest.json` is installed last by an atomic rename and a directory sync, and is the commit; run reports follow it. A failed or cancelled run leaves no manifest and no unpublished part. | §4.1 (format 2: the manifest replaces `_SUCCESS`) | `dataset.Publish` after the gate (dense ordinals, strictly ascending disjoint parts, rows written = rows routed). | built |
| **Deterministic output.** The same listing produces byte-identical parts for any worker or encoder count. | – (Go only) | Part boundaries depend only on rows ([parquet-output.md](parquet-output.md)); parallel column encoding is byte-identical to serial. | built |

I8 (shutdown order) maps to: the drainer stops consuming, sets the stop flag, wakes all waiters, waits for the
workers, then closes the router, which waits for its encoders.
