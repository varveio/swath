# swath-local spike: report

Date 2026-11-05. All numbers below were measured in this container: 4 vCPU (Intel Xeon @ 2.10GHz), 15 GiB RAM,
ext4 on a virtio disk (`/dev/vda`), Linux 6.18.44 (Firecracker guest), Go 1.26.0 (fetched by `GOTOOLCHAIN=auto`,
because parquet-go v0.32.0 and x/sys v0.49.0 need go >= 1.26), GNU find 4.9.0, fd 9.0.0 (`fdfind`, apt),
Python 3.13.18. Every number is labelled with the configuration that produced it. All trees are **synthetic**
(see "Trees"). Raw data: `bench/results/`.

> **Current code:** see "Restructure 2026-11-05" below and [DESIGN.md](DESIGN.md). The sections before it
> describe the pre-restructure spike (its engines, `--order` modes and encode schemes no longer exist); their
> numbers stand as the record of how the design was chosen.

## Answers

1. **Is the parallel fd-relative walker materially faster than find and the naive Go walk (warm cache)? Yes.**
   On the `realistic` tree (971,927 non-dir entries + 99,892 dirs, stat tier, median of 3):
   - `getdents` engine, 12 workers: **1.18 s**. `fstatat` engine (os.File.ReadDir): **1.38 s**.
   - `find -type f -printf '%s %T@ %p\n'` (stats every file): **3.96 s**, which is 3.4x slower than `getdents`.
   - Go `filepath.WalkDir` + `os.Lstat`: **6.30 s** (5.3x). Python `os.walk` + `os.lstat`: **5.80 s**.
   - The same parallel walker with full-path `os.Lstat`: **2.01 s**, so the fd-relative stat is worth 1.7x on its own.
   - Single-threaded, `getdents` w1 (3.57 s) and `fstatat` w1 (4.01 s) are close to `find -printf` (3.96 s). The
     win comes from parallelism plus dirfd-relative stat, not from a faster syscall.
   - Tools that do not stat are faster at listing names. `fd`: 0.92 s. `find -type f`: 1.59 s. Our names tier
     (`--fields names`, getdents): **0.48 s**.
2. **Can we skip the global sort by walking in sorted order? Yes.** Sorted depth-first emission produces
   globally sorted keys with no sort step (proved by test, and byte-identical output: equal part MD5).
   `--order dfs-parallel` (parallel reads plus a bounded reorder buffer) with `--reorder-rows 100000` was the
   fastest full pipeline in its run: **2.85 s, 142 MiB peak RSS, 0 spills**, against **3.03 s and 322 MiB** for
   the sort-based `none` mode (`realistic`, warm, median of 3, same interleaved run). `dfs-sequential` uses the
   least memory (56 MiB) but takes 6.54 s because it walks on one thread. Details are under "Sorted-order walk".
   After the later "Ordered walk optimization", the ordered count-only walk matches the unordered walk (1.27 vs
   1.27 s). The full `dfs-parallel` pipeline takes **1.84 s / 125 MiB**, against **2.48 s / 324 MiB** for `none`
   (same interleaved run, median of 3).
3. **What fraction of a full run is sort + Parquet?** On `realistic` (warm, `getdents`, 4 encoders, median of 3),
   the full run took 2.36 s: 1.24 s walk and 1.10 s sort+write. That is **47 %** sort+write, and 50 % above the
   pure walk (2.36 s vs the 1.18 s count-only walk). With serial encoding (`--encoders 1`, fstatat) it is 52 %
   (1.69 of 3.23 s). On the synthetic `wide` and `deep` trees, which have empty files and near-identical metadata,
   it is 29 % (0.61 of 2.07 s) and 28 % (0.43 of 1.52 s).

## What was built

- `walk`: a parallel directory walker. The work unit is one directory; N workers share a mutex-protected LIFO
  stack with a pending counter. Engines `getdents` (Linux default), `fstatat`, `parallel-lstat`, `portable` and
  `walkdir`. Three metadata tiers (`names`/`stat`/`statx`). Large directories have their stat phase split
  across workers. `--ino-sort` is optional. Errors are counted, never fatal.
- `pqout`: the October spike's writer path re-targeted at schema `swath-local/v0`. It covers the column-values
  API, bare INT adapters, delta keys, single-page ASCENDING key index, served geometry, the 8 MiB byte-target
  row groups and the go-entropy ZSTD codec. New: column-parallel page encoding (`--encoders`), byte-identical to
  serial (tested).
- `sortout`: run-based external sort (row/byte-bounded buffers, Parquet spill runs, k-way merge) into
  byte-targeted `data/part-NNNNN.parquet`. Publication follows Swath's layout: manifest, `.swath-state.json`,
  `symlink.txt`, `_swath_local_summary.json`, `_SUCCESS` last; atomic writes with fsync.
- `walk/ordered.go`: `--order dfs-sequential|dfs-parallel`, sorted depth-first emission streamed straight into
  the Parquet writer (`sortout.Streamer`, which encodes on its own goroutine behind a bounded channel).
- `sortout/parts.go`: opt-in per-part parallel encoding (`--encode-scheme part`), with router-fixed boundaries,
  a ratio frozen after part 0, dense ordinals, N independent serial writers and a pre-publication gate (Pass B).
- `cmd/swath-local`: `list <root>` CLI with a one-line or JSON summary (files, dirs, bytes, errors, walk and
  sort+write times, user/sys CPU, peak RSS from `VmHWM`).
- `verify.py` (DuckDB), `bench/gen_tree.py`, `bench/run.sh`, `bench/summarize.py`, `bench/oswalk_lstat.py`.
- Tests: walker vs `filepath.WalkDir` (every engine, tier, workers {1,3} and dirs on/off); external sort with
  multiple spills and concurrent emitters; unsigned byte order; single-run path; parquet-go round trip of order,
  count, raw footer schema and stamps; serial vs parallel encoding byte-identical; unsorted keys rejected.
  Ordered walks: for 4 engines x {names, stat} x {sequential; parallel with w1/bound 1, w4/bound 3,
  w8/bound 2M} x {dirs, --no-dirs}, the emitted key sequence equals the sorted unordered walk. The tree has `a-b`,
  `a.b`, `a`, `a0`, `ab`, `a b`, `z`, `~`, `\x7f`, `é` and `日本` as files in one directory and as directories in
  another, plus empty and nested directories. The Streamer test covers multi-part output and rejects a
  descending key.
  `go vet ./...` and `go test -race ./...` pass. The walker tests also pass as uid 65534 via `setpriv`, which
  makes the EACCES path real (root bypasses permissions).

Design details and flags: [README.md](README.md).

## Design choices (and why)

- **Schema `swath-local/v0`** (owner scope change), described in the next section. `key`/`size`/`last_modified`
  are identical to Swath's contract; S3-only columns were replaced.
- **Directories emit `kind=DIR` rows** with a trailing-slash key; `--no-dirs` omits them. The root has no row.
- **Default engine `getdents`.** It beat `fstatat` (`os.File.ReadDir`) by more than noise on `realistic` warm
  (1.18 vs 1.38 s at w12; 1.20 vs 1.35 s at w4; user CPU 0.97 vs 1.44 s). On `wide` they tie (0.81 vs 0.81 s)
  and on `deep` they are close (0.67 vs 0.72 s). `strace -c` (deep, w4) shows both make exactly one
  `newfstatat` per entry. `os.OpenFile`/`os.Open` adds 4 `fcntl` per directory (66,698 for 16,678 dirs), which
  the raw path avoids, and `ReadDir` allocates a `DirEntry` per entry.
- **Default workers `min(64, max(4, 3*GOMAXPROCS))` = 12** (research note). Warm-cache scaling flattens at
  GOMAXPROCS. `realistic`/getdents: w1 3.57, w2 1.90, w4 1.20, w8 1.23, w12 1.18 s. Cold-cache gains from more
  workers are small: `realistic`/getdents cold w4 5.50, w12 5.24, w32 5.08 s (single runs).
- **Sorter takes ownership of worker batches** (no copy) and sorts a `[]*Row` permutation. The first version
  copied batches into one shared append buffer under a mutex. Its full-run walk phase was 2.26 s vs 1.38 s
  count-only (`realistic`, fstatat, warm median of 3), with peak RSS 468 MiB. Dev runs (single runs, not the
  benchmark harness) showed `--gc-percent -1` cutting walk 2.4 to 1.33 s, but with only 4 GC cycles. The copy and
  buffer regrowth under GC write barriers, not GC CPU, was the cost. After the change: walk 1.50 s, 340 MiB
  (`realistic`, fstatat, full, median of 3).
- **Column-parallel encoding.** Profiling showed the single-threaded writer as the remaining bottleneck (1.39 s
  CPU for 1.07M rows). parquet-go column writers are independent, so each page's 15 columns are filled, encoded
  and compressed by `--encoders` goroutines. The output is byte-identical (unit test plus equal MD5
  `5c1382b4…` on `realistic` for 1 and 4 encoders). Sort+write with fstatat, warm, median of 3, 1 vs 4 encoders:
  `realistic` 1.69 to 1.10 s, `wide` 1.01 to 0.68 s, `deep` 0.56 to 0.37 s. zstd CPU rose under concurrency
  (profile: `EncodeAll` 0.33 s serial vs 0.78 s with 4 encoders), so the speedup is about 1.5x, not 4x. Concurrent row groups (`BeginRowGroup`) were rejected:
  they need row-group boundaries fixed in advance, which breaks the byte-target grouping.
- **Large-directory split** (`--split-entries 4096`). A directory with more entries has its stat phase cut into
  2048-name chunks for other workers, with a shared ref-counted fd. On a single 200,000-entry directory
  (`bench-data/huge`, 3 runs each): warm `fstatat` 0.21/0.23/0.23 s split vs 0.39/0.39/0.45 s unsplit; cold
  `getdents` median 0.73 s vs 1.30 s. `getdents64` itself and the names tier stay serial per directory.
- **`--ino-sort`** (stat in `d_ino` order) is implemented but off. It showed no measurable effect here. Cold,
  single runs: `realistic` 5.02 vs 5.24 s, `wide` 2.97 vs 3.06 s, `deep` 1.38 vs 1.38 s. On `huge` cold,
  3 runs each: median 0.74 vs 0.73 s split and 1.39 vs 1.30 s unsplit. This is inside run-to-run noise.
- **statx with `AT_NO_AUTOMOUNT`**; directories opened `O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW` (the root
  without `O_NOFOLLOW`).

## Sorted-order walk (`--order`)

**Why it is sorted.** Within one directory, entries are sorted by their key. For a subdirectory that is
`prefix/name/` (exactly its DIR row key, so name + `/`), and for anything else `prefix/name`. Emission is the DIR
row, then that subtree, before the next sibling. Every key in the subtree starts with `prefix/name/`. A sibling
that sorts after that string cannot share it as a prefix, because names contain no `/`, so it sorts after the
whole subtree. The DFS sequence is therefore strictly ascending in unsigned bytes. An example from the test:
`a-b` < `a.b` < `a/` < `a/…` < `a0` < `ab`, since `-` is 0x2d, `.` 0x2e, `/` 0x2f and `0` 0x30. The writer keeps
its strictly-ascending check on in every mode, and it was never violated.

**Modes.**
- `none`: parallel unordered walk, then the external sort (the original design).
- `dfs-sequential`: one goroutine reads a directory (same engine and tier code), sorts its entries, emits and
  recurses. Memory is O(depth × fan-out).
- `dfs-parallel`: workers read directories from a LIFO stack. Each finished directory pushes its
  subdirectories in reverse sorted order, so the next-needed directory is on top. The calling goroutine drains
  the tree depth-first and holds finished-but-early directories. Workers stop taking directories while
  buffered (read but not emitted) rows are at or above `--reorder-rows`. If the drainer needs a directory nobody
  has claimed, it reads it itself, so the bound cannot deadlock.

In both DFS modes rows stream into the Parquet writer through `sortout.Streamer` (no sort, no spill). The
`--split-entries` stat splitting is off in these modes.

**Results (full pipeline: walk + Parquet, `realistic`, getdents, stat tier, warm, median of 3, one interleaved
run).** The host had slowed by this point (see "Measurement drift"), so compare rows within this table only.

| Config | Wall s | User / sys s | Peak RSS MiB | Spill runs | Max reorder-buffered rows (3 reps) |
|---|---:|---:|---:|---:|---|
| `--order none` (2M-row sort buffer) | 3.03 | 3.23 / 5.84 | 322 | 0 | n/a |
| `--order none --sort-buffer-rows 300000` | 4.43 | 5.73 / 6.06 | 335 | 3 | n/a |
| `--order dfs-sequential` | 6.54 | 3.38 / 5.10 | 56 | 0 | n/a |
| `--order dfs-parallel` (12 workers, `--reorder-rows` 2,000,000) | 3.48 | 4.09 / 7.34 | 514 | 0 | 736,123 / 847,024 / 887,250 |
| `--order dfs-parallel --workers 4` | 2.96 | 3.86 / 6.18 | 171 | 0 | 127,008 / 159,532 / 246,935 |
| `--order dfs-parallel --reorder-rows 100000` | **2.85** | 3.80 / 5.74 | **142** | 0 | 100,745 / 100,508 / 100,636 |
| reference: count-only unordered, w12 | 1.86 | 1.15 / 5.41 | 41 | n/a | n/a |
| reference: count-only `dfs-parallel` | 2.79 | 2.39 / 7.02 | 374 | n/a | 755,906 / 90,288 / 886,623 |
| reference: count-only `dfs-sequential` | 6.55 | 1.57 / 5.02 | 15 | n/a | n/a |
| reference: count-only unordered, w1 | 5.76 | 0.94 / 4.73 | 15 | n/a | n/a |

Guest-cold, single runs (`bench/results/order-cold.txt`): `none` 7.40 s / 305 MiB; `dfs-parallel` 7.47 s / 105
MiB; `dfs-parallel --reorder-rows 100000` 7.86 s / 103 MiB; `dfs-sequential` 18.62 s / 54 MiB.

Reading:
- All three modes wrote byte-identical files (equal MD5 `5c1382b4…`), and `verify.py` passed on each (dev check).
- `dfs-sequential` costs the same as a one-worker walk plus per-directory sort and assembly: 6.55 vs 5.76 s
  count-only, with the same syscall counts under `strace -c`. It is the lowest-memory option, but
  single-threaded stat dominates.
- `dfs-parallel` with a large bound runs far ahead of the drain. The buffer reached about 0.9M of the 2M rows on
  a 1.07M-row tree, and the large live heap made it slower (3.48 s) and bigger (514 MiB) than `none`. A 100k
  bound held the buffer at the bound (plus at most one in-flight directory per worker), cut RSS to 142 MiB, and
  was 6 % faster than `none`. The reorder walk itself cost about 1 s over the unordered walk (2.79 vs 1.86 s
  count-only) in this run. "Ordered walk optimization" below removes that cost. The profile shows per-directory row copies, allocation and the per-directory sort, not lock
  contention. Moving encoding to its own goroutine did not change wall time (dev runs: 3.47 vs 3.48 s).
- A forced spill (`none`, 300k buffer, 3 runs) cost 1.4 s more than the in-memory sort, and its RSS was not
  lower (335 MiB: two buffers plus spill writer plus merge).
- Recommendation from these numbers: `dfs-parallel` with a reorder bound around 100k rows. It removes the
  sort, spills and `_staging/` disk use, and caps memory independently of tree size. This holds except for huge
  single directories, which need the split ported into this mode (see gaps). The default stays `--order none`
  with `--reorder-rows 2000000` as specified; the measured better bound is 100,000.

## Ordered walk optimization

**Goal:** bring `--order dfs-parallel` to within 10–20 % of the unordered walk at the same memory bound,
without changing output.

**Outcome:** met. The `dfs-parallel` count-only walk with a 100k-row bound now matches the unordered walk:
**1.27 s vs 1.27 s** (median of 3; it was 1.94 s). The full ordered pipeline is now **1.84 s**, against 2.48 s for
the sort-based `--order none` pipeline, which is 26 % faster. The part MD5 is unchanged (`5c1382b494b1…`) in
`none`, `dfs-sequential` and `dfs-parallel`. `verify.py` passes on the regenerated `out-realistic` and on both
DFS outputs.

### Profile before the changes

`realistic`, warm, `--order dfs-parallel --count-only`, Go pprof. These are single profiled runs: the CPU
profile run had the default 2M bound, and the alloc profile used `MemProfileRate` 64 KiB.

| # | CPU, flat | s | % |
|---|---|---:|---:|
| 1 | syscalls (`Syscall6`: fstatat, getdents64, openat, close) | 3.99 | 54.4 |
| 2 | `runtime.heapSetTypeSmallHeader` (allocating pointer-bearing objects) | 0.66 | 9.0 |
| 3 | `runtime.scanObject` (GC) | 0.23 | 3.1 |
| 4 | `runtime.tryDeferToSpanScan` (GC) | 0.19 | 2.6 |
| 5 | `runtime.memclrNoHeapPointers` | 0.16 | 2.2 |
| 6 | `runtime.spanClass.sizeclass` | 0.13 | 1.8 |
| 7 | `walk.readSorted` (cumulative 6.40 s, 87 %) | 0.12 | 1.6 |
| 8 | `runtime.(*spanInlineMarkBits).init` | 0.09 | 1.2 |
| 9 | `runtime.memmove` | 0.08 | 1.1 |
| 10 | `runtime.(*spanSet).push` | 0.07 | 1.0 |

| # | Allocations | MB | Objects |
|---|---|---:|---:|
| 1 | `readSorted`: row slice regrowth from cap 64, plus the `item{row, child}` copy of every row | 911 | 266k |
| 2 | `statEntries`: key `prefix + name` concatenation | 98 | 1.07M |
| 3 | `unix.ByteSliceFromString`: C-string copy of the name inside `unix.Fstatat` | 36 | 1.14M |
| 4 | `slices.Clone` of dents in `readGetdents` | 33 | 95k |
| 5 | `worker.child`: child job paths | 27 | 281k |
| 6 | `readGetdents`: one string per name | 23 | 903k |
| 7 | `worker.emit` | 17 | n/a |
| | **total** | **1,148** | **3.88M** |

The per-directory sort was 0.04–0.24 s of CPU in the profiles; the coordinator's single-threaded measurement
was 51 ms for the whole tree. The cost was allocation: about 4 objects per entry, 1.1 GB of churn, and the
allocator/GC work that comes with it.

### Changes (getdents engine, the default; other engines keep the generic path)

1. **No row copies; sort before stat.** Per directory, names are parsed from `getdents64` into a per-worker,
   reused name arena (no string per name). Keys `prefix+name` (plus `/` for directories) are built into **one
   key arena per directory** and taken as zero-copy strings (`unsafe.String`; the arena is never mutated
   afterwards). Entries are sorted by key **before** stat. Each stat result is then written directly into its
   final slot of **one row slab sized from the dirent count**. Nothing is appended, copied or moved afterwards.
   - A type change between `getdents` and `stat` fixes the key and re-sorts. This is rare.
   - `DT_UNKNOWN` entries fall back to the generic path.
2. **O(1) splicing.** A directory node is its row slab plus a `kids` list of `{position, child}`. The drainer
   walks the slab by index and recurses at each kid position. It does no per-subtree or per-row work beyond
   handing segments on.
3. **Writer receives segments by pointer.** The drainer passes contiguous sub-slices of the slabs
   (`SegmentSink.EmitSegments`), batched to at least 4096 rows per call. `sortout.Streamer` encodes straight
   from them. Slabs are not pooled: their lifetime ends when the writer has copied the values into parquet-go's
   page buffers, which would need a release protocol. They are left to the GC instead (100k slabs, not 3.9M
   objects).
4. **Fewer wakeups.** Workers and the drainer have separate condition variables.
   - A finishing worker signals the drainer only if the drainer waits for that directory.
   - It wakes workers only if it pushed children.
   - The drainer releases buffered rows in batches of `min(4096, bound/16)`, also before it blocks, and wakes
     workers only when that takes the buffer from at or over the bound to under it.
   - The earlier code broadcast to everyone on every directory.
5. Measured and **dropped**: calling `fstatat`/`statx` as raw syscalls on the NUL-terminated arena names, to
   avoid x/sys's 1.07M C-string allocations. A/B, count-only, 3 runs each: 1.36 s with raw syscalls vs 1.28 s
   with x/sys (median, noise). User CPU was 0.98 vs 1.07 s. With no wall-time gain, the simpler x/sys call was
   kept and the per-architecture syscall files were removed.

Not separately ablated: changes 1, 2 and 4 together. Change 3 (segments vs copying into batches, full pipeline,
3 runs): 1.83 vs 1.89 s wall (noise) and 2.70 vs 2.87 s user CPU. It was kept because it saves CPU and is what
the task asked for.

Profile after (same command, 100k bound):
- **CPU:** syscalls 76.9 % (3.03 s); next `readFast` 0.08 s flat (cumulative 94.7 %). No allocator or GC entry
  is above 1 %.
- **Allocations: 277 MB and 1.52M objects** (before: 1,148 MB and 3.88M). Of these, 233 MB is `readFast`: row
  slabs (about 128 MB of rows), key arenas and child nodes. Another 33 MB / 1.07M objects is x/sys's C-string
  per `fstatat`, the part kept by the decision in item 5.

### Before / after

`realistic` (1,071,819 rows), warm, 3 runs interleaved across all configurations and both binaries, median.
Driven by `bench/order_opt.sh`; raw data in `bench/results/order-opt-raw.tsv`. "Before" is the binary from just
before this pass.

| Config | Before wall s | After wall s | Before user / sys s | After user / sys s | Before RSS MiB | After RSS MiB | Max reorder rows (after) |
|---|---:|---:|---:|---:|---:|---:|---|
| unordered walk, count-only (reference) | 1.26 | 1.27 | 1.07 / 3.30 | 1.08 / 3.28 | 41 | 42 | n/a |
| `dfs-parallel` count-only, `--reorder-rows 100000` | 1.94 | **1.27** | 2.18 / 3.70 | 1.00 / 3.24 | 80 | 69 | 101–105k |
| `dfs-parallel` full pipeline, `--reorder-rows 100000` | 2.35 | **1.84** | 3.74 / 3.73 | 2.73 / 3.40 | 144 | 125 | 104k |
| `dfs-sequential` count-only | 4.38 | 3.74 | 1.41 / 3.06 | 0.77 / 2.80 | 16 | 15 | n/a |
| `dfs-sequential` full pipeline | 4.51 | 3.69 | 3.48 / 3.19 | 2.67 / 3.03 | 57 | 54 | n/a |
| `--order none` full pipeline (4 encoders) | 2.56 | 2.48 | 3.54 / 3.80 | 3.46 / 3.96 | 326 | 324 | n/a |
| `--order none` full, `--encoders 1` (after only) | | 2.90 | | 2.84 / 3.85 | | 320 | n/a |

- Spill runs: 0 in every configuration.
- Individual runs for the before `dfs-parallel` count-only were 1.94, 1.66 and 2.39 s, so the old reorder walk
  was also noisy. After: 1.27, 1.55 and 1.19 s.
- The `none` path did not change, so 2.56 vs 2.48 s is run-to-run noise.

### Sort vs encode, isolated (`--order none`, rows already in memory)

These come from the sorter's own timers in the same runs (median of 3). They exclude the walk:
- `final_sort_ns`: the in-memory global sort of all 1,071,819 rows, a parallel pointer sort with 4 goroutines.
- `merge_write_ns`: from the first row handed to the part writer to `Close`. It covers encode, compress,
  footer, file fsync, and the (single-source) merge iteration that feeds the writer.

| Step | Encoders | Median s | Share of `none` full-pipeline wall |
|---|---:|---:|---:|
| Global in-memory sort of 1.07M keys | n/a | 0.225 (0.221 in the `--encoders 1` runs) | 9 % of 2.48 s |
| Parquet encode + compress + close (rows sorted, in memory) | 4 | 0.881 | 36 % of 2.48 s |
| Parquet encode + compress + close (rows sorted, in memory) | 1 | 1.288 | 44 % of 2.90 s |

So in the sort-based pipeline the sort itself is small (about 0.23 s). The encode is about 4x larger, and it is
what the DFS modes take off the critical path.

### Overlap of write and walk

- In `--order none` the Parquet write **starts only after the walk ends**. Without spills, every row is
  buffered, sorted, then encoded. With spills, spill runs are written during the walk, but the final merge and
  encode still follow it.
- In the DFS modes the write **overlaps the walk**: segments go to the encoder goroutine while directories are
  still being read.
- Measured overlap = ordered full pipeline minus ordered count-only, which is the visible cost of writing:
  - `dfs-parallel`: 1.84 − 1.27 = **0.57 s**, against 0.88 s for the isolated encode with 4 encoders. About
    0.31 s (35 %) of the encode is hidden behind the walk; the rest competes with the walkers for the 4 CPUs.
  - `dfs-sequential`: 3.69 − 3.74 = **−0.05 s**. The single-threaded walk leaves CPUs free, so the encode is
    hidden completely.

## Per-part parallel encoding (Pass B)

This adopts Swath's Java approach (`PartEncoders`/`PartSizer`; scratchpad note `java-parquet-techniques.md`
§(b)/(c), techniques T1, T2, T3, T7 and the T14 gate) behind `--encode-scheme part`.

**Design** (`sortout/parts.go`):
- **Router-fixed boundaries.** A row's "logical bytes" are its uncompressed field sizes, which is
  deterministic. Part 0 is cut at `--part-size` logical bytes and encoded first. Its ratio
  `encoded / logical` is then **frozen**, and every later part is cut at `part-size / ratio` logical bytes. It
  is never re-estimated, so the boundaries do not depend on timing or on the encoder count.
- **Dense ordinals; `file_final` set by the router.** A full part is dispatched only once the next row exists,
  so the router knows the last part.
- **Encoders.** N goroutines, each writing one whole part with its own serial parquet-go writer and its own
  klauspost zstd encoder (concurrency 1, nothing shared). Part 0 is encoded with column-parallel pages, which is
  byte-identical to serial (tested), so the calibration part does not run on one core.
- **Input paths.**
  - In-memory `--order none`: the sorted `[]*Row` is routed by pointer, with no copy.
  - Spilled merge: the iterator reuses one row, so rows are copied into 4096-row slabs.
  - DFS stream: the stream segments are routed by pointer.
- **Bounded in flight.** At most N parts queued, plus N being encoded, plus 1 accumulating, so the writer
  retains at most **(2N+1) parts of rows**. With a 4 MB target on `realistic` a part is about 255k rows, so the
  bound is about 2.3M rows at N=4, which is more than the whole 1.07M-row dataset. This is why the streaming RSS
  below is high.
- **Pre-publication gate** (`checkParts`, applied in both schemes before the first rename): dense ordinals,
  strictly ascending disjoint key ranges (`max_i < min_{i+1}`), and rows written equal to rows routed.
- **Tests.**
  - `TestPartSchemeByteIdenticalAcrossEncoders`: in-memory, merged-spill and stream paths, at least 4 parts,
    encoders 1/2/4. It checks identical boundaries and part MD5s, dense `file_index`, `file_final` only on the
    last part, and row-for-row equality with the column scheme.
  - `TestCheckPartsGate`: overlap, ordinal gap and row-count mismatch are each rejected.
  - On `realistic` with `--part-size 4000000`: the same 6 part MD5s for encoders 1, 2 and 4, under both
    `--order none` and `--order dfs-parallel`. `verify.py` passes, and the rows are equal to the default dataset.

**Part target.** I used `--part-size 4000000` (4 MB) because it gives **6 parts** on `realistic` (the 64 MiB
default gives 1 part with the column scheme and 2 with the part scheme). The parts after calibration landed at
4.58–4.75 MB, which is **15–19 % over target**: the ratio frozen from part 0 does not transfer exactly. Part 0
itself was 0.37 MB (0.09x target, as expected for a calibration unit). Java reports ±0.5 %; matching that would
need a better logical-size model. Not pursued.

**Results** (`realistic`, warm, median of 3, interleaved; `bench/encode_bench.sh`, raw data in
`bench/results/encode-raw.tsv`). "Encode" is the sorter's `merge_write` timer: rows already sorted in memory,
from the first row handed to the writer to the last `Close` (including fsync).

| Configuration | Encoders | Encode s (rows in memory) | Full pipeline wall s | Peak RSS MiB | Parts | Bytes |
|---|---:|---:|---:|---:|---:|---:|
| `--order none`, 4 MB, **part** | 1 | 1.479 | 3.09 | 368 | 6 | 19,159,137 |
| `--order none`, 4 MB, **part** | 2 | 0.833 | 2.38 | 383 | 6 | 19,159,137 |
| `--order none`, 4 MB, **part** | 4 | **0.551** | **2.13** | 362 | 6 | 19,159,137 |
| `--order none`, 4 MB, column | 1 | 1.416 | 2.96 | 325 | 5 | 19,162,143 |
| `--order none`, 4 MB, column | 2 | 1.069 | 2.64 | 324 | 5 | 19,162,143 |
| `--order none`, 4 MB, column | 4 | 0.951 | 2.52 | 340 | 5 | 19,162,143 |
| `--order none`, 64 MiB, part | 4 | 1.312 | 2.90 | 372 | 2 | 19,152,078 |
| `--order none`, 64 MiB, column | 4 | 0.853 | 2.40 | 326 | 1 | 19,155,989 |
| `dfs-parallel` (100k), 4 MB, part | 4 | overlapped | 2.00 | 330 | 6 | 19,159,137 |
| `dfs-parallel` (100k), 4 MB, column | 4 | overlapped | 1.93 | 120 | 5 | 19,162,143 |
| `dfs-parallel` (100k), 64 MiB, part | 4 | overlapped | 2.57 | 210 | 2 | 19,152,078 |
| `dfs-parallel` (100k), 64 MiB, column | 4 | overlapped | **1.77** | 122 | 1 | 19,155,989 |
| `--order none`, 4 MB, part, **SpeedFastest** | 4 | 0.482 | 1.99 | 375 | 5 | 20,249,355 |
| reference: `dfs-parallel` count-only (100k) | | | 1.49 | 68 | | |

Reading:
- **With rows in memory and at least N parts, per-part encoding wins clearly.** It scales 2.7x from 1 to 4
  encoders (1.48 to 0.55 s), against 1.5x for per-column (1.42 to 0.95 s), and is 1.7x faster at 4 encoders.
  This matches Java's finding: per-column parallelism is capped by the key column and the shared codec. At
  4 encoders the full `--order none` pipeline drops from 2.52 to 2.13 s.
- **With few parts it loses.** At the 64 MiB default there are only 2 parts. The calibration part is small and
  the second part is encoded serially (1.31 vs 0.85 s).
- **In the streaming DFS modes it does not help.** The encode already overlaps the walk (count-only 1.49 s vs
  full 1.77–2.00 s). Per-part has to hold whole parts in flight: RSS 330 vs 120 MiB at 4 MB parts, with no
  wall-time gain (2.00 vs 1.93 s, within noise).
- **Default left at `--encode-scheme column`. This is an owner decision.** It is better or equal everywhere
  except in-memory sorting with many parts, it bounds memory in streaming, and it keeps the default output
  byte-identical to before. Recommendation: use `part` for `--order none` on trees large enough for at least
  2N parts; keep `column` for the DFS modes. An `auto` rule would need the part count before encoding, which is
  only known after calibration. Not implemented.
- **zstd `SpeedFastest` vs `SpeedDefault`** (per-part, 4 MB, 4 encoders): encode 0.48 vs 0.55 s (−13 %), full
  pipeline 1.99 vs 2.13 s, but **+5.7 % bytes** (20.25 vs 19.16 MB). Part boundaries also shifted (5 parts),
  because the frozen ratio changed. Default left at `SpeedDefault`; `--codec go-entropy-fastest` is available.

## Comparison with other listers (Pass C)

Run last, after Pass B. Nothing else was running in the guest, but the host was slower than in the main run: the
`swath-local` stat walk on `realistic` took 2.31 s here vs 1.18 s then, with sys time 7.0 vs 3.2 s. **Compare
only within this table.** Warm cache, 3 runs interleaved across tools and trees, median. Output went to
`/dev/null`. Driven by `bench/compare.sh`; raw data in `bench/results/compare-raw.tsv`.

Tools:
- `gdu`: `go install github.com/dundee/gdu/v5/cmd/gdu@latest`; it reports version "development".
- From crates.io with `cargo install --locked`, all built within 20–145 s: `diskus` 0.9.0, `dust` (du-dust)
  1.2.7, `pdu` (parallel-disk-usage) 0.24.0, `dua` (dua-cli) 3.0.0.
- `fdfind` 9.0.0 (apt) and GNU find 4.9.0.

**What each tool actually does.** Syscall counts are from `strace -f -c` on `deep` (516,670 entries below the
root). The stat form was read from `strace -e trace=newfstatat,statx`. Thread defaults, mount crossing and
symlink handling come from each tool's `--help`. None of the trees contain mount points, so mount crossing was
not exercised.

| Tool / invocation | Stats per entry (deep) | Stat form | Default threads | Crosses mounts | Follows symlinks | Output |
|---|---|---|---|---|---|---|
| `gdu -n -p -s` | 500,044 `newfstatat` (files only) | full path (`os.Lstat`) | 4 (`--max-cores`) | yes (`-x` to stop) | no (`-L`: file symlinks only) | total disk usage |
| `dust -d 0` | 516,681 `statx` | full path | rayon default (`-T`) | yes (`-x`) | no (`-L` derefs) | total |
| `diskus` | 516,675 `statx` | full path | 3 × cores (help) | yes (no option) | no | total disk usage |
| `pdu --max-depth=1` | 516,681 `statx` | full path | auto | yes (`-x`) | no | top-level sizes |
| `dua aggregate` | **644,470** `statx` (1.25 per entry) | files relative to dirfd, directories by full path | logical CPUs | yes (`-x`) | no | total |
| `fdfind -uu --changed-before 2100-01-01 .` | 516,678 `statx` | full path | CPUs | yes (`--one-file-system`) | no (`-L`) | paths |
| `fdfind -uu .` | 0 (uses `d_type`) | n/a | CPUs | yes | no | paths |
| `find -type f -printf '%s %T@ %p\n'` | 533,380 `newfstatat` | not checked | 1 | yes (`-xdev`) | no | paths, size, mtime |
| `swath-local --fields names --count-only` | 0 (`getdents64` `d_type`) | n/a | 12 | yes (no option) | never | rows (discarded) |
| `swath-local --count-only` (stat) | 516,694 `newfstatat` | **relative to dirfd** | 12 | yes | never | rows (discarded) |
| `swath-local` full pipeline | as stat | relative to dirfd | 12 + 4 encoders | yes | never | sorted Parquet dataset |

The du-style tools only sum sizes, keeping per-directory totals or a tree. `fd` and `find` print paths.
`swath-local` builds a full 15-column row per entry, including key strings, even when it then discards it.

**Results** (warm, median of 3, wall s, user / sys s, peak RSS):

| Tool | realistic wall | realistic user / sys | realistic RSS MiB | wide wall | wide user / sys | wide RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| `gdu` | 3.57 | 1.90 / 9.23 | 26 | 1.22 | 1.37 / 2.83 | 76 |
| `dust` | 3.10 | 1.74 / 9.10 | 377 | 1.15 | 0.92 / 2.70 | 217 |
| `diskus` | 2.45 | 0.87 / 8.69 | 7 | 0.75 | 0.45 / 2.49 | 7 |
| `pdu` | **2.26** | 0.73 / 8.19 | 4 | **0.72** | 0.38 / 2.32 | 4 |
| `dua` | 4.03 | 2.12 / 9.94 | 11 | 3.86 | 1.68 / 5.47 | 7 |
| `fdfind` + time filter (stat) | 2.83 | 1.32 / 9.13 | 8 | 1.02 | 1.01 / 2.71 | 9 |
| `fdfind` (no stat) | 0.96 | 1.05 / 1.75 | 9 | 0.34 | 0.65 / 0.38 | 10 |
| `find -printf` (stat, 1 thread) | 6.48 | 0.58 / 5.46 | 3 | 2.38 | 0.44 / 1.77 | 3 |
| **swath-local names**, count-only | **0.66** | 0.67 / 1.58 | 42 | **0.17** | 0.17 / 0.34 | 32 |
| **swath-local stat**, count-only | 2.31 | 1.19 / 7.00 | 45 | 0.78 | 0.50 / 2.12 | 30 |
| swath-local full: `dfs-parallel`, 100k bound → sorted Parquet | 2.94 | 3.01 / 6.99 | 123 | 1.21 | 1.55 / 2.40 | 78 |
| swath-local full: `--order none` → sorted Parquet | 3.58 | 3.52 / 7.65 | 322 | 1.57 | 2.04 / 2.56 | 198 |

Reading:
- **Name listing (no stat):** `swath-local --fields names` is the fastest tool measured. It took 0.66 vs 0.96 s
  for `fd` on `realistic`, and 0.17 vs 0.34 s on `wide`.
- **Stat every entry:** `swath-local` ties the fastest Rust du-style tools.
  - `realistic`: 2.31 s vs `pdu` 2.26 and `diskus` 2.45.
  - `wide`: 0.78 s vs 0.72 and 0.75.
  - It beats `fd` with a stat filter (1.2–1.3x), `dust` (1.3–1.5x), `gdu` (1.5x), `dua` (1.7x on `realistic`,
    4.9x on `wide`) and single-threaded `find -printf` (2.8–3.1x).
- **Kernel CPU.** `swath-local` spends less kernel time than every full-path stat tool: sys 7.0 s vs 8.2–9.9 s
  on `realistic`. This matches dirfd-relative `fstatat` avoiding path walks. `pdu` makes it up in user time
  (0.73 vs 1.19 s), because it builds no rows.
- **Full pipeline cost.** A globally sorted, verified Parquet dataset costs 2.94 s on `realistic` with
  `dfs-parallel`, 1.3x the fastest tool that only sums sizes. The sort-based `--order none` costs 3.58 s.
- **Not established here:** cold-cache behaviour of these tools, hard-link de-duplication (several du tools
  count hard links once; `swath-local` lists every name), and behaviour on network or overlay filesystems.

## Restructure 2026-11-05

The spike was rebuilt as a reusable seed ([DESIGN.md](DESIGN.md)). **Sections above this one describe the
pre-restructure code and CLI** (engines, `--order`, `--encode-scheme`, the external sort); their measurements
stand as history but their flags no longer exist. The older `bench/*.sh` scripts drive the old binary.

**What changed**
- **One pipeline.** provider → order → part router → dataset (`listing.Run(ctx, Config)`); `cmd/swath-local`
  only parses flags. Deleted: the external sort, spill runs, run reader, k-way merge, pointer sort, split
  batches, `--order none`/`dfs-sequential`, the `fstatat` and `parallel-lstat` engines (`walkdir` survives only
  as the test oracle `filepath.WalkDir`), `--ino-sort`, `--statx-mask`, `--split-entries` (internal constants
  now), `--encode-scheme`, `--codec`, `--gc-percent`, `--cpuprofile`/`--memprofile` (now
  `SWATH_LOCAL_CPUPROFILE`/`SWATH_LOCAL_MEMPROFILE`), `--sort-buffer-*`, `--reorder-rows` and
  `--row-group-bytes`. **11 flags remain**: `-out`, `-overwrite`, `-fields`, `-no-dirs`, `-no-link-target`,
  `-part-size`, `-memory`, `-workers`, `-encoders`, `-count-only`, `-json`. The reorder bound is
  `memory / 2 KiB` rows (131,072 at the 256 MiB default).
- **Provider contract** (`internal/source`): a tree of ranges, each streaming rows and ordered child ranges
  (`Emitter.Rows`, `Emitter.Child`). The fs provider is the first implementation; DESIGN.md says how S3
  split/steal, a key-cursor checkpoint, GCS, Azure and Drive fit it. The order stage streams the frontier as
  rows arrive and bounds rows of ranges that finished early.
- **Encoding**: per-part architecture only (router-owned deterministic boundaries with a ratio frozen after
  part 0, dense ordinals, router-set `file_final`, publication gate). Parts are fed while open through bounded
  queues, at most `-encoders` in flight, and a part encodes its page columns in parallel when fewer parts than
  encoders are active. That keeps the single 64 MiB part at today's speed (below) and bounds memory for any
  part size. Part 0 is cut at `part-size / 0.1` logical bytes (0.1 is the assumed compression ratio), so the
  default run on `realistic` is still one part and **byte-identical**.
- **Context and errors**: `signal.NotifyContext` in main; every stage takes the context and stops on the first
  error (sink, encoder, provider); unpublished parts are removed on every error path; errors are wrapped with
  path or phase; sentinel errors from `dataset` are mapped to flag names only in the CLI.
- **Schema as data** (`internal/schema`): one column list drives the Parquet node, the row-to-column fill and
  the manifest message string (`TestFooterMatchesMessage` ties them together); Swath's 13-column S3 schema is
  expressed as specs only.
- **Linux engine**: one `getdents64` parser (`binary.NativeEndian`, `bytes.IndexByte`, fuzzed), one open helper,
  one `statAt`, `readNames`/`buildKeys`/`statInto`/`emitChunks` phases; directories over 4096 entries become
  2048-entry chunk ranges sharing one fd (the same chunk mechanism as an S3 split). An entry whose type changes
  between listing and stat is now dropped and counted (`changed`) instead of re-sorted.
- `.swath-state.json` is no longer written (no resume; consumers ignore it). `-statx` uses the full mask;
  the default `stat` tier stays on `fstatat` (full-mask statx would fill `birth_time` and change the bytes).

**Byte identity** (`realistic`): default settings give one part with MD5 `5c1382b494b1b17aa9c8303077a5ab25`,
unchanged. `-part-size 4000000` gives 5 parts (it gave 6 before the ratio guess changed part 0), identical
across `-encoders` 1, 2 and 4 (`verify.py` passes on both).

**Performance work during the restructure** (each measured by A/B before keeping):
- The emitter now collects a range's items and takes the order lock once per range (or per 1024 rows, or at
  once when the drainer is on that range); one lock per `Rows`/`Child` call had added about 25 % to the
  names tier.
- The drainer reads finished ranges without the lock (their items are immutable).
- `ctx.Err()` takes the context's mutex in Go 1.26; hot loops now check an atomic flag (order) or a
  non-blocking receive on `ctx.Done()` (provider). Profile before the fix: `futex` and `Mutex.Lock` among the
  top entries.
- The router sends each order batch to a part as one channel message instead of one message per directory
  segment: the 64 MiB full pipeline went from 2.21 s to 1.97 s (median of 5, A/B against the previous build).
- Not kept: waking one worker per new range instead of broadcasting (no difference).

**Before / after** (`realistic`, warm, median of 3, interleaved, pre-restructure binary built from commit
`7d99970`; `bench/restructure.sh`, raw data in `bench/results/restructure-raw.tsv`). The host was noisy
(load average 2.6 at the start; several configurations, before and after, have one run 0.3–0.5 s off).

| Configuration | Before wall s | After wall s | Before user / sys s | After user / sys s | Before RSS MiB | After RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| count-only, stat (before: unordered) | 1.24 | | 1.02 / 3.42 | | 40 | |
| count-only, stat (before: dfs-parallel 100k) | 1.64 | 1.81 | 0.95 / 3.22 | 1.19 / 3.41 | 70 | 93 |
| count-only, names (before: unordered) | 0.52 | | 0.64 / 1.13 | | 41 | |
| count-only, names (before: dfs-parallel 100k) | 0.51 | 0.58 | 0.70 / 1.11 | 0.92 / 1.07 | 68 | 87 |
| full, 64 MiB part (before: dfs-parallel, column) | 1.93 | 1.99 | 2.83 / 3.28 | 3.35 / 3.43 | 124 | 158 |
| full, 4 MB parts, 1 encoder (before: part scheme) | 2.67 | **2.17** | 2.50 / 3.56 | 2.83 / 3.37 | 315 | **121** |
| full, 4 MB parts, 2 encoders | 2.16 | 2.02 | 2.55 / 3.56 | 2.98 / 3.60 | 325 | **132** |
| full, 4 MB parts, 4 encoders | 1.99 | 1.96 | 2.80 / 3.72 | 3.30 / 3.48 | 327 | **156** |

Individual runs, stat count-only: before 1.64 / 1.79 / 1.33 s, after 1.26 / 1.82 / 1.81 s; both builds have
runs near 1.2–1.3 s and near 1.6–1.8 s in this session, so the difference is inside the noise.

Reading: the full pipelines are level or faster, with a third to a half of the memory at 4 MB parts
(streaming per-part encoding no longer holds whole parts). **Residual regression:** the names tier is about
10–14 % slower (0.58 vs 0.51 s) with about 0.2 s more user CPU, and every row uses 0.2–0.5 s more user CPU.
Profiles of old and new names runs show the same top entries (syscalls ~50 %, sort, allocation, GC); I did not
find a single cause. Suspects not yet isolated: one `Emitter` and node allocation per range plus the generic
order stage's item lists (the old code used one fixed rows-plus-children node), and the extra parts machinery
(logical-size accounting per row) in the full pipeline.

**Line counts** (non-test / test Go lines):

| Before | src | test | After | src | test |
|---|---:|---:|---|---:|---:|
| `cmd/swath-local` | 305 | 180 | `cmd/swath-local` | 202 | 83 |
| `walk` | 1,618 | 413 | `internal/source` + `internal/source/fs` | 31 + 836 | 349 |
| `sortout` | 1,162 | 498 | `internal/order` | 382 | 162 |
| `pqout` | 767 | 108 | `internal/parquet` | 790 | 265 |
| `record` | 105 | 0 | `internal/schema` | 239 | 0 |
| `internal/proc` | 56 | 0 | `internal/listing` | 233 | 137 |
| | | | `internal/dataset` | 426 | 115 |
| **total** | **4,013** | **1,199** | **total** | **3,139** | **1,111** |

New tests: mid-walk cancellation leaves no parts and returns `context.Canceled`; a sink failure stops the walk
promptly (fewer than 200 of 9,331 ranges expanded); provider failure; an encoder failure and a publish failure
leave no `_SUCCESS`; the dirent parser is fuzzed; the parquet-go copy contract behind zero-copy strings is
pinned; the README flag list is generated from the flag set and checked by a test.

## S3 provider 2026-11-05

**What was built.**
- **Provider** (`internal/source/s3`): ListObjectsV2 pages of 1000 keys with continuation tokens and no
  delimiter, each page emitted as it arrives. Each page is checked for ascending keys, both within the page
  and against the previous page.
- **Connection and credentials:** `s3://bucket[/prefix]` roots, `-endpoint`, `-region`, `-path-style`.
  The default credential chain is used, falling back to anonymous access when it finds none.
- **Retries:** the SDK's standard retryer, wrapped so retries are counted (`s3.retries` in the summary).
- **Experiment flags:** `-fetch-owner` and `-max-keys` (the experiment guard).
- **Stage 2:** `-seed`, a static delimiter seed.
- **Schema.** Rows use Swath's canonical schema bound as data in `internal/schema` (`schema.S3`), so the
  writer, the manifest message string and the value filling are shared with the local schema. The mapping
  follows Swath's Java (`S3ObjectEntryMapper`, `ListEntryWriteSupport`):
  - the etag has one pair of quotes stripped, and the multipart `hex-N` form is kept;
  - `size` is 0 and `last_modified` is epoch 0 when absent, as Java writes them;
  - `version_id` and `is_latest` are null, `is_delete_marker` is false and `row_type` is `OBJECT`;
  - `checksum_algorithm` is the first algorithm returned;
  - `owner_*` is filled only with `-fetch-owner`.
- **Dataset.**
  - The manifest's `sourceBucket` is the bucket and `sourceKind` is `s3`.
  - `_swath_summary.json` is written as the subset of Swath's schema_version 2 run summary that readers check:
    `completed`, empty `filters`, and `config.target` = `s3://bucket` or `s3://bucket/prefix`.
  - The local dataset still writes none, and its MD5 is unchanged (`5c1382b4…`).
- **Interface fit.**
  - **`internal/source`:** the contract was unchanged.
  - **The shared row type:** it needed the object-store fields. They sit behind one pointer
    (`Row.Object`), which is nil and costs 8 bytes in a filesystem row.
  - **`internal/order` (two changes, both found with S3):**
    1. Only a range the drainer is *waiting for* (or expanding itself) may exceed the budget. Before, the
       drainer's whole current node was exempt, which let one bucket-wide streaming range buffer every row
       when the sink was slower (971,927 rows, 621 MiB).
    2. When the drainer expands its own current range, rows up to the first child go straight to the sink.
       Without this, a root range the drainer reached before any worker was buffered whole.

    After both changes: `max_buffered=0`, 68 MiB RSS.
  - Both changes keep the deadlock argument, and the order tests passed 40× under `-race`.

**Schema comparison** (`verify.py` against a fake-bucket dataset, `bench/results/verify-s3-fake.txt`). All 13
columns match contracts §4 in order, physical type, repetition and logical type, as DuckDB reads them:
- `key` BYTE_ARRAY REQUIRED String;
- `size` INT64 OPTIONAL unannotated;
- `last_modified` INT64 OPTIONAL Timestamp(UTC, MICROS);
- `etag`, `storage_class`, `version_id`, `owner_id`, `owner_display_name`, `checksum_algorithm`, `checksum_type`
  BYTE_ARRAY OPTIONAL String;
- `is_latest` BOOLEAN OPTIONAL;
- `is_delete_marker` BOOLEAN REQUIRED;
- `row_type` BYTE_ARRAY REQUIRED String.

Every row is a current object. The manifest's `fileSchema` is the parquet-mr message string rendered from the
same column list.

**Fake bucket** (gofakes3 in memory, `bench/s3fake`).
- **Contents:** the 971,927 non-directory keys of the `realistic` tree with their mtimes. Each body is the
  key, so sizes are key lengths and ETags differ.
- **Loading:** 8.0 s straight into the backend.
- **Server:** HTTP on localhost, warm.

| Run (971,927 keys, 972 pages) | Wall s | Client user / sys s | Peak RSS MiB | Rows/s | Notes |
|---|---:|---:|---:|---:|---|
| sequential, full pipeline | 10.0–10.6 | 10.8–11.2 / 0.9–1.0 | 67–70 | 92k–97k | 1 part, 27.3 MB |
| sequential, `-count-only` | 11.1 | 10.3 / 1.6 | | 87k | the fake server used 4.2 s of CPU for this listing |
| `-seed`, 1 / 4 / 8 workers (3 runs each) | 9.3–9.9 | 11.0–11.9 | 148–153 | ~100k | 3 seed prefixes; byte-identical to sequential |
| sequential 12 workers, same run set | 9.8–10.5 | 10.8–11.1 | 69–70 | | |

**The client, not the fake, dominates.** A CPU profile of a sequential run attributes:
- 8.4 s of 11.95 s client CPU (70 %) to the SDK's REST-XML deserializer (`awsRestxml_deserializeOpDocumentList…`),
  about 8.6 µs per object;
- 2.2 s to Parquet encoding;
- about 0.2 s to the provider and the order stage together.

A sequential listing parses one page at a time on one core. The seed helps only as far as the prefixes divide
the keys: `realistic` has 3 top-level prefixes (`data/`, `home/`, `projects/`) and `projects/` holds most of
them, so the seed gains 5–10 %. The seed's higher RSS comes from finished children waiting in the reorder
buffer.

**Real bucket: not reachable for listing.**
- `curl -sI https://commoncrawl.s3.amazonaws.com/?list-type=2&max-keys=1` reached S3 through the proxy and got
  `403 AccessDenied`.
- With the SDK, the container's AWS environment credentials were used first and rejected:
  `api error InvalidAccessKeyId: The AWS Access Key Id you provided does not exist in our records`.
- Anonymously (credentials unset): `StatusCode: 403 … api error AccessDenied: Access Denied`. commoncrawl
  refuses anonymous ListBucket.
- Per the instructions, no other host was tried, so there are no real-bucket numbers.

**Varve.** A temporary test in `varve/internal/swath` (Varve `2353ada`; deleted afterwards, `git status` clean;
copy in the scratchpad) opened the datasets with `Open(ctx, DirSource(root), "s3", "bench")`:

| Dataset | Open | Eligibility | Flat List | Delimiter List | Stream |
|---|---|---|---|---|---|
| whole bucket, 1 part | ok | eligible, no reasons | ok | root → `data/ home/ projects/`; `projects/proj-00-` → `projects/proj-00-model/` | 971,927 objects = manifest, ascending |
| whole bucket, 7 parts (`-seed`, 4 MB parts) | ok | eligible | ok | same | 971,927, ascending |
| `s3://bench/home/` (prefix) | refused: `run summary contradicts whole-bucket coverage` | | | | |
| local `out-realistic` | refused: `manifest does not describe the selected bucket dataset` | | | | |

The S3 dataset passes every check that the local one failed:
- the bucket match;
- the run summary;
- the four S3 columns `version_id`, `is_latest`, `is_delete_marker` and `row_type`, with their pure-object
  statistics.

The prefix refusal is correct, not a bug. Varve treats a dataset as a whole-bucket listing, and the run
summary says this one is not. Opening a prefix dataset would need a Varve-side notion of a prefix dataset.

**Stage 2 (static delimiter seed; since replaced by the parallel core's seed, see the next section).**
- **Done:** one delimited ListObjectsV2 of the root yields the root's own objects (rows) and its common prefixes
  (child ranges at their sorted positions, with the trailing-slash rule). The order stage lists the children in
  parallel.
- **Byte identity:** sequential, and the seed at 1, 4 and 8 workers, gave the same part MD5 (`2982b28d1b31…`).
- **gofakes3 deviation:** gofakes3 returns a key equal to a common prefix (`a/`) both as an object and as a
  prefix, so the seed listing drops objects whose key contains the delimiter after the prefix; S3 itself never
  returns them.
- **Limit:** one prefix with most of the keys stays a single sequential range; that is where Swath's split and
  steal start (Stage 3).

**Binary size:** the AWS SDK roughly doubles the stripped binary: linux/amd64 24.7 MB (was 13.3), linux/arm64
22.5, darwin/arm64 23.1, windows/amd64 25.2 MB.

## S3 parallel core 2026-11-05

**What was built** (`internal/source/s3/parallel.go`, `pivot.go`). The basics of Swath's parallel engine,
following the spec written from the Java engine
(`scratchpad/s3-parallel-core-spec.md`). DESIGN.md "S3" has the full rules. In short:
- `(lo, hi]` ranges: the first page is sent with `StartAfter=lo`, later pages with continuation tokens. A page
  ending at `hi` ends the range without another request.
- A one-page delimiter seed (`-seed`) with a cut cap of `min(1000, 4×workers)`, even subsampling and Swath's
  closing sentinel.
- Owner-side, demand-driven splits at page boundaries, with `hunger = workers − active − queued`, the progress
  gate, the largest-remainder victim with the frontier first, and the guard clauses, each counted.
- A port of Swath's `ByteMidpoint`: safe scalars, the bounded midpoint, ForwardReflect and the cap fallback.
- The instrumentation in the JSON summary:
  - `pages`, `requests`, `retries`, `overfetch_keys`;
  - `seed.*`;
  - `split.{considered, pivot_extrapolated, accepted_bounded, accepted_frontier, rejected.*, skipped.*}`;
  - `child_empty.{lower, upper}`;
  - `max_active`, `max_queued`, `mean_active`, `busy_ns`, `sink_wait_ns`.
- The sequential continuation-token provider stays as the reference (`-sequential`).
- The parallel core's seed replaces stage 2's static prefix seed, whose children were whole prefixes.

**Deviations from the spec, each with its reason:**
1. **Pivot: density extrapolation first.** Keys are read as numbers in the alphabet seen on the last page. The
   target is `cursor + page span × max(4, hunger)`, at most halfway to `hi`, cut after its first character that
   differs from the cursor. The midpoint or reflection is the fallback. Synthetic T6 balance check, both runs at
   `max_active=16`:
   - midpoint and reflection only: 2.58 s, 171 empty children;
   - with extrapolation: 1.15 s, 187 empty children.

   On `p/%07d` keys the spec's pivots mostly land just after the cursor. The cursor and `hi` diverge at
   adjacent scalars (`'/'` vs `'0'`), so `between` falls back to `cursor + " "`. ForwardReflect from ⊥ moves
   only the last character, and later frontier pivots truncate to the next block start.
2. **No optional order wait-time stats.** Aggregate idle time is `workers × wall − busy_ns`.

**Contract and `order`: no change in this stage.** The provider contract held as written. Splits end the
owner's `Expand` with two `Child` calls, which `order` already supports. The two `order` changes in this
session were made earlier, in the S3 provider stage: the budget exemption is limited to the awaited node, and a
drainer-expanded range streams directly. This stage kept them.

**Invariant tests** (all pass; `go test -race ./...` green):

| Test | What | Result |
|---|---|---|
| T1 | 14 keyspaces × 50 seeds × W ∈ {1, 4, 64} | Every key exactly once, in order, and the covered intervals tile (⊥, ⊤] |
| T2 | pivot properties | 20,000 random pairs plus named cases: `a < m < b`, valid UTF-8, ≤ 1024 bytes, safe; no pivot for `a`/`a\x00`; ForwardReflect `m > c`; prefixCeil |
| T3 | guard and gates | One case per clause (cursor, bound, done, unsafe cursor, no pivot); no split at 1 worker (no demand); no split on a single final page |
| T4 | paging | Page ending at `hi` uses 1 request; truncated empty page, out-of-order page and key ≤ lo are errors; cancellation mid-request returns `context.Canceled` with no goroutines left; seed truncated, cap 8 at W=2 with subsampling; sentinel |
| T5 | parallel = sequential | Through `listing.Run` on gofakes3 with 5 ms latency, 12,000 keys, 64 KiB parts: Parquet parts byte-identical and manifest equal apart from its timestamp, without and with the seed |
| T6 | balance smoke | Synthetic: `p/%07d` × 200k, 20 ms per page, W=16, default budget: `max_active=16`, 191 splits, 187 empty children of 382, 391 requests for 200 pages of keys, 1.15 s |

T1's keyspaces:
- empty, oneKey, exactly3000, flatRandom, singlePrefixFlat, flatUpperHex;
- longKeys1024, lastByteDiffer, maxScalarRun, supplementaryPlane;
- deepTree, badlySkewed, hotNeedle;
- markers plus keys with `%`, controls and DEL.

T1 runs on a fake that returns short truncated pages and rejects unsafe start-after values like real S3. Splits
are taken by a seeded coin, with the guard on.

**Fake bucket** (971,927 keys of `realistic`, gofakes3 on localhost with no added latency; the client and the
fake share the 4 vCPUs; median of 3, interleaved; `bench/s3_parallel.sh`, raw data in
`bench/results/s3-parallel-raw.tsv`). Every run produced the same Parquet bytes (`2982b28d1b31…`).

| Config | Wall s | User s | RSS MiB | Splits | Empty children | Requests | max_active |
|---|---:|---:|---:|---:|---:|---:|---:|
| sequential (oracle) | 10.62 | 11.10 | 66 | 0 | 0 | 972 | n/a |
| parallel W=1 | 11.02 | 11.41 | 66 | 0 | 0 | 972 | 1 |
| parallel W=4 | **5.82** | 14.46 | 168 | 165–228 | 20–30 | 1,074–1,120 | 4 |
| parallel W=8 | 6.24 | 16.41 | 145 | 365–393 | 85–102 | 1,219–1,245 | 8 |
| parallel W=16 | 6.89 | 18.39 | 160 | 592–633 | 227–233 | 1,411–1,433 | 16 |
| parallel + seed W=1 | 11.00 | 11.21 | 67 | 0 | 0 | 977 | 2 |
| parallel + seed W=4 | 6.78 | 13.87 | 154 | 159–190 | 22–31 | 1,085–1,108 | 5 |
| parallel + seed W=8 | 7.21 | 16.17 | 169 | 395–427 | 121–127 | 1,258–1,273 | 8 |
| parallel + seed W=16 | 7.73 | 18.51 | 171 | 631–651 | 264–266 | 1,445–1,457 | 16 |

Reading:
- **Speedup.** The parallel core gives 1.8x at 4 workers on this host. It is CPU-bound: the SDK's XML
  deserialization (about 8.6 µs per object, see "S3 provider") and the fake server share 4 cores. More workers
  add splits, empty children and extra requests without adding cores: 16 workers needs 47 % more requests than
  sequential, of which 230 are empty children. Against real S3, latency rather than CPU bounds a page, so more
  workers should pay off; that was not measured.
- **The no-probe cost is visible.** At W=16 about 37 % of split children list nothing, while at W=4 about 6 %
  do. A probe (or the owner-gate floors left out) would remove most of them. `child_empty` is the counter to
  watch.
- **The seed does not help here.** `realistic` has 3 top-level prefixes, so the seed adds 3 tiles while
  splitting does the rest. Its tiles are bounded, so splits use the midpoint path more and children are more
  often empty.

**Real bucket:** not measured. As recorded in "S3 provider", commoncrawl refuses anonymous ListBucket
(`403 AccessDenied`), and no other host was tried.

**Left out** (per the spec, §E):
- durability and resume;
- thief-side narrowing;
- structure and point probes;
- the owner gate chain and pacing;
- multi-level and mass-aware seeding;
- the rate-anchored estimator;
- AIMD;
- versions mode;
- per-worker idle-time stats in `order`.

## Measurement drift

The host slowed during the session (Firecracker guest; nothing else was running in the guest, and `vmstat`
showed steal time). Count-only `getdents` w12 on `realistic` took 1.18 s (median of 3) in the main warm run and
1.86 s in the later `--order` run. `find -printf` went from 3.96 to 5.88 s and `getdents` w1 from 3.57 to 5.76 s.
Each table compares configurations measured in the same interleaved run. Do not compare absolute times across
tables. Ratios held: the parallel walker beat `find -printf` by 3.4x in the main run and 3.2x in the later run.

## Schema and cost

Physical schema as DuckDB 2.0 reads it from `/home/user/bench-data/out-realistic` (`parquet_schema`; DuckDB
`DESCRIBE` reports every column nullable regardless). Full output: `bench/results/verify-out-realistic.txt`.

| Column | DuckDB type | Parquet physical / repetition / logical | Swath Java contract (contracts §4) |
|---|---|---|---|
| `key` | VARCHAR | BYTE_ARRAY, REQUIRED, String (converted UTF8) | `BINARY (STRING)`, required: **same** |
| `size` | BIGINT | INT64, OPTIONAL, none | `INT64`, optional, unannotated: **same** |
| `last_modified` | TIMESTAMP WITH TIME ZONE | INT64, OPTIONAL, Timestamp(isAdjustedToUTC=1, MICROS) (converted TIMESTAMP_MICROS) | `INT64 TIMESTAMP(MICROS,UTC)`, optional: **same** |
| `kind` | VARCHAR | BYTE_ARRAY, REQUIRED, String | replaces `storage_class` + `row_type` |
| `mode`, `uid`, `gid`, `nlink` | INTEGER | INT32, OPTIONAL, none | new |
| `inode`, `dev`, `blocks` | BIGINT | INT64, OPTIONAL, none | new |
| `ctime`, `atime`, `birth_time` | TIMESTAMP WITH TIME ZONE | INT64, OPTIONAL, Timestamp(UTC, MICROS) | new |
| `link_target` | VARCHAR | BYTE_ARRAY, OPTIONAL, String | new |

Differences from the Java contract are all intentional (owner scope change). Absent: `etag`, `storage_class`,
`version_id`, `is_latest`, `is_delete_marker`, `owner_id`, `owner_display_name`, `checksum_algorithm`,
`checksum_type`, `row_type`. Added: the columns marked new. Message name `swath_listing` and footer sort stamps
match Swath. `swath.sort.file_index` is 1-based; `swath.sort.file_final=true` appears only on the last part (from
`SortedParquetWriter`). The manifest keeps Java's fields and order, plus additive `schemaId`, `sourceKind`,
`sourceRoot` and `totalRows`; `sourceBucket` is `""`. **Not checked:** reading these files with the Java reader
or swath-replay (no JDK was run). The shape matches `Manifest.isCompleteManifest` by code reading only.

Cost per tier (`realistic`, warm, median of 3, 12 workers, count-only):

| Tier | getdents engine | fstatat engine | Syscalls per entry | Columns filled |
|---|---:|---:|---|---|
| `names` | 0.48 s | 0.66 s | 0 (getdents only, amortised) | key, kind |
| `stat` | 1.18 s | 1.38 s | 1 `fstatat` (+1 `readlinkat` per symlink) | + size, last_modified, mode, uid, gid, nlink, inode, dev, blocks, ctime, atime, link_target |
| `statx --statx-mask basic` | 1.25 s | 1.39 s | 1 `statx` | same as stat |
| `statx --statx-mask all` | 1.19 s | 1.41 s | 1 `statx` | + birth_time |

- **Free with the directory read:** key and kind (from `d_type`).
- **One syscall per entry, after which everything else is free:** every stat column. On this tree the stat tier
  costs about 2.5x the names tier (getdents, 0.48 to 1.18 s). On this kernel statx (basic or all) costs the same
  as fstatat within noise, so `birth_time` is effectively free once statx is used.
- **Extra syscall only for symlinks:** `link_target` (readlinkat). Unmeasurable here because `realistic` has
  only 80 symlinks: `--no-link-target` 1.34 vs 1.38 s, fstatat. It stays on by default.
- **Left out, because each costs extra reads per file or per entry:** content digests (read every byte), xattrs
  (`listxattr`/`getxattr` per entry), ACLs (`getxattr system.posix_acl_*`), file attributes such as immutable
  (`ioctl` per file), and the mount ID / `STATX_MNT_ID`. None are implemented.
- A full run also pays for building rows. In full mode the walk phase is slower than count-only: `realistic`
  1.24 vs 1.18 s, but `wide` 1.47 vs 0.81 s, with sys time 4.6 vs 2.2 s. This is consistent with first-touch
  page faults on about 120 MB of fresh row batches (count-only reuses one batch). It was not isolated further.

## Benchmark tables

Method: `bench/run.sh warm`. The tree is warmed once with `find -printf`, then each configuration runs 3 times
(repetitions interleaved across configurations), timed by GNU `/usr/bin/time` (wall/user/sys/max RSS of the
whole pipeline). The table shows the median. "entries/s" = non-directory entries in the tree / median wall.
`count` is what the tool counted: `find`/`fd` count regular files only; swath-local counts files, symlinks and
directories. `sl-*` = swath-local, `wN` = workers, unsuffixed = 12 workers. Count-only rows come from the first
binary; walk code was unchanged afterwards. `sl-full-*` rows come from the final binary (`warm-full-v2-raw.tsv`);
the first binary's full rows are in `warm-raw.tsv` (realistic fstatat full: 3.98 s).

### Headline (warm, median of 3, stat tier unless noted)

| Config | wide wall s | deep wall s | realistic wall s | realistic user / sys s | realistic peak RSS MiB | realistic non-dir entries/s |
|---|---:|---:|---:|---:|---:|---:|
| `find -type f \| wc -l` (no stat) | 0.45 | 0.42 | 1.59 | 0.41 / 1.19 | 3 | 611,275 |
| `find -type f -printf '%s %T@ %p\n'` | 2.77 | 2.23 | 3.96 | 0.92 / 2.93 | 3 | 245,436 |
| `fdfind --type f --hidden --no-ignore` (no stat) | 0.38 | 0.31 | 0.92 | 1.07 / 1.71 | 9 | 1,056,442 |
| Python `os.walk` + `os.lstat` | 3.50 | 3.38 | 5.80 | 1.50 / 4.01 | 11 | 167,574 |
| sl `walkdir` (WalkDir + Lstat, 1 thread) | 3.92 | 3.49 | 6.30 | 2.00 / 4.19 | 15 | 154,274 |
| sl `parallel-lstat` w12 | 1.04 | 1.07 | 2.01 | 1.95 / 4.93 | 44 | 483,546 |
| sl `portable` w12 | 1.21 | 1.11 | 2.13 | 2.20 / 4.85 | 46 | 456,304 |
| sl `fstatat` w12 | 0.81 | 0.72 | 1.38 | 1.44 / 3.32 | 41 | 704,295 |
| sl `getdents` w12 | 0.81 | 0.67 | 1.18 | 0.97 / 3.18 | 44 | 823,667 |
| sl `getdents --fields names` | 0.16 | 0.15 | 0.48 | 0.58 / 1.08 | 42 | 2,024,848 |
| sl full list, `getdents` (walk + sort + Parquet, 4 encoders) | 2.07 | 1.52 | 2.36 | 3.31 / 3.72 | 315 | 411,833 |
| sl full list, `fstatat`, `--encoders 1` | 2.47 | 1.70 | 3.23 | 3.43 / 3.80 | 336 | 300,906 |
| sl full list, `getdents --fields names` | 0.89 | 0.57 | 1.63 | 2.70 / 1.71 | 323 | 596,274 |

### Worker scaling (warm, median of 3, count-only, stat tier, realistic; wall s)

| Engine | w1 | w2 | w4 | w8 | w12 |
|---|---:|---:|---:|---:|---:|
| parallel-lstat | 5.82 | 3.18 | 2.04 | 1.94 | 2.01 |
| portable | 6.09 | 3.42 | 2.11 | 2.02 | 2.13 |
| fstatat | 4.01 | 2.19 | 1.35 | 1.34 | 1.38 |
| getdents | 3.57 | 1.90 | 1.20 | 1.23 | 1.18 |

`wide` and `deep` show the same shape (full tables in `bench/results/tables.md`). Example, `wide` getdents:
2.43 / 1.21 / 0.82 / 0.79 / 0.81 s.

### Cold cache (one run each, `sync; echo 3 > /proc/sys/vm/drop_caches` before every run)

`drop_caches` is permitted here. It empties the guest page/dentry/inode caches (Buffers+Cached+SReclaimable fell
from about 4.6 GB to about 0.19 GB). This is a VM, so the host may still cache the disk image: read these as
guest-cold, not true-disk-cold. They are single runs.

| Config | wide wall s | deep wall s | realistic wall s | realistic user / sys s |
|---|---:|---:|---:|---:|
| `find -type f` (no stat) | 1.22 | 1.15 | 8.54 | 0.52 / 4.42 |
| `find -printf` (stat) | 14.16 | 4.39 | 19.99 | 1.26 / 12.79 |
| `fdfind` (no stat) | 0.74 | 0.65 | 3.24 | 1.74 / 4.85 |
| Python `os.walk` + `os.lstat` | 12.11 | 5.55 | 19.57 | 2.90 / 10.86 |
| sl `walkdir` | 11.92 | 5.33 | 20.04 | 2.88 / 11.73 |
| sl `parallel-lstat` w12 | 3.72 | 1.57 | 5.91 | 2.91 / 14.51 |
| sl `fstatat` w4 / w12 | 3.65 / 3.39 | 1.49 / 1.41 | 5.88 / 5.39 | 2.13 / 13.36 (w12) |
| sl `getdents` w4 / w12 / w32 | 3.36 / 3.06 / 2.99 | 1.51 / 1.38 / 1.42 | 5.50 / 5.24 / 5.08 | 1.58 / 13.29 (w12) |
| sl `getdents --ino-sort` w12 / w32 | 2.97 / 2.88 | 1.38 / 1.39 | 5.02 / 5.18 | 1.60 / 12.81 (w12) |
| sl full list, `getdents` | 3.65 | 1.81 | 6.37 | 4.12 / 13.65 |

Guest-cold stat listing is sys-CPU bound (13 s of sys on 4 CPUs for `realistic`), not I/O-wait bound. The
parallel walker (getdents w12, 5.24 s) is 3.8x faster than both `find -printf` (19.99 s) and `walkdir` (20.04 s) on `realistic`, and fd (no
stat) remains faster than any stat tier.

### Output and write throughput (warm, final binary, default settings: 64 MiB parts, 8 MiB groups, go-entropy)

| Tree | Rows | Parts | Dataset bytes | MB per million rows | Sort+write s (median) | Rows/s in sort+write |
|---|---:|---:|---:|---:|---:|---:|
| realistic, stat | 1,071,819 | 1 | 19,155,989 | 17.9 | 1.10 | 974,000 |
| realistic, names | 1,071,819 | 1 | 8,674,174 | 8.1 | 0.99 | 1,083,000 |
| wide, stat | 1,001,000 | 1 | 2,597,846 | 2.6 | 0.61 | 1,641,000 |
| deep, stat | 516,670 | 1 | 2,152,439 | 4.2 | 0.43 | 1,202,000 |

Sort+write covers final in-memory sort, merge, encode, compress, file fsync and publication. It excludes the walk
it overlaps with. `realistic` with serial encoding: 1.69 s, 634k rows/s. The `wide`/`deep` sizes come from
synthetic empty files with sequential metadata and are not representative. `realistic` (sparse files with
log-uniform sizes, real-looking names) is the better guide. Peak RSS of a full `realistic` run was about 315 MiB
for 1.07M rows (about 300 B/row resident). Runs above the 2,000,000-row default buffer were not measured at that
scale. A forced 4-run, 5-part `realistic` run (`--sort-buffer-rows 300000 --part-size 4000000`) produced
row-identical content to the 1-part dataset and passed `verify.py` (43 checks:
`bench/results/verify-realistic-multipart.txt`).

### Binary size (`CGO_ENABLED=0`, statically linked, final source)

| Target | default flags | `-ldflags='-s -w'` |
|---|---:|---:|
| linux/amd64 | 19,318,459 B | 13,299,874 B |
| linux/arm64 | 18,107,609 B | 12,386,466 B |
| darwin/arm64 | 18,553,202 B | 12,640,610 B |
| windows/amd64 | 19,420,160 B | 13,629,952 B |

Most of the size is parquet-go's dependency tree (go.mod lists brotli, lz4, protobuf, go-geom and others).
Per-package attribution was not measured.

## Portability

The module cross-compiles with `CGO_ENABLED=0` for linux/amd64, linux/arm64, darwin/arm64 and windows/amd64;
`go vet ./...` passes for all four. Only Linux was run. Linux-only pieces: the `getdents`/`fstatat` engines
(`walk_linux.go`: raw `getdents64`, `fstatat`/`statx`/`readlinkat`, `O_NOFOLLOW` directory fds, large-directory
split, `--ino-sort`, `statx` tier), the full `Stat_t` mapping in `sysinfo_linux.go`, and peak-RSS/CPU reporting.
Elsewhere `walk_portable.go` makes `portable` (`os.ReadDir` + `DirEntry.Info()`) the default. Windows fills size,
mtime, mode bits, atime and birth_time (from `CreationTime`; no ctime). macOS/BSD fill only kind, size, mtime and
mode. Keys are `/`-separated on every OS.

On Linux the portable fallback makes the same syscalls per entry: `strace -c`, `deep`, 4 workers, 516,671
`newfstatat` for every engine. It is 1.8x slower (`realistic` warm w12: 2.13 vs 1.18 s) because each `lstat`
resolves a full path, `os.ReadDir` sorts names, and every entry allocates a `FileInfo`. Natural fast paths for
later: `getattrlistbulk(2)` on macOS and Windows directory enumeration (`FindFirstFileEx` /
`GetFileInformationByHandleEx`), both of which return size and times with the names.

## Trees (synthetic; `bench/gen_tree.py`, seed 20261105)

| Tree | Non-dir entries | Dirs | Shape |
|---|---:|---:|---|
| `wide` | 1,000,000 | 1,000 | 1,000 empty files per dir |
| `deep` | 500,000 | 16,670 | fan-out 4, depth 8, first 12,500 leaves × 40 empty files (a full 4^8 tree cannot hold 40/leaf at 500k) |
| `realistic` | 971,927 (971,847 files + 80 symlinks) | 99,892 | 40 project trees (src/, nested node_modules/, .git/objects/), 300 days × 24 h Hive partitions × 20 files, 20 home dirs with spaces and non-ASCII names; sparse files with log-uniform sizes (943.8 GB apparent, about 0 allocated) |
| `huge` | 200,000 | 1 | one directory (split test only) |

Total about 2.79M inodes. The trees stay in `/home/user/bench-data`. The retained dataset
`/home/user/bench-data/out-realistic` (default settings, `getdents`, 1 part, 19.2 MB) passes `verify.py`.

## Known gaps

- Names that are not valid UTF-8 are skipped and counted; for a directory the whole subtree is skipped. This
  needs an escaping rule before it can match Swath's UTF-8 key contract. No 1024-byte key cap is enforced.
- No `--one-file-system`: the walk crosses mount points. No checkpoint or resume.
- `getdents64` and the names tier are serial within one directory; only the stat phase splits, and only in
  `--order none`. The DFS modes read each directory whole on one worker, so a huge directory is serial there.
  Its rows also count once against the reorder bound, which is exceeded by up to one in-flight directory per
  worker.
- `--encode-scheme part` sizes parts from a frozen part-0 ratio. On `realistic` the parts landed 15–19 % over
  target, because the logical-size model is coarse. In streaming modes it retains up to (2N+1) parts of rows,
  and that bound is not tied to `--reorder-rows`.
- `dfs-parallel` reorder-bound waiting uses one mutex and condition variable. That is not a bottleneck at
  4 CPUs (profile), but it is untested at high core counts.
- Timestamps are truncated to microseconds. uid/gid ≥ 2^31 and inode/dev ≥ 2^63 read back negative (bit
  patterns).
- The run report `_swath_local_summary.json` uses swath-local's own schema. It was renamed from
  `_swath_summary.json`, Swath's reserved name, which Varve parses and rejects. `.swath-state.json` has
  `run_id: null`.
- No cycle protection (bind mounts). Some error categories are coarse: a directory that vanishes before its open
  counts as `open_errors` (review L4, L5; not fixed).
- Encoding is one part at a time. Concurrent zstd inflated compression CPU, and parts are not encoded in parallel.
- Interoperability was verified with DuckDB and parquet-go, not with Swath's Java reader or swath-replay. Varve's Go
  reader does not accept it yet; see "Varve reader compatibility".
- Cold-cache numbers are guest-cold in a VM and single runs. Windows and macOS builds were compiled, not run.
- All trees are synthetic. Real trees (deep `node_modules`, large flat directories, network or overlay
  filesystems) were not measured.

### Review fixes 2026-11-05

These fixes come from an independent review (`scratchpad/review/review.md`). They are covered by tests only; no
benchmark was re-run. The retained `out-realistic` dataset was regenerated once with the fixed binary and has the
same part MD5.

1. **`--overwrite` could delete the listed tree (High).** Fixed. The root is validated before `--out` is touched.
   `--out` equal to the root or an ancestor is refused, by path and by dev/inode, including through a symlinked
   alias. `--overwrite` needs an existing swath-local dataset and is refused for an `--out` inside the root. It
   removes only the dataset's own entries.
   Test: `TestOverwriteSafety` (`list $T/src --out $T --overwrite` and `--out $T/src/out --overwrite` both fail,
   and nothing is deleted).
2. **An `--out` inside the root listed itself (Medium).** Fixed. The out directory is located by identity and
   skipped (`SkipDirKeys`) in all five engines and all three `--order` modes.
   Test: `TestOutInsideRootIsSkipped`, with `--sort-buffer-rows 2` so spill runs are written inside the root
   during the walk.
3. **`--split-entries 1` looped forever (Low).** Fixed. The flag and `walk.Options` reject values that are not
   0 or >= 2, and the chunk is clamped to >= 1.
   Tests: `TestSplitEntriesValidation`, `TestSplitEntriesOneRejected`.
4. **A failed run leaked spill readers and left `_staging/` and `.tmp` parts (Low).** Fixed. `Sorter.Finish`
   closes every opened run reader on all paths. Both `Finish` paths remove `_staging/` and
   `data/*.parquet.tmp` on failure, and `_SUCCESS` is never written.
   Tests: `TestFailedFinishCleansUp` (duplicate key during a multi-run merge) and `TestStreamerRejectsDescendingKeys`.
5. **The path-based engines followed a directory swapped for a symlink (Low).** Mitigated, best-effort. After
   opening a child directory, `parallel-lstat` and `portable` re-check it with `Lstat` and `fstat` and skip it on
   a mismatch (`unsafe_dirs`). Test: `TestOpenDirCheckedRejectsSymlink`.

## Varve reader compatibility

From the same review. Varve at `2353ada4`, probed with its `swath.Open`/`List`/`Stream` API on
`out-realistic`. The dataset is **not yet readable by Varve**, for three reasons:

- **(a) Bucket and provider.** `Open` requires provider `s3` or `gcs` and a non-empty bucket equal to the
  manifest's `sourceBucket`. The spike writes `""`. This needs a Varve-side change: a local-dataset `Open` keyed
  on `schemaId`/`sourceKind` that does not take a provider or bucket. Writing a fake bucket name would only hide
  the mismatch.
- **(b) Run summary name.** Varve parses `_swath_summary.json` when it is present and rejected the spike's own
  schema. **Fixed here:** the report is now `_swath_local_summary.json`. Varve treats the Swath summary as
  optional, so it passes. `verify.py` checks the new name.
- **(c) S3 columns.** `columnsFor` requires `version_id`, `is_latest`, `is_delete_marker` and `row_type`.
  `decodeObject` and `Eligibility` then require `row_type == "OBJECT"`, null versions and no delete markers.
  There are two options. Varve can treat a schema without those columns as all current objects, gated on
  `schemaId == "swath-local/v0"` (recommended). Or swath-local can pad four constant S3 columns. **We did not
  pad**, because that would reintroduce the S3 semantics the local schema dropped.
- **Also:** `--fields names` writes a null `size`, and Varve rejects rows with a missing size. A names-tier
  dataset would still fail after (a) to (c), unless Varve accepts a null size for local datasets or swath-local
  refuses `names` when writing a dataset.

What did pass: the manifest shape and checks, part sizes and MD5s against the manifest, row counts, footer sort
stamps, `key`/`size`/`last_modified` types, and the key ColumnIndex/OffsetIndex policy.

## Skipped or not measured

- The native zstd codec and SQLite checkpointing (out of scope per the brief).
- Signed-off cold-cache medians: cold runs are single runs, except `huge` (3 each).
- `readlink` cost: unmeasurable with 80 symlinks.
