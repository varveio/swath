# Filesystem (`internal/source/localfs`)

A local or mounted directory supports dataset output and direct `du` summaries. A local
`list` prints text rows or writes a dataset; local scans do not resume. Mounted shares use the operating system's
filesystem API. [Object-store request controls](../../sources.md#object-store-requests) do not
apply to this provider; metadata and platform limits are described below.

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.
The walk is the [tree core](../tree-sources.md)'s: `tree.Walker` is the `source.Provider`, the provider is its
`Reader` (its engines below read, sort and stat a directory; `emit` is the walk decision per row: a
directory is walked, pruned by `--one-file-system` or omitted as the excluded output), chunks are the
walker's segments, and `files`, `dirs`, `bytes`, `skipped_dirs` and `other_device_dirs` are the walker's
counts. Listings and count reports are unchanged by the move.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCALFS_ENGINE=portable`): `os.File.ReadDir`, re-checking that an opened child directory is the one
  that was listed. Its stat tier differs by platform: on macOS one `fstatat` per entry relative to the open
  directory (every stat column and `birth_time`; `os.DirEntry.Info` is an `lstat` of the full path, which
  resolves the path again per entry), on Windows the directory enumeration's own size, times and attributes
  (no call per entry; no `uid`, `gid`, `nlink`, `inode`, `dev` or `allocated_bytes`), elsewhere `Lstat`.
- **Large directories** (getdents engine). An entry holds its name and key as offsets into the directory's
  buffers, no pointer, so growing, sorting and chunking a huge directory costs no GC write barriers. After the
  first 1,024 entries the buffers are grown once to the count the directory's `st_size` suggests (report:
  `presized`). The sort compares keys after the directory's shared prefix; a directory of 32,768 entries or
  more is sorted in runs of at least 16,384, concurrently (up to GOMAXPROCS), then merged pairwise
  (`parallel_sorts`); names in a directory are distinct, so the order is the serial sort's. Chunks take the
  directory's buffers rather than copies. Offsets are 32-bit: a listing skips a directory whose names, or
  keys, pass 4 GiB, counts it in `read_errors` and warns (a count reads in slabs and has no such limit).
- **Workers: 4 per CPU, at most 64.** Without `--workers` a local tree gets `min(64, 4 × GOMAXPROCS)`
  (GOMAXPROCS follows the CPU affinity and the cgroup quota), not the object stores' 64: a stat waits on the
  disk or the inode cache, not on a round trip. On a 1-CPU, 512 MiB cgroup (ext4 on a persistent disk, cold;
  issue #388; 2 rounds) 4 workers were fastest or tied on a nested tree of ~1M entries (28.3 s, 28.6 s at 64,
  44.6 s at 1) and on 32 directories of 60,000 files (19.9 s and 90 MiB, against 49.1 s and 261 MiB at 64). On
  one directory of 1M files 1 worker was fastest (119 s, 187 s at 4, 176 s at 64), every count there limited by
  the memory a whole directory holds. On a 4-vCPU machine (GOMAXPROCS 16), 1M files in one directory on XFS,
  ext4, btrfs and ZFS plateau from 4 to 16 workers, warm and cold. A count's processor boost still applies at
  the default (`count_procs`). Storage that waits per request (network filesystems) may want more `--workers`;
  nilfs2 wants fewer (#365).
- **A count runs with more processors.** A stat or a directory read that waits on the disk holds its Go
  processor until the scheduler hands it off, so one processor per CPU leaves CPUs idle on a cold tree. A count
  (`swath du`, `swath diag count`) raises GOMAXPROCS for the run, at most `--workers`, and restores it
  (`count_procs` in the report): to 4 per CPU at the `stat` and `statx` tiers, to 2 per CPU at the `names`
  tier, where more processors only add contention when the tree is warm. A count has no encoders, reorder
  storage or readahead to resize; runs that write output keep GOMAXPROCS as it is.
- **NFS: attributes with the names.** The Linux NFS client fetches attributes with a directory's names
  (READDIRPLUS) on its first read, and on a later read only if more than 8 of its entries were looked up since
  the previous one (`nfs_use_readdirplus` in `fs/nfs/dir.c`). A directory read whole before any stat would cost
  one LOOKUP per entry past the first page, so at a stat tier, after a read that fills the buffer, the getdents
  engine stats 9 of its entries before the next (`readdirplus_hints`); fdu, which stats each batch before
  reading the next, keeps READDIRPLUS on the same way. A 200,000-file directory over NFS 4.2, cold, Linux 7.0
  client: 199,722 LOOKUPs to none, 5.85 s to 2.42 s ([measurements](../measurements.md)). The heuristic is
  the kernel's and can change; the client's first reads are short (19, then 5 entries) and are not hinted, as
  hinting after them measured worse (3,277 LOOKUPs).
- **A count does not sort, nor hold a directory.** Nothing consumes a count's order, so both engines skip each
  directory's sort (`unsorted_dirs` in the report) and the count runs with `order.Options.Unordered`
  (`TestUnsortedCountEqualsSortedListing`). getdents engine, warm, 4 vCPU, paired: ~1M entries −10.6 % at the
  `names` tier and −5.5 % at `stat`; a 200,000-entry directory −19.7 % ([measurements](../measurements.md)).
  The getdents engine also reads a count's directory in slabs of about 8,192 entries (`count_slabs`): a slab
  that ends the directory within 4,096 entries is statted by its reader; otherwise its entries go to other
  workers as chunks, followed by a continuation range on the same fd that reads the next slab once the
  chunks are taken (`order.Count` pops last-pushed first and pushes a range's children in reverse). A
  directory then holds about one slab beyond the chunks being statted instead of all its entries (about
  140 MiB for 1M files, which made a count thrash the page cache in 512 MiB; issue #388), and keeps the
  chunks' parallel stats, which a single reader statting batch by batch (as `find` does) lost: 1.6× slower
  cold, 2× warm, on 1M files in one directory on a 4-CPU runner. A listing still reads a directory whole: it
  sorts it.
- **Tiers** (`--metadata`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `birth_time`).
- **Cancellation:** a huge directory stays cancellable. getdents checks the context every 64 reads, the sort
  is bracketed by checks (skipped with it in a count), the portable engine checks every 1,024 entries, and each stat loop checks the context.
- Symlinks are never followed (`O_NOFOLLOW`). Names are bytes: one that is not valid UTF-8 is listed with
  its exact bytes, like any other. Mount points are crossed unless `--one-file-system` (`Options.OneFilesystem`):
  `New` reads the root's device (refused under the `names` tier, or where the engine reads no device: the
  portable engine on Windows), and `emit` keeps a DIR row whose `dev` differs but hands on no child range for it
  (counter `other_device_dirs`); `Report` adds `one_filesystem` to the coverage's filters, so a dataset says it
  stops at the root's filesystem. `--no-dirs` (`Options.NoDirs`) walks every directory but writes no DIR row, and
  `Report` adds `no_dirs` to the filters likewise.
- **Recursive exclusion pruning.** Before workers start, `opened.Run` derives an immutable directory
  predicate from `registry.Env.Filter`. The filter package's glob compiler recognizes terminal `/**`
  segments without a second glob language. `Walker.SetPruneDirectories` installs it in the tree
  core, which retains the directory row for the ordinary row filter but submits no child for a matching
  directory; this applies to ordered listings,
  counts, native chunks/slabs and the portable engine. Includes, metadata filters, directory-row-only
  exclusions and regexes do not themselves stop descent. Automatic output-directory `Skip` exclusions
  still drop both the row and traversal.
  The report's `pruning` classifies configured rules: `reason` is `no_excludes`, `row_filters_only`, or
  `recursive_excludes`; `recursive_globs`, `row_only_globs` and `regex_rules` count the rules in each
  class, as given (repeated rules count separately). `pruned_dirs` counts child traversals prevented,
  once per directory even when rules overlap (`tree.Stats.Excluded`);
  reader decisions take precedence: `Omit` for output directories, then `Prune` for another device
  (`tree.Stats.Pruned` maps only to `other_device_dirs`);
  `dirs_read`, `skipped_dirs` and `other_device_dirs` keep their existing meanings. No excluded
  descendant counts or bytes are estimated. Original filter descriptions stay in `coverage.filters`;
  pruning adds neither skipped-entry errors nor output-directory exclusions.
- A link target is at most 4,096 bytes (`link_target`'s `schema.Spec.MaxBytes`, PATH_MAX): the writer's
  admission prices it at that width, and a longer target stops the run with the typed `schema.ErrValueTooLong`, as
  a key over its cap does; a target within it is written byte for byte.
