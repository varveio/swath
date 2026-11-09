# Filesystem (`internal/source/localfs`)

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCALFS_ENGINE=portable`): `os.File.ReadDir` + `Lstat`, re-checking that an opened child directory is
  the one that was listed.
- **Large directories** (getdents engine). An entry holds its name and key as offsets into the directory's
  buffers, no pointer, so growing, sorting and chunking a huge directory costs no GC write barriers. After the
  first 1,024 entries the buffers are grown once to the count the directory's `st_size` suggests (report:
  `presized`). The sort compares keys after the directory's shared prefix; a directory of 32,768 entries or
  more is sorted in runs of at least 16,384, concurrently (up to GOMAXPROCS), then merged pairwise
  (`parallel_sorts`); names in a directory are distinct, so the order is the serial sort's. Chunks take the
  directory's buffers rather than copies. Offsets are 32-bit: a directory whose names, or keys, pass 4 GiB is
  skipped and counted in `read_errors`.
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
  client: 199,722 LOOKUPs to none, 5.85 s to 2.42 s ([measurements](../../measurements.md)). The heuristic is
  the kernel's and can change; the client's first reads are short (19, then 5 entries) and are not hinted, as
  hinting after them measured worse (3,277 LOOKUPs).
- **A count does not sort.** Nothing consumes a count's order, so the getdents engine skips each directory's
  sort (`unsorted_dirs` in the report) and the count runs with `order.Options.Unordered`
  (`TestUnsortedCountEqualsSortedListing`). Warm, 4 vCPU, paired: ~1M entries −10.6 % at the `names` tier and
  −5.5 % at `stat`; a 200,000-entry directory −19.7 % ([measurements](../../measurements.md)).
- **Tiers** (`--metadata`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `birth_time`).
- **Cancellation:** a huge directory stays cancellable. getdents checks the context every 64 reads, the sort
  is bracketed by checks (skipped with it in a count), the portable engine checks every 1,024 entries, and stat checks as before.
- Symlinks are never followed (`O_NOFOLLOW`). Names are bytes: one that is not valid UTF-8 is listed with
  its exact bytes, like any other. Mount points are crossed unless `--one-filesystem` (`Options.OneFilesystem`):
  `New` reads the root's device (refused under the `names` tier, or where the engine reads no device: the
  portable engine off Linux), and `emit` keeps a DIR row whose `dev` differs but hands on no child range for it
  (counter `other_device_dirs`); `Report` adds `one_filesystem` to the coverage's filters, so a dataset says it
  stops at the root's filesystem.
- A link target is at most 4,096 bytes (`link_target`'s `schema.Spec.MaxBytes`, PATH_MAX): the writer's
  admission prices it at that width, and a longer target stops the run with the typed `schema.ErrValueTooLong`, as
  a key over its cap does; a target within it is written byte for byte.
