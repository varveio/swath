# Filesystem (`internal/source/localfs`)

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCALFS_ENGINE=portable`): `os.File.ReadDir` + `Lstat`, re-checking that an opened child directory is
  the one that was listed.
- **Tiers** (`--metadata`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `birth_time`).
- **Cancellation:** a huge directory stays cancellable. getdents checks the context every 64 reads, the sort
  is bracketed by checks, the portable engine checks every 1,024 entries, and stat checks as before.
- Symlinks are never followed (`O_NOFOLLOW`). Names are bytes: one that is not valid UTF-8 is listed with
  its exact bytes, like any other. Mount points are crossed unless `--one-filesystem` (`Options.OneFilesystem`):
  `New` reads the root's device (refused under the `names` tier, or where the engine reads no device: the
  portable engine off Linux), and `emit` keeps a DIR row whose `dev` differs but hands on no child range for it
  (counter `other_device_dirs`); `Report` adds `one_filesystem` to the coverage's filters, so a dataset says it
  stops at the root's filesystem.
- A link target is at most 4,096 bytes (`link_target`'s `schema.Spec.MaxBytes`, PATH_MAX): the writer's
  admission prices it at that width, and a longer target stops the run with the typed `schema.ErrValueTooLong`, as
  a key over its cap does; a target within it is written byte for byte.
