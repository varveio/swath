# Local and mounted filesystems

A path lists a directory and everything below it, or a single file as its one row. Its rows are entries:
files (every kind that is not a directory: regular files, symbolic links, devices) and directories, each
directory's key ending in `/`. swath walks the directory through the operating system, so a mounted share
(NFS, SMB, a FUSE mount) lists as any directory does.

## The smallest command

```bash
swath du -d 1 ~/Downloads        # totals per directory, one level down
swath list ~/Downloads | head    # every entry, as TSV in a pipe
```

No account and no credentials: [Your first inventory](../first-inventory.md) walks through both and a saved
inventory.

## Permissions

swath needs to read the directories it walks and to stat their entries; it never opens a file's contents.
A root directory that does not exist or cannot be opened (permissions) stops the run before anything is
written, with exit 1 and one line on stderr (`swath: DIR: permission denied`).

Entries below the root that cannot be read (permissions) are left out: swath names up to five of them with
their cause on stderr and exits 3, its last line counting them; the report's `outcome.unreadable` and the
dataset's `coverage.unreadable` hold the count ([Troubleshooting](../troubleshooting.md#entries-could-not-be-read)).

## Options for this source

| Option | Effect |
|---|---|
| `--metadata names\|stat\|statx` | what is read per entry: names only (no size or time), `stat` (the default), or `statx`, which adds `birth_time` (Linux) |
| `--no-dirs` | omit the directories' rows (they are still walked) |
| `--no-link-target` | do not read symbolic links' targets |
| `--one-file-system` | do not walk into a directory on another device (a mount point), as `du -x` and `find -xdev`; its row is kept |

`swath du` also takes `--allocated` (allocated bytes instead of apparent sizes) and `-l`, `--count-links`
(count a hard-linked file under each of its names; by default it counts once), and `--max-inodes`, the most
hard-linked files it remembers ([Understand your results](../results.md)).

`--no-dirs` and `--one-file-system` mark a dataset as not the whole directory (`coverage.filters` holds
`no_dirs` or `one_filesystem`). `--one-file-system` and `--allocated` need each entry's stat: they are refused
with `--metadata names`.

A SOURCE that is a file is read as the same entry in a listing of its parent: at the `--metadata` tier, and a
symbolic link as the link (kind `SYMLINK`, its target in `link_target`), never followed. Its key is its name.

Recursive exclude globs can prevent descent into entire subtrees ([Filters](../filters.md#globs)); other row
filters do not stop the walk. On a filesystem `--workers` defaults to 4 per CPU, at most 64.

## Writing a dataset

A directory's dataset can be written to a local directory (`-o DIR`) or to a bucket (`-o s3://...`,
`gs://...` or `az://...` with `--work-dir DIR`; see that store's page).

The output may not be the listed directory itself or contain it. A new or empty directory inside the listed
one is accepted as the output: the walk skips it and the dataset records it in `coverage.exclusions`.
`--overwrite` of an output inside the listed directory is refused (exit 2): write the dataset to a new
directory outside it. A text file inside the listed directory is left out the same way, with its temporary
and the attempt's scratch directory beside it: the rows never list the run's own output, on a first run or a
repeat.

## Limits and known differences

- A local listing keeps no journal: an interrupted one is run again, and `swath resume` does not apply. On
  Windows, local listings work, text output and datasets alike, including a dataset written to a bucket.
- Names that are not valid UTF-8 are listed with their exact bytes: escaped in TSV and the table, as
  `key_base64` in JSONL ([Output](../output.md#text-rows)). A dataset's `key` column is binary for the same
  reason.
- A directory's row carries its own size (`st_size` on Unix); `swath du` counts directories separately and
  leaves their sizes out of its byte totals.
- A walk is not a snapshot: entries created, changed or removed while swath walks may or may not appear, and
  one removed during the walk is not counted as unreadable.
