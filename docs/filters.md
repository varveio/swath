# Filters

```bash
swath list s3://bucket --include 'logs/2026/**' --exclude '*.tmp'
swath list /data --include '*.parquet' --modified-since 7d --min-size 1MiB
swath du gs://bucket --storage-class NEARLINE,COLDLINE
swath list s3://bucket --include-regex '^logs/2026/[0-9]{2}/app-'
```

Filters keep or drop rows. They apply to `swath list` and `swath du` alike, before rows are written or counted.

| Option | Keeps a row when |
|---|---|
| `--include GLOB` | the glob matches its key (repeatable: any of them) |
| `--exclude GLOB` | drops a row the glob matches (repeatable) |
| `--include-regex REGEX` | the regular expression matches anywhere in its full key (repeatable) |
| `--exclude-regex REGEX` | drops a row the expression matches anywhere in its full key (repeatable) |
| `--min-size SIZE`, `--max-size SIZE` | its size is within the bounds (inclusive) |
| `--modified-since TIME`, `--modified-until TIME` | its modification time is within the bounds (inclusive) |
| `--storage-class NAME[,NAME...]` | its storage class is one of those named (repeatable) |

A row is kept when no include is given or any include, glob or regex, matches it, and no exclude matches it:
an exclude always wins, whatever the order of the options. The other filters must all hold as well.

## Globs

A glob matches the key below SOURCE: for `s3://bucket/logs/` the part after `logs/`; for a local tree the path
below its root. The rows still show the full key.

| Pattern | Matches |
|---|---|
| `*.gz` (no `/`) | the last segment, at any depth: `a.gz`, `x/y/a.gz` |
| `logs/*.gz` (with a `/`) | from SOURCE's root: `logs/a.gz`, not `logs/x/a.gz`; a leading `/` only anchors (`/a.txt`) |
| `*`, `?` | any characters, or one, within a segment (never `/`) |
| `**` | any number of whole segments, as a segment of its own: `logs/**`, `**/tmp`, `a/**/b`; a trailing `**` is at least one, so `logs/**` is what lies below `logs/`, not the `logs/` row (as in gitignore) |
| `[abc]`, `[a-z]`, `[!a-z]` | one character (never `/`) |
| `{a,b}` | either (not nested) |
| `\c` | the character `c` |

A local directory's row has a key ending in `/` and is matched as its name: `--exclude raw` drops the row of
the directory `raw/` but lists what is below it; `--exclude '2025/**'` drops everything below `2025/` but keeps
its row, and `--exclude 2025/ --exclude '2025/**'` drops both; `--exclude '**/node_modules/**'` drops the
contents of every `node_modules` directory. On local and mounted filesystems, an exclude glob ending in
a recursive `/**` also prevents descent into matching directories. The directory's own row still passes
through the ordinary filters. Other globs and regexes filter rows without stopping traversal; include,
size and time filters never decide whether to enter a directory. A supported recursive exclude still
prunes when combined with regex filters or includes, because excludes always win.

An empty or malformed pattern is refused with exit 2.

## Regular expressions

`--include-regex` and `--exclude-regex` match anywhere in the full key, SOURCE's prefix included (a glob matches
the key below SOURCE): anchor with `^` to match from the start. The syntax is Go's RE2: backreferences
(`\1`), lookahead and lookbehind, possessive quantifiers and atomic groups are refused with exit 2, `\z` ends
the text (`\Z` is refused). RE2 runs in time linear in the key, so no pattern can stall a listing.

```text
$ swath du s3://bucket --include-regex '(a|b)\1'
swath: invalid --include-regex "(a|b)\\1" (Go RE2 syntax): error parsing regexp: invalid escape sequence: `\1`
```

## Sizes, times and storage classes

- `--min-size` and `--max-size` take a [SIZE](commands/swath.md#sizes-and-durations): `1M` and `1MiB` are
  binary, `1MB` decimal. They never match an S3 delete marker.
- `--modified-since` and `--modified-until` take a date (`2026-01-01`), a time (`2026-01-01T12:00:00`, with
  `Z` or an offset; UTC without one), or a duration before the run started (`7d`, `12h`), which the run
  records as the time it names, so a resume keeps the same bound. A row without a time is kept.
- `--storage-class` matches in any case. On S3 and GCS it is the storage class; on Azure Blob the access tier
  (`Hot`, `Cool`, `Cold`, `Archive`). A row without a class is in none and is dropped.

On a local tree these apply to every row, directories too, by their own size and time, as `find -size` and
`-newer` do: `--min-size 1M` drops the directories, whose own size is small.

A filter on a value the source does not read is refused before the run (exit 2):

```text
$ swath list /data --metadata names --min-size 1
swath: --min-size, --max-size, --modified-since and --modified-until read each entry's stat: not with --metadata names
$ swath list /data --storage-class STANDARD
swath: --storage-class: a local tree has no storage classes
```

## What narrows the listing

On S3 and GCS, when every include has a literal prefix (an anchored glob's text up to its first wildcard, a regex's `^literal`), swath lists only
the longest prefix common to them. `--include 'logs/2026/0*/**'` on `s3://bucket` lists `logs/2026/0` and
nothing else: the same rows, fewer requests. A glob without a `/`, or a regex without `^literal`, turns it
off; excludes never change it. Azure Blob lists the whole source and filters the rows. Object-store
exclusions do not change listing requests.

Local and mounted trees also avoid traversing subtrees covered by recursive exclude globs, as described
above. Their run report records the rule classification in `source.stats.pruning` and the directories
not entered in `source.stats.pruned_dirs`. Reading the parent and the excluded directory's metadata can
still occur; errors below it are no longer encountered. No count or byte total is inferred for unvisited
descendants.

The run report says what happened in `invocation.narrowing`: `narrowed` (and `invocation.source` names the
narrowed prefix), `include_without_prefix`, `no_longer_prefix` (the common prefix does not extend SOURCE's)
or `provider_lists_root`. `output.rows_filtered` counts observed rows the filters dropped; descendants
that were never visited are not counted in either `rows_fetched` or `rows_filtered`.

## Filtered datasets

A dataset written with filters says so: its manifest's `coverage.filters` lists one description per rule
(`include=…`, `include_regex=…`, `exclude=…`, `exclude_regex=…`, `min_size=…`, `max_size=…`,
`modified_after=…`, `modified_before=…`, `storage_class=…`), and `swath show` counts them. An application that
needs the whole source reads a non-empty `coverage.filters` as a partial listing ([dataset.md](dataset.md#the-swath-extension)).
`--no-dirs` and `--one-file-system` mark a local dataset the same way (`no_dirs`, `one_filesystem`).

The filters are part of the run's identity: `swath resume` keeps them and refuses others.
