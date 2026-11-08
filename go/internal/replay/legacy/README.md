# legacy (internal/replay/legacy)

The replay server's reader of the sorted Parquet listings that came before SeekParquet: the Java tool's
footer-stamped parts and the swath-dataset format 2 earlier Go builds wrote. SeekParquet datasets, which
this tool writes now, are read through `github.com/varveio/seekparquet/go` (`internal/replay/store`); this
package was the public `go/sortedparquet` until then.

An sstable-style reader over sorted Parquet listings. A lower bound is routed to a part and row group by the
decoded first key of every row group; the key column's ColumnIndex and OffsetIndex locate the page that can
hold it; only that page onward is decoded, a bounded number of rows at a time, in unsigned byte order of the
keys. It imports only the standard library and `github.com/parquet-go/parquet-go`.

## Layout

| Package | What it is |
|---|---|
| `legacy` | the format-neutral core: `Open` over parts given in key order, `Reader.Read` (a bounded range read), `Reader.Scan` (the same read handed to a sink column by column, the values aliasing the decoded pages, for a caller that keeps its own copy), `Reader.RollupRead` (a prefix and single-byte-delimiter skip-scan), `Successor`, `CommonPrefix`, the refusal vocabulary |
| `legacy/javaera` | the Java tool's sorted output: footer-stamped parts of the 13-column canonical schema |
| `legacy/format2` | manifest datasets (format 2): `manifest.json` lists the parts with exact base64 key bounds |
| `legacy/legacytest` | writes small synthetic Java-era parts and format 2 datasets for tests |

The core knows no stamp names. A profile decides whether a set of files is a sorted listing, then calls `Open`.

## Columns and schema ids

The core is column-agnostic: a row is its key plus the columns a read names, each by name with its typed
Parquet value (null when the row's part lacks the column), and a read decodes the pages of exactly the
columns it names. The profiles report the schema id the files declare (`format2`: the manifest's
`schema_id`; `javaera`: `javaera.SchemaID`, `swath/canonical`, for its 13 columns), and a consumer chooses
its projection, the columns it reads and how it maps them, by that declared id, never by which columns
happen to be present. `Options.Expect` only states the types a column must have wherever it exists.

## What a profile requires

**Java-era** (`javaera.Open`): the path is one part or a directory whose direct children named `*.parquet`
are the parts, in byte order of their names, without recursion. Refused, with the first failing reason:

| Reason | When |
|---|---|
| `no_stamp` | no parts, an unreadable footer, or a part without `swath.sort.order` = `key_bytes_unsigned,version_id_null_first,row_type_rank`, a known `swath.sort.mode` and an integer `swath.sort.format_version` |
| `unsupported_mode` | a part stamped `swath.sort.mode` other than `objects` |
| `unknown_format_version` | a part stamped `swath.sort.format_version` other than 1 |
| `incomplete_multifile` | the N parts' `swath.sort.file_index` (1 when absent) are not exactly 1..N, or not exactly one part, the one at N, carries `swath.sort.file_final=true` |
| `mixed_row_types` | a non-empty row group whose `row_type` footer statistics are not minimum = maximum = `OBJECT` |
| `sanity_failed` | the decoded row-group first keys do not ascend strictly across the parts |

Key ColumnIndex bounds of 61 bytes or more are treated as possibly truncated (the Java writer truncates to 64
or 1,024 bytes on a code point boundary and records neither).

**Format 2** (`format2.Open`): the directory holds `manifest.json` with `format` = `swath-dataset` and
`format_version` = 2. Any schema id is served if the parts have a required byte-array `key` column. Refused
with `invalid_manifest` (unreadable, another format, no schema id, a part path outside the dataset),
`unknown_format_version` (manifest or `swath.profile_version` not 2), `incomplete_multifile` (a listed part
missing, or its size or row count unlike its entry), `no_stamp` (no `swath.profile` = `swath-dataset`, a
`swath.schema_id` unlike the manifest's, no positive `swath.index_bound_bytes`, or a row group not recorded
as sorted by `key` ascending), `sanity_failed` (part bounds that do not ascend, or decoded first and last
keys unlike the manifest's exact bounds). Bounds of `swath.index_bound_bytes` bytes or more are treated as
possibly truncated.

Both: `unsupported_schema` (no required byte-array `key`, or a column of another type than `Expect` says)
and `no_page_index` (a non-empty row group whose key column lacks a ColumnIndex or an OffsetIndex).

## What the reader guarantees

- Routing uses each non-empty row group's first key decoded from its data page at `Open`, never statistics;
  those keys must ascend strictly across row groups and parts.
- Every key page it decodes holds one non-empty key per row the OffsetIndex gives it, strictly ascending,
  after every key decoded before it in the same read, and inside its ColumnIndex bounds. A truncated
  maximum is either raised above the last key or, when its bytes are all 0xFF, a prefix of it, so a bound
  that may be truncated only bounds the keys below its successor; a page is skipped only when that
  conservative bound rules it out. A violation is an error wrapping `ErrCorrupt`; keys out of order inside
  one row group are a `*DisorderError` naming the part, row group, row and key, whose reason (`ReasonOf`)
  is `row_group_disorder`, and so are key page bounds that descend from one page to the next (row -1).
- Values come back as stored: a null size or time is null. Rendering policy belongs to the caller.
- `RollupRead` returns the entries a one-key-at-a-time walk over `Read` would: each common prefix costs one
  routing hop and at most one page-index landing past it, and a row group whose first key and the next
  group's first key share a common prefix is skipped without a read.
- A `Reader` is safe for concurrent use; parsed footers and page indexes are shared and loaded once. Both
  profiles read parts through a `FileCache`: every open, the first included, waits for a slot under the
  cache's bound (`DefaultMaxOpenFiles`: half the soft descriptor limit, at most 4,096), an idle descriptor
  is closed (least recently used first) when another part needs one, and an evicted part is reopened by
  path and must be the same file (`os.SameFile`). Opens are non-blocking on unix and anything but a
  regular file is refused, so a FIFO put at a part's path cannot stall a read. A read waiting for a
  descriptor ends with its context, and `Close` fails such waits with `ErrClosed` at once.

## What it does not check

Keys it never decodes; value columns beyond their Parquet type; part MD5 digests and manifest coverage
(a dataset that stopped early is served as what it holds); that file contents stay unchanged after `Open`
(a part replaced by another file is refused when it is reopened, not when it is rewritten in place).

The key ColumnIndex's declared `BoundaryOrder` is not read: the reader checks the page bounds themselves,
on every read path. The Java replay server instead refuses a row group whose key index does not declare
`ASCENDING`, and only on its delimiter (rollup) path, when it reads that group; so a part whose bounds
ascend but whose index declares `UNORDERED` (parquet-go's default for a one-page index) is served by both
on plain listings and refused by the Java server alone on delimiter listings. The Java writer and the Go
writer both declare `ASCENDING` for sorted output, as `legacytest` does.
