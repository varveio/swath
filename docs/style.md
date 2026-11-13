# Documentation style and terminology

This guide keeps swath's public writing consistent without flattening the technical
detail that makes the project useful.

## Names and voice

- Write the project name as **swath** in lowercase everywhere, including in headings and
  at the beginning of a sentence.
- Use code formatting only for literal executables, commands, flags, paths, package names,
  and artifact identifiers: `swath`, `swath list`, `--sort`, and
  `ghcr.io/varveio/swath`.
- Use plain swath when referring to the project or tool in ordinary prose.
- Use US English in first-party documentation: `behavior`, `color`, `analyze`.
- Prefer direct verbs—list, write, resume, query, sort, inspect—over abstract nouns such
  as surface, disposition, or seam in user-facing pages.
- State the action and consequence before implementation rationale.

Contributor specifications can use exact internal terminology when the term has a defined
contract. User guides should introduce an internal term only when the user must recognize
it in a command, file, report, or error message.

## Core terms

### Object, key, and prefix

Use **object** for an S3 object and **key** for its byte-exact name. Do not use **file** as
a synonym in technical claims. A broad introduction may say “objects—the files stored in
the bucket” once.

Use **prefix** for a key prefix. Directory or folder language is acceptable only when
describing a user interface or a dataset's physical output directory.

### Local trees: entry, file, and directory

A local directory tree is not an object store, and its words are its own. Its rows are
**entries**: **files** (every kind that is not a directory: regular files, symbolic links,
devices) and **directories**. Do not call a local entry an object in prose or on the
terminal. Machine-readable names (report fields, TSV columns, JSON members) stay the same
for every source so one script reads them all: there `objects` counts a local tree's files and
never a directory, and a field that means rows, directories included, says `rows` (`rows_emitted`;
`progress.json`'s `rows`). A directory's own size is not an object's: it is in no object byte count.
On the terminal a rate takes the rows' noun (`entries/s` for a local tree, `keys/s` for a store,
`rows/s` with `--all-versions`), what swath writes is a **part** (Parquet) or a **file** (a `.tsv` or
`.jsonl` output), and sizes are binary (`KiB`, `MiB`), as in the tables.

### Live listing, result, and snapshot

swath performs a **live listing** and publishes a **listing result** or **inventory**.

Do not call the result a point-in-time snapshot. Use this standard qualification when
consistency matters:

> swath publishes the complete result of the live listing it performed. It is not a
> point-in-time snapshot of a bucket that changes during the run.

`_SUCCESS` means the result was completely published; it does not add transactional
snapshot semantics.

### swath dataset and SeekParquet dataset

- **swath dataset** is the product term: a committed SeekParquet dataset that carries one of
  swath's registered schema ids and a valid `extensions.swath`. Use it in product
  documentation, CLI output and downstream integrations. On first use in a beginner or
  workflow page, define it:

  > A swath dataset is a directory of sorted Parquet parts and a `manifest.json` that
  > commits them, in the SeekParquet format, with one of swath's registered schema ids
  > and valid swath metadata ([the swath dataset profile](../go/docs/dataset.md)).

- **SeekParquet dataset** is the format term: any dataset that satisfies the SeekParquet
  sorted-key format, whoever wrote it. Use it when the format itself matters:
  interoperability, generic readers, conformance and diagnostics.
- **swath dataset profile** is swath's contract on top of the format: its schema ids, the
  `extensions.swath` metadata and what it records. The Go port's
  [`go/docs/dataset.md`](../go/docs/dataset.md) owns it.
- **Parquet part** is one Parquet file the manifest lists.
- **Legacy Java dataset** is the Java tool's format 1, with its own commit rules
  (`_SUCCESS`, the run summary). Use the term in migration documents and legacy
  diagnostics. The Java tool's own pages call it a **managed Parquet dataset**: a
  directory of Parquet parts plus swath's manifest, completion marker, run report, and
  temporary resume state.

### Supported and experimental

Use these labels consistently:

- **Supported:** expected to work within the documented scope and covered by the normal
  compatibility gates.
- **Experimental:** implemented but not covered to the same stability or conformance
  level; behavior may change.
- **Diagnostic:** intended for controlled measurement or investigation, not ordinary
  configuration.
- **Planned:** not implemented. Planned controls must not be presented as selectable
  current behavior.

Name the implementation and release when the support differs. The Java v0.3.2 release
supports S3 general-purpose buckets; its GCS XML access is experimental. Unreleased Go
also has native GCS, local/mounted filesystem and Azure Blob flat-namespace providers.
Use the Go CLI and provider docs for their exact limits.

## Evidence and performance claims

Every prominent measured run should identify as much of the following as is available:

- swath version and commit;
- date;
- target bucket or fixture;
- client machine and region;
- object-store region;
- full command;
- output mode;
- clock being reported; and
- machine-readable report or trace.

Label an observed number as an observation, not a portable limit or promise. Prefer:

> This run reached 655,346 keys/s on the stated machine and bucket.

over:

> swath lists at 655,346 keys/s.

When provenance is incomplete, say so rather than inferring it.

## Information ownership

Give each detailed fact one canonical owner. Other pages may summarize it in one or two
sentences and link to that owner.

| Fact | Owner |
| --- | --- |
| Product promise, fit and release status | root `README.md` |
| Go first successful workflow | `go/docs/getting-started.md` |
| Go commands and options | `go/docs/commands/` (options generated from the flag table) and installed Go CLI help |
| Go output formats and filters | `go/docs/output.md`, `go/docs/filters.md` |
| Go sources, credentials and endpoints | `go/docs/sources.md` |
| Go exit codes, run report and log events | `go/docs/reference/` |
| Go operations and recovery | `go/docs/running.md` |
| Go schemas and dataset layout | `go/docs/dataset.md` |
| Go engine and providers | `go/docs/internals/` |
| Go dated measurements | `go/docs/internals/measurements.md` |
| Option syntax, accepted values, and defaults | installed CLI help |
| Dated raw evidence | `docs/ops/dev/field-investigations.md` |

Do not copy a detailed table into several pages. Summarize and link.

## Editing discipline

- Put a successful copy-pasteable command before architecture, tuning, or benchmarks.
- Keep a decision-critical warning beside the action it affects.
- Remove stale rollout history, review narration, and rejected alternatives from current
  guides; preserve durable rationale in an ADR, issue, or dated investigation.
- Avoid repeated declarations that a page is exact, canonical, authoritative, honest, or
  deliberate. Demonstrate rigor with clear ownership, tests, evidence, and current links.
- Prefer one short caveat over several overlapping qualifications.
- When a concept needs several paragraphs merely to explain a surprising CLI behavior,
  consider changing the interface before adding more prose.
