# Concepts

The words the other pages use, what each one means, and what swath does and does not promise about it. Read this
once before running swath against a large source or building on its output. The commands are in
[getting-started.md](getting-started.md); the mechanisms behind each promise are in
[internals/contracts.md](internals/contracts.md).

## Source and scope

A **source** is what swath lists: a local or mounted directory (or a single file), `s3://BUCKET[/PREFIX]`,
`gs://BUCKET[/PREFIX]` or `az://ACCOUNT/CONTAINER[/PREFIX]` ([sources.md](sources.md)). The **scope** is the part
of it a run covers: the whole bucket or tree, or the keys that start with the prefix. A prefix is a string
prefix, not a directory: `s3://bucket/logs` covers `logs/` and `logs-old/`, `s3://bucket/logs/` only `logs/`.

swath reads listing metadata only: names, sizes, times, ETags and checksums, storage classes and the other
fields a listing returns. It never reads, writes or deletes the contents of what it lists. The permissions it
needs are the store's list permissions ([sources.md](sources.md)).

## Keys and order

Every row has a **key**: the object name in an object store, the path below the root in a local tree. Keys are
bytes, compared as unsigned bytes; nothing is normalized, case-folded or decoded. A local tree's keys need not be
UTF-8, so its `key` column is binary; an object store's is text.

swath writes rows in ascending key order, in every output: text rows, files and datasets. The order comes from
how the listing is divided (below), not from a sort afterwards.

## Ranges and workers

A large listing is slow because an object store returns one page of keys per request (up to 1,000 on S3 and GCS,
5,000 on Azure Blob), and each request needs the continuation of the one before. swath divides the key space into
**ranges**, each a half-open interval `(a, b]` of keys, and lists them concurrently with `--workers` workers
(64 by default). When a worker is idle, a busy range is split at a key between its cursor and its end, and the
two halves are listed independently. The ranges of a run never overlap and leave no gap, so every key in scope is
listed by exactly one range ([internals/range-engine.md](internals/range-engine.md)).

Splitting costs requests: a listing makes about one request per page of keys, plus the requests that find split
points and any retries. The run report counts them ([Cost](running.md#cost)). A local tree is walked directory by
directory instead, with its own workers.

## Three kinds of output

| Output | What it is | Durable while running? |
|---|---|---|
| **Text rows** (`swath list`, no `-o` or `-o NAME.tsv`/`.jsonl`) | a table, TSV or JSONL stream in key order | no: an interrupted run starts again |
| **Totals** (`swath du`) | counts and bytes per prefix down to `-d` levels | no: nothing is kept |
| **A dataset** (`swath list -o DIR`) | sorted Parquet parts and a manifest | an object-store run can be resumed |

Choose a dataset when a program will read the result, when the listing is long enough that an interruption
would hurt, or when you need to know how complete the result is ([output.md](output.md#choosing-an-output)).

## Datasets: parts and the manifest

A **dataset** is a directory (or a bucket prefix) that holds:

- `data/part-NNNNN.parquet`: the **parts**. Each part holds a contiguous run of keys; the parts are disjoint and
  in key order, so reading them in order reads every row in key order. A part closes when it reaches
  `--part-size` (64 MiB by default) or, if it holds rows, after `--part-interval` (30 seconds by default).
- `manifest.json`: the **manifest**. It lists every part with its row count, size, first and last key and
  SHA-256, and records what was listed and how completely (`extensions.swath`).

**The manifest is the commit.** swath writes it last, atomically, after every part it names is durable. A
directory without a manifest is not a dataset, whatever parts it holds. A reader reads the parts the manifest
lists, never a glob of `data/` ([dataset.md](dataset.md)).

The format is a [SeekParquet](https://github.com/varveio/seekparquet) dataset with swath's schemas and metadata:
any Parquet reader can read the parts, and a SeekParquet reader can also seek by key.

## Coverage

The manifest's **coverage** says how much of the scope the dataset holds:

- `complete`: the listing reached the end of its scope;
- `stop_reason`: why it stopped (`completed` for a finished run);
- `skipped_entries`: entries that could not be read (a local walk's permission errors, entries removed during
  the walk), left out of the rows;
- `filters`: the filters that left rows out ([filters.md](filters.md));
- `exclusions`: what was left out by design, such as an output directory inside the listed tree.

The dataset holds the whole source as listed only when `complete` is true, `stop_reason` is `completed`,
`skipped_entries` is 0 and `filters` is empty. A program that needs the whole source checks these before it
trusts the rows ([getting-started.md](getting-started.md#5-check-that-it-is-complete)).

## A listing is not a snapshot

No store offers an atomic listing of a bucket, and a filesystem walk takes time. An object created or deleted
while swath lists it may or may not appear. A dataset is the complete result of the listing swath performed,
with every key it saw exactly once, not the state of the source at one instant. The manifest records this as
`coverage.snapshot: false`, always.

## Runs, the journal and resume

A **run** is one invocation that writes one output. A dataset run of an object store keeps a **journal** in a
workspace beside the parts (`DIR/.swath/`, or `--work-dir` for a dataset in a bucket). Parts are published in key
order while the run lists, each recorded in the journal before it becomes visible.

If the run stops before its manifest is written (a signal, `--max-duration`, `--stall-timeout`, a full disk, a
crash or a kill), the published parts and the journal stay. `swath resume DIR` then:

1. verifies every published part against its journal record (size and SHA-256);
2. continues listing after the last published key, with the command the journal recorded;
3. writes the manifest when it reaches the end.

What a resume promises: no published key is listed into the dataset again and none is skipped. On a source that
did not change, the dataset is byte-identical to an uninterrupted run of the same build. Rows that were listed
but not yet in a published part are listed again, except where certified spill files let the resume replay them
([running.md](running.md#stopping-and-resuming)).

What it does not: it does not see changes made to the source before the last published key since the first
attempt, and it cannot continue a run whose journal or parts were altered, whose build wrote another journal
version, or whose environment resolves another endpoint or region. Local trees, text output and `swath du` keep
no journal: run them again. On Windows, journaled runs and `swath resume` are refused
([running.md](running.md#known-limits)).

## Memory, spill and disk

Rows reach swath out of order across ranges, but must leave in order. swath holds rows ahead of the slowest range
in memory up to `--sort-buffer`, then **spills** them to disk under the output's workspace (up to `--spill-budget`,
half the output filesystem's free space by default) and reads them back in order. `--sort-buffer` bounds that
buffer, not the process's memory; the run report records the peak RSS. A sorted run of a large bucket can spill
almost every row once: size the disk for it ([production.md](production.md#disk-sizing)).

## The run report and the outcome

Every run ends with an **outcome**: an exit code ([reference/exit-codes.md](reference/exit-codes.md)), a class
(`completed`, `fatal`, `interrupted`, `deadline`, ...) and a cause. `--report FILE` writes it with the run's counters, request
counts, resources and, on S3 and GCS, an estimated request cost at the provider's list price
([reference/run-report.md](reference/run-report.md)). Keep the report and `swath --version` with any dataset you
may need to investigate.

## Next

- [running.md](running.md): sizing, watching, throttling, stopping and resuming large runs.
- [dataset.md](dataset.md): the dataset's files, schemas and metadata in full.
- [troubleshooting.md](troubleshooting.md): by symptom and exit code.
