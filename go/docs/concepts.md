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

swath reads listing metadata: names, sizes, times, ETags and checksums, storage classes and the other fields a
listing returns. It does not read, change or delete the contents of what it lists. Listing needs the store's list
permission; a few options need more, such as reading Azure blob tags ([sources.md](sources.md)).

## Three kinds of output

| Output | What it is | Kept if the run stops? |
|---|---|---|
| **Text rows** (`swath list` with no `-o`, `-o -`, or `-o NAME.tsv`/`.jsonl`) | a table, TSV or JSONL stream | no: an interrupted run starts again |
| **Totals** (`swath du`) | counts and bytes per prefix down to `-d` levels | no: nothing is kept |
| **A dataset** (`swath list -o DIR`, or `-o` a bucket prefix with `--work-dir`) | sorted Parquet parts and a manifest | an object-store run keeps a journal and can be resumed; a local tree's cannot |

Choose a dataset when a program will read the result, when the listing is long enough that an interruption
would hurt, or when you need a record of how complete the result is ([output.md](output.md#choosing-an-output)).

With the default metadata and directory-row options, local text rows and datasets include each directory's own
size (`st_size` on Unix). `swath du` excludes directory sizes from its byte totals and counts directories
separately, so its total can differ from the sum of the listing's `size` column.

## Keys and order

Every row has a **key**: the object name in an object store, the path below the root in a local tree. Keys are
bytes, compared as unsigned bytes; nothing is normalized, case-folded or decoded. A local tree's keys need not be
UTF-8, so its `key` column is binary; an object store's is text.

Rows are written in ascending key order, in text rows and in datasets. A current-object listing has one row per
key; with `--all-versions` a key's rows (its versions, generations or snapshots) are consecutive, so keys repeat
and are non-decreasing ([dataset.md](dataset.md#keys-and-order)).

## Datasets: parts and the manifest

A **dataset** is a directory or a bucket prefix that holds:

- the **parts**, Parquet files under `data/`: `data/part-NNNNN.parquet` in a local directory,
  `data/RUN/ATTEMPT/part-NNNNN.parquet` in a bucket. Each part holds a contiguous run of keys; the parts are
  disjoint and in key order, so reading them in order reads every row in key order. Parts target `--part-size`
  (64 MiB by default); their encoded sizes vary. `--part-interval` (30 seconds by default) can close a part that
  holds rows early, and `--part-rotation-max-rows` adds a row-count cut. With `--all-versions` a key's rows stay
  in one part, so a cut waits for the key's last row.
- `manifest.json`, the **manifest**. It lists every part with its path, row count, size, first and last key and
  SHA-256, and records what was listed and how completely (`extensions.swath`).

**The manifest is the commit.** It is written after every part it names is durable: in a local directory by an
atomic rename and a directory sync, in a bucket by a create that succeeds only if no manifest is there. Without a
manifest there is no dataset, whatever parts exist. A reader takes the part paths from the manifest, never from a
glob of `data/`: a bucket's `data/` can hold parts of attempts the manifest does not name ([dataset.md](dataset.md)).

The format is a [SeekParquet](https://github.com/varveio/seekparquet) dataset with swath's schemas and metadata:
any Parquet reader can read the parts, and a SeekParquet reader can also seek by key.

## Coverage

The manifest's **coverage** says how much of the recorded scope the dataset holds:

- `complete`: the listing reached the end of its scope;
- `stop_reason`: why it stopped (`completed` for a finished run);
- `skipped_entries`: errors that kept entries or subtrees out of the result. Entries found missing during stat and
  failed symbolic-link target reads are excluded; a failed target read retains the symlink row. A directory that
  disappears before it can be opened still contributes an open error;
- `filters`: the filters that left rows out ([filters.md](filters.md));
- `exclusions`: what was left out by design, such as an output directory inside the listed tree.

When `complete` is true, `stop_reason` is `completed`, `skipped_entries` is 0 and `filters` is empty, the dataset
covers the recorded scope (the `source` and `scope` beside the coverage), minus its exclusions. A program that
needs a whole-source inventory must also check that the source and scope are the ones it expects, accept any
exclusions explicitly, and treat a missing coverage field as unknown, not as zero or empty
([getting-started.md](getting-started.md#5-check-that-it-is-complete)).

A nonzero exit does not mean there is no manifest: a run whose coverage is partial, a report that could not be
written after the commit, or a directory sync that failed after the manifest's rename can each leave a manifest
behind ([reference/exit-codes.md](reference/exit-codes.md)). Read the manifest, not the exit code, to learn what a
directory holds.

## A listing is not a snapshot

swath gives no source-wide point-in-time guarantee. A dataset records the rows its live listing observed and
selected, subject to its coverage: an object created or deleted while swath lists may or may not appear.
`coverage.snapshot` is always false.

## Ranges and workers

A large object-store listing is slow because a store returns one page of keys per request (up to 1,000 on S3 and
GCS, 5,000 on Azure Blob), and each request needs the continuation of the one before. swath divides the key space
into **ranges**, each a half-open interval `(a, b]` of keys, and lists them concurrently with `--workers` workers
(64 by default). When a worker is idle, the owner of a busy range may split it at a key between its cursor and
its end, and the two halves are listed independently. The ranges of a run never overlap and leave no gap, so every
key in scope is listed by exactly one range ([internals/range-engine.md](internals/range-engine.md)). The order
of the output comes from this division, not from a sort afterwards.

Splitting costs requests: a listing makes about one request per page of keys, plus the requests that place split
points and any retries. The run report counts them ([Cost](running.md#cost)). A local tree is walked directory by
directory instead, with its own workers.

## Runs, the journal and resume

A **run** is one invocation that writes one output. A dataset run of an object store keeps a **journal** in a
workspace: `DIR/.swath/` for a local directory, `--work-dir` for a dataset in a bucket. Parts are published in
key order while the run lists. In a local directory each part's journal record is synced before the part is
renamed into `data/`. In a bucket a part is uploaded before its record is journaled, so it can be visible on its
own before the run records it; it becomes part of a dataset only when the manifest names it.

If the run stops before its manifest is written (a signal, `--max-duration`, `--stall-timeout`, a full disk, a
crash or a kill), the published parts and the journal stay. `swath resume DIR` then:

1. checks every part the journal records: its size and SHA-256, and in a bucket also the object's recorded
   identity;
2. continues listing after the last recorded key, with the command the journal recorded;
3. writes the manifest when it reaches the end.

What a resume promises: no recorded key is listed into the dataset again and none in scope is skipped. On a
source that did not change, the resumed dataset holds the same rows as an uninterrupted run. Its parts are
byte-identical to that run's only with the same build and encoder path and without time-based cuts
(`--part-interval`), and the manifest's timestamps can differ. Rows listed but not yet in a recorded part are listed
again, except where certified spill files let the resume replay them ([running.md](running.md#stopping-and-resuming)).

What it does not: a resume does not see changes the source made, since the first attempt, to keys before the
last recorded key. Replayed spill keeps the earlier observation above that key too: an object added inside a
replayed interval can be missed, and one deleted since can remain. A resume cannot continue a run whose journal or
parts were altered, whose journal version this build does not support, or whose environment resolves another
endpoint or region. Local trees, text output and `swath du` keep no journal: run them again. On Windows, journaled
runs and `swath resume` are refused ([running.md](running.md#known-limits)).

## Memory, spill and disk

Rows reach swath out of order across ranges, but leave in order. swath holds rows ahead of the slowest range in
memory up to `--sort-buffer`, then **spills** them to disk and reads them back in order. A dataset run spills in
its local workspace, a text file run beside the file, and a run that writes to stdout under the temporary
directory (`TMPDIR`). The default cap, `--spill-budget`, is half of the spill filesystem's free space after a
reserve; at the cap or the reserve, listing waits instead of spilling.

`--sort-buffer` bounds that buffer, not the process's memory; the run report records the peak RSS. A local walk
also holds each directory's entries while it reads them, so one very large directory can take more memory than
`--sort-buffer`. A sorted run of a large bucket can spill almost every row once: size the disk for it
([production.md](production.md#disk-sizing)).

## The run report and the outcome

When swath settles a run, its **outcome** records an exit code ([reference/exit-codes.md](reference/exit-codes.md)),
a class (`completed`, `fatal`, `interrupted`, `deadline`, ...) and, when there is one, a cause. `--report FILE`
asks for the outcome with the run's counters, request counts and resources; a crash or a forced kill can leave no
report. The report can also estimate listing-request cost for AWS S3 and GCS at the provider's reference rate. S3
estimates require no endpoint override; GCS estimates require its default service endpoint
([reference/run-report.md](reference/run-report.md)). Keep the report and `swath --version`
with any dataset you may need to investigate.

## Next

- [running.md](running.md): sizing, watching, throttling, stopping and resuming large runs.
- [dataset.md](dataset.md): the dataset's files, schemas and metadata in full.
- [troubleshooting.md](troubleshooting.md): by symptom and exit code.
