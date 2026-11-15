---
title: Field guide
description: How swath lists a large bucket in parallel and still writes one sorted inventory, shown on a real listing of 37 million objects.
outline: [2, 2]
---

# How swath lists a bucket in parallel

A bucket's listing API hands out keys one page at a time, in order. swath lists a large bucket quickly by
dividing its keys into ranges that many workers list at once, and it still writes one sorted inventory. This
guide shows how, on one real run: the whole public bucket `s3://noaa-gestofs-pds`, listed into a sorted Parquet
inventory by 64 workers on a 4-vCPU cloud VM.

- **Objects:** 37,064,981
- **Time:** 60.9 s. Listing ended at 54 s, and publishing the parts took 7.6 s more.
- **Requests:** 39,016, none retried or throttled.
- **Starting ranges:** 257, found with 3 requests.
- **Splits:** 2,203.
- **Largest starting range:** 64.5% of the objects, split 1,834 times.

[The run report](https://swath.varve.io/runs/noaa-gestofs-pds/){target="_self"} draws every range and split of this run from its trace. The run was
measured once, with main at `d8fbe439` on 2026-11-15, and the bucket changes over time
([how it was measured](../../docs/internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15)).

## The constraint

A listing request returns up to 1,000 keys in byte order, and a token for the next page. It can also start
after any key you name, whether or not that key exists: `start-after` on S3, `startOffset` on GCS and
`startFrom` on Azure Blob. So the keys can be cut anywhere, for free, and each piece listed on its own.

What no request tells you is how many keys lie between two cut points. The only way to learn how much work a
piece holds is to list it, and that is the work. Dividing a listing is a guess, and the guess is corrected
while the listing runs.

The NOAA run shows why it has to be corrected. Its 257 starting ranges came from the bucket's folder names.
The median range held 42,080 objects; one held 23.9 million, 64.5% of the bucket.

## The range, and the one-key mistake

Every piece of work is a range `(lo, hi]`: the keys after `lo`, up to and including `hi`. Its first request
starts after `lo`, and keys past `hi` are dropped. The boundary key belongs to the range on its left: of two
neighbors `(a, b]` and `(b, c]`, only the left one lists `b`. Get that rule wrong in either direction and every
split loses one key or lists it twice, silently, once per split.

<figure class="swath-figure">
<svg viewBox="0 0 480 132" role="img" aria-labelledby="fg-split-title fg-split-desc">
  <title id="fg-split-title">A range split by its owner</title>
  <desc id="fg-split-desc">The range from lo to hi. The part up to the cursor c is listed. The rest is split at the pivot m into a lower child from c to m and an upper child from m to hi.</desc>
  <text class="label" x="74" y="22" text-anchor="middle">listed</text>
  <text class="label" x="226" y="22" text-anchor="middle">lower child</text>
  <text class="label" x="388" y="22" text-anchor="middle">upper child</text>
  <rect class="done" x="10" y="34" width="128" height="34" />
  <rect class="lower" x="138" y="34" width="176" height="34" />
  <rect class="upper" x="314" y="34" width="156" height="34" />
  <line class="tick" x1="10" y1="30" x2="10" y2="76" />
  <line class="tick" x1="138" y1="30" x2="138" y2="76" />
  <line class="tick" x1="314" y1="30" x2="314" y2="76" />
  <line class="tick" x1="470" y1="30" x2="470" y2="76" />
  <text class="mono" x="10" y="94" text-anchor="start">lo</text>
  <text class="mono" x="138" y="94" text-anchor="middle">c</text>
  <text class="mono" x="314" y="94" text-anchor="middle">m</text>
  <text class="mono" x="470" y="94" text-anchor="end">hi</text>
  <text class="mono" x="240" y="122" text-anchor="middle">(lo, c] ∪ (c, m] ∪ (m, hi]</text>
</svg>
<figcaption>A split of the range <code>(lo, hi]</code>. Its owner has listed up to <code>c</code> and hands
the rest on as two ranges that idle workers take. The three pieces cover the range, and none overlaps another.</figcaption>
</figure>

Keys are compared as bytes, unsigned, the order S3 and GCS list them in. Azure Blob lists in UTF-16 order,
and swath's Azure source serves its ranges in byte order over it. Every stage that handles a key checks the
order again: the range, the ordering stage, the part writer and the journal. A test splits ranges at random
points and checks that every key is listed exactly once
([the invariants](../../docs/internals/contracts.md#the-invariants-i1i12)).

## Starting ranges: the seed

Before it lists any object, swath lists the top of the bucket with the delimiter `/` to find its folders, and
cuts the bucket at them. A folder name is cheap and says where the structure is, though not how many objects
are behind it. A level with fewer folders than workers is listed one level deeper, every folder at once,
within fixed budgets: at most 4 levels, 64 requests and 6 round trips. swath keeps at most
`min(1000, 4 × workers)` cuts, evenly spaced. A bucket that fits on one page is listed by the seed's first
request alone.

In the NOAA run three requests listed 2,169 top-level folders, in under half a second. At 64 workers swath kept
256 of them as cuts, which makes 257 ranges.

## Splits: the owner hands off

Workers finish their ranges at different times. After each full page, a range's owner checks whether any
worker is idle with nothing queued. If one is, and its own range is among the largest left, it splits what it
has not listed yet. Its last listed key `c` becomes the start of a lower child `(c, m]`, and an upper child
`(m, hi]` takes the rest. The owner hands both on and stops, and idle workers take them.

Only the owner splits its range, at a page boundary, against the cursor it has. No other worker cuts into a
range while it is being listed, so a split cannot race the owner's next page, and no request is repeated.

A split is made only where it is worth its requests: each child pays at least one request of its own. swath
normally does not split a remainder under two pages, or where either child would be under one page, as
estimated from the range's last page.

The cut point `m` need not be a key that exists. swath places it from what the listing has already shown:

- by reflecting the cursor from where the chain of splits began, so each further split hands out more;
- by the density of keys on the last page, when that reaches further;
- at a folder boundary, found by a delimiter listing from the cursor when a range's splits keep landing in
  the folder it is already in.

Every cut point is valid UTF-8 and leaves out characters a store may refuse in a start bound.

The NOAA run made 2,203 splits, all of them when a worker was idle: 1,579 by reflection, 416 by density, 121
at folder boundaries and 87 at the midpoint between the cursor and the end of the range. The first came at
13 s, when workers began to run out of starting ranges. The largest starting range was split 1,834 times,
and all 64 workers listed pieces of it.
[The range engine](../../docs/internals/range-engine.md) has the rules in full.

## Where parallelism stops helping

- **Chains of splits.** A split waits for a page, so a chain of splits that each depend on the one before
  costs one page's round trip per link. A balanced tree of splits keeps those chains short.
- **A range that cannot be split.** When no usable key lies between a range's cursor and its end, its owner
  lists the rest alone, one page per round trip.
- **The store.** swath shares one adaptive request limit across its workers. It reduces concurrency under
  sustained throttling and recovers as the answers improve
  ([throttling](../../docs/watch-a-run.md#throttling)). In the NOAA run requests in flight averaged 52.3 of 64.
- **The output.** Rows leave through one sorted stream and the part writers, so a run that writes an
  inventory can wait on its output (next section).

The request count has a floor: one request per page of keys. The NOAA run made 39,016 requests for 37,064,981
objects, about 1.05 per 1,000. A billion objects cost at least a million requests
([cost](../../docs/size-a-run.md#cost)). A scheduled inventory (S3 Inventory, GCS Storage Insights, Azure Blob
inventory) can cost less, when a daily or weekly report is fresh enough
([when swath fits](../../docs/start-here.md#when-swath-fits)).

## One sorted stream

Ranges finish out of order, yet the inventory is sorted without a sort afterwards. Each range lists its keys
in order, and its children sort right after what it listed. The ranges therefore form a tree whose
depth-first walk is key order. One drainer walks that tree and hands rows to the output as they arrive at
the front of the walk, the frontier.

Rows listed ahead of the frontier wait in memory, up to a budget: `--sort-buffer`, by default 1/16 of memory
and at most 1 GiB. Past it they spill to disk. Each range writes its own file, already in order, coded against
the row before and compressed. When the drainer reaches that range it reads the file front to back, with no
merge and no index. Once listing has ended, on a machine with cores to spare, the rest of the stream is cut
into parts where the writer would cut them, and several parts are written at once.

Every row passes one check on its way out: its key must sort after the previous row's. A violation fails the
run before any output sees it. With the same build, the same listing gives byte-identical parts at any
number of workers, unless a part was closed by time ([ordering](../../docs/internals/ordering.md)).

In the NOAA run the drainer spent 41.3 s waiting for the frontier while the workers listed further ahead.
Spill wrote 795 MB to disk, 4.4 GB of rows before coding and compression, with at most 618 MB on disk at
once.

## Commit before emit, and resume

An inventory of an object store keeps a journal in its output directory. A part joins the dataset only after
its journal record is on disk. The manifest, `manifest.json`, is written last and names every part: a
directory without it holds no dataset.

The resume point is the last key of the last recorded part. `swath resume DIR` checks every part already
published against its record, size and SHA-256, then lists again after that key. It never writes a key twice
and never skips one. No worker's cursor and no split is saved, so a resume makes its own splits. Ranges that
spilled ahead of the output can be sealed as certified spill files, and a resume replays those instead of
listing them again. The tests stop a run at every crash point, including with a real SIGKILL, resume it, and
compare the result with an uninterrupted run, byte for byte
([stopping and resuming](../../docs/resume-and-refresh.md#stopping-and-resuming)).

A listing is not a point-in-time snapshot. swath publishes the complete result of the live listing it
performed: an object written or deleted while it runs may or may not be in it.

## Listing a bucket again: the seeded window

A refresh can start from the previous inventory instead of folder names:

```bash
swath list s3://bucket/logs/ -o /data/logs-2026-11-08 --seed-from /data/logs-2026-11-07
```

The previous dataset knows how many keys lie where, so swath cuts the bucket into tiles of 32 pages each, by
expected keys, and skips the delimiter seed and most early splits. Its requests approach one per page.

With those counts the run lists the bucket front to back. Tiles are taken in key order, and a new one starts
only while the tiles already taken fit a window at the output: 8 million rows, or less when the disk or the
sort buffer holds less. Rows listed ahead of the output, and the spill they need, stay near that window
instead of growing with the bucket. The splits stay adaptive: a region that grew since the previous run is
split as on any run ([seeding a refresh](../../docs/resume-and-refresh.md#seeding-a-refresh-from-the-previous-dataset)).

## Exact totals without sorting: the `swath du` view

`swath du` keeps no rows and needs no order across ranges. Its workers count rows as they arrive: there is no
drainer, no spill and no file. Bare `swath du` on a terminal shows the largest prefixes at any depth, and every
figure in it is exact.

It uses the order inside each range. The rows a range lists before it splits, or all of them if it does not,
are one run of contiguous keys. A prefix that a run enters after its first row and leaves before its last is
complete, so its total is final. The prefixes at a run's two ends are partial; they are summed by name across
runs and are complete at the end. Of the complete prefixes, the view keeps at most the 100 largest at each
depth; the others are dropped, and their bytes are already counted in their parent's. Each parent's other
children are folded into one counted `(N more)` line, so every level adds up
([the view](../../docs/commands/swath-du.md#the-view)).

A bare `swath du` of the same bucket later that day counted 37,071,317 objects in 52.2 s with 39,257 requests,
at a peak of 118 MiB of memory. The inventory run peaked at 1.51 GiB.

## Read the run

[The run report](https://swath.varve.io/runs/noaa-gestofs-pds/){target="_self"} is generated from the NOAA run's trace by
[`tools/explainer`](../../tools/explainer/README.md). It shows how the starting ranges turned out, the rate of
the run over time, the cut points it chose, a map of every range over the keys and time, a replay of the run,
and a ledger that rebuilds the ranges from the trace and checks that they cover the bucket once.

If you have a bucket whose keys this engine cannot divide well, [open an issue](https://github.com/varveio/swath/issues).
The shape of its key distribution is enough; names and contents are not needed.
