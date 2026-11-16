---
title: Field guide
description: How swath lists a large bucket in parallel and still writes one sorted inventory, shown on a real listing of 37 million objects.
outline: [2, 2]
---

<script setup>
import MiniModel from './MiniModel.vue'
import RunSeeds from './RunSeeds.vue'
import RunInFlight from './RunInFlight.vue'
</script>

# How swath lists a bucket in parallel

A bucket's listing API hands out keys one page at a time, in order. swath lists a large bucket quickly by
dividing its keys into ranges that many workers list at once, and it still writes one sorted inventory. This
guide shows how, on one real run: the whole public bucket `s3://noaa-gestofs-pds`, listed into a sorted Parquet
inventory by 64 workers on a 4-vCPU cloud VM.

- **Objects:** 37,064,981
- **Time:** 60.9 s. The last range ended at 53.1 s; draining, publication and finalization continued for about
  7.8 s.
- **Requests:** 39,016, none retried or throttled.
- **Starting ranges:** 257, found with 3 requests.
- **Splits:** 2,203.
- **Largest starting range:** 64.5% of the objects, split 1,834 times.

[The run report](https://swath.varve.io/runs/noaa-gestofs-pds/){target="_self"} draws every range and split of this run from its trace. The run was
measured once, with main at `d8fbe439` on 2026-11-15, and the bucket changes over time
([how it was measured](../../docs/internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15)).

## The mechanism in miniature

Before the details, the whole mechanism at a size you can watch. A seed cuts the key space into starting
ranges, 64 workers list them, the owner of a heavy range splits it for workers that are idle, and the ordering
stage stitches what they list into one sorted stream. The stream is written as parts, and each part is recorded
before it joins the dataset. The sections below take these steps one at a time.

<MiniModel />

## The constraint

A listing request returns one page of keys and a token for the next page. The keys come in order: on S3 and
GCS in byte order, comparing keys byte by byte as unsigned numbers. A request can also start after a key you
name, whether or not that key exists: `start-after` on S3, `startOffset` on GCS and `startFrom` on Azure Blob.
The providers differ in the details, and swath adapts their page sizes (up to 1,000 keys on S3 and GCS, up to
5,000 on Azure Blob), their ordering and whether a start bound is inclusive or exclusive. Ranges can start at
admissible synthetic bounds, without finding an existing object: any string the store accepts as a start bound
will do. So the keys can be cut into pieces, and each piece listed on its own.

<figure class="swath-figure">
<svg viewBox="0 0 480 192" role="img" aria-labelledby="fg-pages-title fg-pages-desc">
  <title id="fg-pages-title">Pages follow one another; a range can start at any bound</title>
  <desc id="fg-pages-desc">Above, three pages of a listing, each request starting after the last key of the page before. Below, the key space with two start bounds, a and b, named without listing anything. How many keys lie between them is unknown until the range is listed.</desc>
  <defs>
    <marker id="fg-arrow-acc" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path class="acc" d="M0 0 L8 4 L0 8 z" /></marker>
    <marker id="fg-arrow-sig" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path class="sig" d="M0 0 L8 4 L0 8 z" /></marker>
  </defs>
  <text x="0" y="16">A listing reads one page after another</text>
  <rect class="lower" x="0" y="28" width="104" height="28" />
  <rect class="lower" x="116" y="28" width="104" height="28" />
  <rect class="lower" x="232" y="28" width="104" height="28" />
  <text class="sm" x="52" y="47" text-anchor="middle">page 1</text>
  <text class="sm" x="168" y="47" text-anchor="middle">page 2</text>
  <text class="sm" x="284" y="47" text-anchor="middle">page 3</text>
  <text class="label" x="350" y="47">…</text>
  <path class="s-acc" d="M96 58 Q110 72 120 60" marker-end="url(#fg-arrow-acc)" />
  <path class="s-acc" d="M212 58 Q226 72 236 60" marker-end="url(#fg-arrow-acc)" />
  <text class="sm label" x="0" y="88">each request starts after the last key of the one before</text>
  <text x="0" y="122">A range can start after any bound the store accepts</text>
  <line class="s-ink" x1="0" y1="152" x2="480" y2="152" />
  <line class="s-sig" x1="120" y1="140" x2="120" y2="164" stroke-width="2" />
  <line class="s-sig" x1="360" y1="140" x2="360" y2="164" stroke-width="2" />
  <text class="mono t-sig" x="120" y="136" text-anchor="middle">a</text>
  <text class="mono t-sig" x="360" y="136" text-anchor="middle">b</text>
  <line class="s-sig" x1="128" y1="172" x2="352" y2="172" marker-start="url(#fg-arrow-sig)" marker-end="url(#fg-arrow-sig)" />
  <text class="sm t-sig" x="240" y="189" text-anchor="middle">how many keys lie between? no request says</text>
</svg>
<figcaption>Following pages is cheap but takes one round trip at a time. Naming a bound costs nothing, but
nothing says how much work lies behind it.</figcaption>
</figure>

What no request tells you is how many keys lie between two cut points. The only way to learn how much work a
piece holds is to list it, and that is the work. Dividing a listing is a guess, and the guess is corrected
while the listing runs.

The NOAA run shows why it has to be corrected. Its 257 starting ranges came from the bucket's folder names.
The median range held 42,080 objects; one held 23.9 million, 64.5% of the bucket.

<RunSeeds />

## The range, and the one-key mistake

Every piece of work is a range `(lo, hi]`: the keys after `lo`, up to and including `hi`. Its first request
starts after `lo`, and keys past `hi` are dropped. The boundary key belongs to the range on its left: of two
neighbors `(a, b]` and `(b, c]`, only the left one lists `b`. A boundary need not name an object, but when it
does, the rule decides who lists it. Incorrect boundary ownership can omit or duplicate an existing boundary
key; the order guard, the check every row passes on its way out ([below](#one-sorted-stream)), rejects
duplicates.

<figure class="swath-figure">
<svg viewBox="0 0 480 222" role="img" aria-labelledby="fg-bound-title fg-bound-desc">
  <title id="fg-bound-title">Three ways to own a boundary key</title>
  <desc id="fg-bound-desc">Five keys split at the key b. If both ranges exclude b, nobody lists it. If both include it, it is listed twice. swath's left range (a, b] includes it and the right range (b, c] starts after it, so b is listed once.</desc>
  <line class="outline dashed" x1="290" y1="22" x2="290" y2="198" />
  <text class="mono" x="290" y="16" text-anchor="middle">b</text>
  <text class="t-alert" x="0" y="44">gap</text>
  <text class="sm label" x="0" y="60">both exclude b</text>
  <rect class="s-alert" x="120" y="34" width="162" height="24" />
  <rect class="s-alert" x="298" y="34" width="170" height="24" />
  <g fill="currentColor"><circle cx="160" cy="46" r="3.5" /><circle cx="225" cy="46" r="3.5" /><circle cx="355" cy="46" r="3.5" /><circle cx="420" cy="46" r="3.5" /></g>
  <path class="s-alert" d="M284 40 L296 52 M296 40 L284 52" stroke-width="2" />
  <text class="sm t-alert" x="290" y="76" text-anchor="middle">b listed by neither</text>
  <text class="t-alert" x="0" y="112">overlap</text>
  <text class="sm label" x="0" y="128">both include b</text>
  <rect class="s-alert" x="120" y="102" width="178" height="24" />
  <rect class="s-alert" x="282" y="102" width="186" height="24" />
  <g fill="currentColor"><circle cx="160" cy="114" r="3.5" /><circle cx="225" cy="114" r="3.5" /><circle cx="285" cy="114" r="3.5" /><circle cx="295" cy="114" r="3.5" /><circle cx="355" cy="114" r="3.5" /><circle cx="420" cy="114" r="3.5" /></g>
  <text class="sm t-alert" x="290" y="144" text-anchor="middle">b listed twice</text>
  <text class="t-acc" x="0" y="180">swath</text>
  <text class="sm label" x="0" y="196">the left owns b</text>
  <rect class="lower" x="120" y="170" width="170" height="24" />
  <rect class="lower" x="290" y="170" width="178" height="24" />
  <g class="acc"><circle cx="160" cy="182" r="3.5" /><circle cx="225" cy="182" r="3.5" /><circle cx="284" cy="182" r="4.5" /><circle cx="355" cy="182" r="3.5" /><circle cx="420" cy="182" r="3.5" /></g>
  <text class="sm t-acc mono" x="205" y="214" text-anchor="middle">(a, b]</text>
  <text class="sm t-acc mono" x="379" y="214" text-anchor="middle">(b, c]</text>
</svg>
<figcaption>The left range lists keys up to and including <code>b</code>; the right one starts after
<code>b</code>. Together the ranges of a run cover the keys once, so workers run side by side without
removing duplicates afterwards.</figcaption>
</figure>

Keys are compared as bytes, unsigned, the order S3 and GCS list them in. Azure Blob lists in UTF-16 order,
and swath's Azure source serves its ranges in byte order over it. Every stage that handles a key checks the
order again: the range, the ordering stage, the part writer and the journal. A test splits ranges at random
points and checks that every key is listed exactly once
([the invariants](../../docs/internals/contracts.md#the-invariants-i1i12)).

## Starting ranges: the seed

A delimiter listing asks for the folder names under a level, the common prefixes up to the next `/`, instead
of every key below them. A folder name is cheap and says where the structure is, though not how many objects
are behind it. The seed concurrently requests delimiter listings and the first data page, descends at most four
levels below the top, and emits that data page if complete: a bucket that fits on one page is listed in one
round trip. Otherwise the page is discarded and swath cuts the bucket at its folders. A level with fewer
folders than workers is listed one level deeper, every folder at once, within fixed budgets: at most four levels
below the top, 64 requests and 6 round trips. swath keeps at most `min(1000, 4 × workers)` cuts, evenly
spaced.

<figure class="swath-figure">
<svg viewBox="0 0 480 220" role="img" aria-labelledby="fg-seed-title fg-seed-desc">
  <title id="fg-seed-title">The seed cuts at folder names</title>
  <desc id="fg-seed-desc">One wave of two requests: a delimiter listing of the top level, which returns folder names such as _para3/ and estofs.20210102/, and the first data page, kept only if the bucket fits on it. The folder names become cuts on the key space, which make the starting ranges.</desc>
  <defs>
    <marker id="fg-arrow-ink" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto"><path fill="currentColor" d="M0 0 L8 4 L0 8 z" /></marker>
  </defs>
  <text x="0" y="16">One wave of requests, before any range is listed</text>
  <rect class="lower" x="0" y="28" width="228" height="46" />
  <text x="12" y="48">delimiter listing</text>
  <text class="sm label" x="12" y="66">folder names of the top level</text>
  <rect class="outline" x="252" y="28" width="228" height="46" />
  <text x="264" y="48">first data page</text>
  <text class="sm label" x="264" y="66">kept if the bucket fits on it</text>
  <text class="sm mono" x="12" y="96">_para3/</text>
  <text class="sm mono" x="12" y="114">estofs.20210102/</text>
  <text class="sm mono" x="12" y="132">…</text>
  <text class="sm mono" x="12" y="150">stofs_2d_glo.20261108/</text>
  <text class="sm label" x="252" y="114">2,169 folders in the NOAA run</text>
  <line class="s-ink" x1="0" y1="186" x2="480" y2="186" />
  <g class="s-acc" stroke-width="2">
    <line x1="18" y1="176" x2="18" y2="196" /><line x1="44" y1="176" x2="44" y2="196" /><line x1="70" y1="176" x2="70" y2="196" /><line x1="96" y1="176" x2="96" y2="196" /><line x1="122" y1="176" x2="122" y2="196" /><line x1="148" y1="176" x2="148" y2="196" /><line x1="174" y1="176" x2="174" y2="196" /><line x1="200" y1="176" x2="200" y2="196" /><line x1="226" y1="176" x2="226" y2="196" /><line x1="252" y1="176" x2="252" y2="196" /><line x1="278" y1="176" x2="278" y2="196" /><line x1="304" y1="176" x2="304" y2="196" /><line x1="330" y1="176" x2="330" y2="196" /><line x1="356" y1="176" x2="356" y2="196" /><line x1="382" y1="176" x2="382" y2="196" /><line x1="408" y1="176" x2="408" y2="196" /><line x1="434" y1="176" x2="434" y2="196" /><line x1="460" y1="176" x2="460" y2="196" />
  </g>
  <path class="s-ink" d="M200 146 L230 170" marker-end="url(#fg-arrow-ink)" />
  <text class="sm label" x="240" y="164">cuts, evenly spaced</text>
  <text class="sm label" x="0" y="212">256 cuts kept: 257 starting ranges</text>
</svg>
<figcaption>The seed's first wave. A narrow top level would add waves, one level down each, within the
budgets.</figcaption>
</figure>

In the NOAA run the seed listed 2,169 top-level folders with 3 requests, in under half a second. At 64 workers
swath kept 256 of them as cuts, which makes 257 ranges.

## Splits: the owner hands off

Workers finish their ranges at different times. After each full page, a range's owner checks whether any
worker is idle with nothing queued. If one is, and its own range is among the largest left, it splits what it
has not listed yet. Its last listed key `c` becomes the start of a lower child `(c, m]`, and an upper child
`(m, hi]` takes the rest. The owner hands both on and stops, and idle workers take them.

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

Only the owner splits its range, at a page boundary, against the cursor it has. No other worker cuts into a
range while it is being listed, so a split cannot race the owner's next page, and no request is repeated.

A split is made only where it is worth its requests: each child pays at least one request of its own. swath
normally does not split a remainder under two pages, or where either child would be under one page, as
estimated from the range's last page.

The cut point `m` need not be a key that exists. swath places it from what the listing has already shown:

- by reflecting the cursor from the chain's anchor, where the chain of splits began: `m` lies about as far past
  the cursor as the cursor lies past the anchor, so each further split hands out more;
- by the density of keys on the last page, when that reaches further: how closely the page's keys are spaced
  says how far a few pages' worth of keys reach;
- at a folder boundary, found by a delimiter listing from the cursor when a range's splits keep landing in
  the folder it is already in.

<figure class="swath-figure">
<svg viewBox="0 0 480 196" role="img" aria-labelledby="fg-reflect-title fg-reflect-desc">
  <title id="fg-reflect-title">Three splits along one chain, each reflected from the anchor</title>
  <desc id="fg-reflect-desc">Three rows, one per split. In each, the range's owner has listed up to its cursor c, and the cut point m lies as far past c as c lies past the anchor a. The lower child goes to an idle worker; the upper child carries the chain on. The piece handed out doubles from one split to the next.</desc>
  <line class="outline dashed" x1="20" y1="24" x2="20" y2="168" />
  <text class="sm mono t-sig" x="20" y="16" text-anchor="middle">a</text>
  <rect class="done" x="20" y="28" width="24" height="22" />
  <rect class="lower" x="44" y="28" width="24" height="22" />
  <rect class="upper" x="68" y="28" width="402" height="22" />
  <text class="sm mono" x="44" y="66" text-anchor="middle">c</text>
  <text class="sm mono" x="68" y="66" text-anchor="middle">m</text>
  <rect class="done" x="68" y="80" width="8" height="22" />
  <rect class="lower" x="76" y="80" width="56" height="22" />
  <rect class="upper" x="132" y="80" width="338" height="22" />
  <text class="sm mono" x="76" y="118" text-anchor="middle">c</text>
  <text class="sm mono" x="132" y="118" text-anchor="middle">m</text>
  <rect class="done" x="132" y="132" width="8" height="22" />
  <rect class="lower" x="140" y="132" width="120" height="22" />
  <rect class="upper" x="260" y="132" width="210" height="22" />
  <text class="sm mono" x="140" y="170" text-anchor="middle">c</text>
  <text class="sm mono" x="260" y="170" text-anchor="middle">m</text>
  <text class="sm label" x="470" y="190" text-anchor="end">each lower child goes to an idle worker</text>
</svg>
<figcaption>Reflection along one chain of splits. The upper child carries the anchor <code>a</code>, so its
next split, by whichever worker lists it, hands out about twice as much: a few splits reach across a large
remainder instead of peeling a page at a time.</figcaption>
</figure>

Every cut point is valid UTF-8 and leaves out characters a store may refuse in a start bound.

The NOAA run made 2,203 splits, all of them when a worker was idle: 1,579 by reflection, 416 by density, 121
at folder boundaries and 87 at the midpoint between the cursor and the end of the range. The first came at
13 s, when workers began to run out of starting ranges. The largest starting range was split 1,834 times,
and all 64 workers listed pieces of it.
[The range engine](../../docs/internals/range-engine.md) has the rules in full.

<RunInFlight />

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

<figure class="swath-figure">
<svg viewBox="0 0 480 160" role="img" aria-labelledby="fg-chain-title fg-chain-desc">
  <title id="fg-chain-title">A chain of splits against a balanced tree</title>
  <desc id="fg-chain-desc">Two ranges over three round trips. On the left each split peels a small piece off the front, so after three round trips there are four pieces and most of the range is still on one worker. On the right each split halves its range, so after three round trips there are eight pieces of equal size.</desc>
  <text x="0" y="16">A chain</text>
  <text x="260" y="16">A balanced tree</text>
  <rect class="lower" x="0" y="28" width="30" height="22" /><rect class="upper" x="30" y="28" width="190" height="22" />
  <rect class="lower" x="0" y="60" width="30" height="22" /><rect class="lower" x="30" y="60" width="30" height="22" /><rect class="upper" x="60" y="60" width="160" height="22" />
  <rect class="lower" x="0" y="92" width="30" height="22" /><rect class="lower" x="30" y="92" width="30" height="22" /><rect class="lower" x="60" y="92" width="30" height="22" /><rect class="upper" x="90" y="92" width="130" height="22" />
  <rect class="lower" x="260" y="28" width="110" height="22" /><rect class="upper" x="370" y="28" width="110" height="22" />
  <rect class="lower" x="260" y="60" width="55" height="22" /><rect class="upper" x="315" y="60" width="55" height="22" /><rect class="lower" x="370" y="60" width="55" height="22" /><rect class="upper" x="425" y="60" width="55" height="22" />
  <rect class="lower" x="260" y="92" width="27.5" height="22" /><rect class="upper" x="287.5" y="92" width="27.5" height="22" /><rect class="lower" x="315" y="92" width="27.5" height="22" /><rect class="upper" x="342.5" y="92" width="27.5" height="22" /><rect class="lower" x="370" y="92" width="27.5" height="22" /><rect class="upper" x="397.5" y="92" width="27.5" height="22" /><rect class="lower" x="425" y="92" width="27.5" height="22" /><rect class="upper" x="452.5" y="92" width="27.5" height="22" />
  <text class="sm label" x="0" y="136">4 pieces; most of it on one worker</text>
  <text class="sm label" x="260" y="136">8 pieces of equal size</text>
  <text class="sm label" x="0" y="156">one row per round trip</text>
</svg>
<figcaption>Splits wait for pages, so the depth of the tree of splits is a floor under the time. A cut point
that peels a little off the front makes a chain; one that hands out half or more keeps the tree shallow.</figcaption>
</figure>

The request count has a floor: one request per page of keys. The NOAA run made 39,016 requests for 37,064,981
objects, about 1.05 per 1,000. On S3, a billion objects require at least a million listing requests
([cost](../../docs/size-a-run.md#cost)). A scheduled inventory (S3 Inventory, GCS Storage Insights, Azure Blob
inventory) can cost less, when a daily or weekly report is fresh enough
([when swath fits](../../docs/start-here.md#when-swath-fits)).

## One sorted stream

The inventory is written as Parquet parts: files of rows in key order, each starting after the last key of the
one before. Ranges finish out of order, yet the inventory is sorted without a sort afterwards. Each range lists
its keys in order, and its children sort right after what it listed. The ranges therefore form a tree whose
depth-first walk is key order. One drainer walks that tree and hands rows to the output as they arrive at the
front of the walk, the frontier.

<figure class="swath-figure">
<svg viewBox="0 0 480 214" role="img" aria-labelledby="fg-stream-title fg-stream-desc">
  <title id="fg-stream-title">The tree of ranges and the frontier</title>
  <desc id="fg-stream-desc">A range has listed its first part and split; its upper child has split again. Walked depth first, left to right, the pieces are in key order. The stream below has every row up to the frontier, inside the lower child still being listed. Rows already listed further right wait, in memory and then on disk.</desc>
  <rect class="done" x="0" y="8" width="120" height="22" />
  <rect class="outline dashed" x="120" y="8" width="360" height="22" />
  <path class="outline" d="M200 30 L200 52 M375 30 L375 52" />
  <rect class="done" x="120" y="52" width="80" height="22" />
  <rect class="outline" x="200" y="52" width="80" height="22" />
  <rect class="upper" x="280" y="52" width="50" height="22" />
  <rect class="outline dashed" x="330" y="52" width="150" height="22" />
  <path class="outline" d="M370 74 L370 96 M445 74 L445 96" />
  <rect class="upper" x="330" y="96" width="80" height="22" />
  <rect class="upper" x="410" y="96" width="30" height="22" />
  <rect class="outline" x="440" y="96" width="40" height="22" />
  <line class="s-ink" x1="200" y1="46" x2="200" y2="176" stroke-width="2" />
  <text class="sm" x="196" y="150" text-anchor="end">frontier</text>
  <rect class="acc" x="0" y="160" width="200" height="16" />
  <rect class="upper" x="280" y="160" width="160" height="16" />
  <text class="sm label" x="0" y="194">the stream, in key order</text>
  <text class="sm label" x="478" y="194" text-anchor="end">listed ahead: waits in memory,</text>
  <text class="sm label" x="478" y="210" text-anchor="end">then on disk</text>
</svg>
<figcaption>Grey is listed and already in the stream, solid is listed and waiting, outlined is not listed yet.
A range that split keeps only what it listed; its children list the rest.</figcaption>
</figure>

Rows listed ahead of the frontier wait in memory, up to a budget: `--sort-buffer`, by default 1/16 of memory
and at most 1 GiB. Past it they spill to disk. Each range writes its own file, already in order, coded against
the row before and compressed. When the drainer reaches that range it reads the file front to back, with no
merge and no index. Once listing has ended, on a machine with cores to spare, the rest of the stream is cut
into parts where the writer would cut them, and several parts are written at once.

Every row passes one check on its way out, the order guard: its key must sort after the previous row's. A
violation fails the run before any output sees it. With the same build and encoder path (amd64 with AVX2, or
every other platform), output is byte-identical across worker counts unless time closes a part
([ordering](../../docs/internals/ordering.md)).

In the NOAA run the drainer spent 41.3 s waiting for the frontier while the workers listed further ahead.
Spill wrote 795 MB to disk, 4.4 GB of rows before coding and compression, with at most 618 MB on disk at
once.

## Commit before emit, and resume

Journaled runs keep recovery state locally: in the output directory, or the work directory for remote output
(`--work-dir`, when `-o` names a bucket). The journal holds a record per part, with its size, its SHA-256 and
its last key. An inventory of an object store keeps one; a filesystem run does not. A part joins the dataset
only after its journal record is on disk. The manifest, `manifest.json`, is written last and names every part:
a directory without it holds no dataset.

<figure class="swath-figure">
<svg viewBox="0 0 480 200" role="img" aria-labelledby="fg-commit-title fg-commit-desc">
  <title id="fg-commit-title">Parts, their journal records, and the resume point</title>
  <desc id="fg-commit-desc">Four parts in key order. Parts 0 to 2 have journal records on disk and are in the dataset. Part 3 is written but its record is not on disk when the run crashes. The resume point K is the last key of part 2: a resume checks parts 0 to 2 and lists again after K.</desc>
  <text class="sm label" x="0" y="42">parts</text>
  <text class="sm label" x="0" y="78">journal</text>
  <text class="sm label" x="0" y="112">dataset</text>
  <rect class="lower" x="76" y="26" width="88" height="24" />
  <rect class="lower" x="172" y="26" width="88" height="24" />
  <rect class="lower" x="268" y="26" width="88" height="24" />
  <rect class="outline dashed" x="364" y="26" width="88" height="24" />
  <text class="sm mono" x="120" y="43" text-anchor="middle">part 0</text>
  <text class="sm mono" x="216" y="43" text-anchor="middle">part 1</text>
  <text class="sm mono" x="312" y="43" text-anchor="middle">part 2</text>
  <text class="sm mono" x="408" y="43" text-anchor="middle">part 3</text>
  <g class="acc"><circle cx="120" cy="74" r="6" /><circle cx="216" cy="74" r="6" /><circle cx="312" cy="74" r="6" /></g>
  <circle class="outline dashed" cx="408" cy="74" r="6" />
  <rect class="acc" x="76" y="100" width="88" height="16" />
  <rect class="acc" x="172" y="100" width="88" height="16" />
  <rect class="acc" x="268" y="100" width="88" height="16" />
  <line class="s-alert" x1="356" y1="20" x2="356" y2="134" stroke-width="2" />
  <text class="mono t-alert" x="356" y="152" text-anchor="middle">K</text>
  <text class="t-alert" x="408" y="16" text-anchor="middle">crash</text>
  <text class="sm label" x="0" y="176">a resume checks parts 0 to 2 against their records,</text>
  <text class="sm label" x="0" y="194">then lists again after K</text>
</svg>
<figcaption>Commit before emit. A part's record reaches disk before the part joins the dataset, so the records
always describe what is published. The resume point <code>K</code> is the last key of the last recorded
part.</figcaption>
</figure>

The resume point is the last key of the last recorded part. `swath resume DIR` checks every part already
published against its record, size and SHA-256, then lists again after that key. It never writes a key twice
and never skips one. No worker's cursor and no split is saved, so a resume makes its own splits. Ranges that
spilled ahead of the output can be sealed as certified spill files, each holding exactly the rows of one key
interval as the run listed them, recorded once its bytes are on disk. A resume replays those instead of
listing them again. The tests stop a run at every crash point, including with a real SIGKILL, and resume it.
Crash tests compare rows, and compare part bytes when build, encoder path and time-cut conditions permit
([stopping and resuming](../../docs/resume-and-refresh.md#stopping-and-resuming)).

A listing is not a point-in-time snapshot. swath publishes the complete result of the live listing it
performed: an object written or deleted while it runs may or may not be in it.

## Listing a bucket again: the seeded window

A refresh can start from the previous inventory instead of folder names:

```bash
swath list s3://bucket/logs/ -o /data/logs-2026-11-08 --seed-from /data/logs-2026-11-07
```

The previous dataset knows how many keys lie where, so swath targets tiles of about 32 listing pages, by
expected keys, and skips the delimiter seed and most early splits. Its requests approach one per page.

With those counts the run lists the bucket front to back. Tiles are taken in key order, and the window guides
admission toward eight million rows at the output, or fewer when the disk or the sort buffer holds less; it is
not a strict storage bound. Rows listed ahead of the output, and the spill they need, stay near that window
instead of growing with the bucket. The splits stay adaptive: a region that grew since the previous run is
split as on any run ([seeding a refresh](../../docs/resume-and-refresh.md#seeding-a-refresh-from-the-previous-dataset)).

<figure class="swath-figure">
<svg viewBox="0 0 480 128" role="img" aria-labelledby="fg-window-title fg-window-desc">
  <title id="fg-window-title">Tiles admitted in key order, inside a window</title>
  <desc id="fg-window-desc">Sixteen tiles in key order. The first four are written. The next five are admitted and listing, inside the window at the output. The rest wait to be admitted.</desc>
  <path class="s-acc" d="M120 34 L120 24 L270 24 L270 34" stroke-width="1.5" />
  <text class="sm t-acc" x="195" y="16" text-anchor="middle">the window at the output</text>
  <g class="done"><rect x="0" y="38" width="29" height="28" /><rect x="30" y="38" width="29" height="28" /><rect x="60" y="38" width="29" height="28" /><rect x="90" y="38" width="29" height="28" /></g>
  <g class="lower"><rect x="120" y="38" width="29" height="28" /><rect x="150" y="38" width="29" height="28" /><rect x="180" y="38" width="29" height="28" /><rect x="210" y="38" width="29" height="28" /><rect x="240" y="38" width="29" height="28" /></g>
  <g class="outline dashed"><rect x="270.5" y="38.5" width="28" height="27" /><rect x="300.5" y="38.5" width="28" height="27" /><rect x="330.5" y="38.5" width="28" height="27" /><rect x="360.5" y="38.5" width="28" height="27" /><rect x="390.5" y="38.5" width="28" height="27" /><rect x="420.5" y="38.5" width="28" height="27" /><rect x="450.5" y="38.5" width="28" height="27" /></g>
  <text class="sm label" x="60" y="86" text-anchor="middle">written</text>
  <text class="sm label" x="195" y="86" text-anchor="middle">admitted, listing</text>
  <text class="sm label" x="375" y="86" text-anchor="middle">not admitted yet</text>
  <text class="sm label" x="0" y="118">tiles of about 32 pages, cut from the previous inventory's counts</text>
</svg>
<figcaption>A refresh lists front to back. A new tile is admitted as the output moves on, so the rows waiting
for the output stay near the window instead of growing with the bucket.</figcaption>
</figure>

## Exact totals without sorting: the `swath du` view

`swath du` keeps no rows and needs no order across ranges. Its workers count rows as they arrive: there is no
drainer, no spill and no file. Bare `swath du` on a terminal shows the largest prefixes at any depth, and every
figure in it is exact. On the NOAA bucket it took 53.7 s for 37,081,362 objects, with a peak of 119 MiB
against the sorted inventory's 1.51 GiB
([how it was measured](../../docs/internals/measurements.md#a-public-bucket-for-the-home-page-noaa-gestofs-pds-2026-11-15)).

It uses the order inside each range. The rows a range lists before it splits, or all of them if it does not,
are one run of contiguous keys. A prefix that a run enters after its first row and leaves before its last is
complete, so its total is final. The prefixes at a run's two ends are partial; they are summed by name across
runs and are complete when the listing ends. Of the complete prefixes and objects, the view keeps the 100
largest at each depth, whatever order the listing completes them in. A dropped one's bytes are already counted
in its parent's.

The view then fills the terminal from the largest down, each line under its parent with its size, object
count, share, bar and newest date, and the largest prefixes open first. Each parent's other children are
folded into one counted `(N more)` line, so every level adds up. No line goes to an entry under 0.5% of the
total (a quarter of one line's share on a short terminal): a long tail of small prefixes stays in its fold, and
the view may end shorter than the terminal. On a local tree, `!` after the size marks a line whose sums leave
out a directory that could not be read ([the view](../../docs/totals.md#the-view)).

<figure class="swath-figure">
<svg viewBox="0 0 480 136" role="img" aria-labelledby="fg-du-title fg-du-desc">
  <title id="fg-du-title">Complete and partial prefixes in one run of keys</title>
  <desc id="fg-du-desc">One range's run of contiguous keys starts inside the prefix a/ and ends inside d/. The prefixes b/ and c/ lie wholly inside it, so their totals are final. a/ and d/ are cut by the run's ends and are summed by name across runs.</desc>
  <text class="sm label" x="240" y="14" text-anchor="middle">one range's rows: contiguous keys</text>
  <rect class="acc" x="50" y="22" width="380" height="18" />
  <path class="outline dashed" d="M50 22 L50 86 M430 22 L430 86" />
  <rect class="outline dashed" x="0.5" y="54" width="109" height="26" />
  <rect class="lower" x="110" y="54" width="120" height="26" />
  <rect class="lower" x="230" y="54" width="140" height="26" />
  <rect class="outline dashed" x="370" y="54" width="109.5" height="26" />
  <text class="sm mono" x="80" y="72" text-anchor="middle">a/</text>
  <text class="sm mono" x="170" y="72" text-anchor="middle">b/</text>
  <text class="sm mono" x="300" y="72" text-anchor="middle">c/</text>
  <text class="sm mono" x="400" y="72" text-anchor="middle">d/</text>
  <text class="sm t-acc" x="240" y="104" text-anchor="middle">complete: the total is final</text>
  <text class="sm label" x="0" y="104">partial</text>
  <text class="sm label" x="478" y="104" text-anchor="end">partial</text>
  <text class="sm label" x="240" y="128" text-anchor="middle">partial prefixes are summed by name across runs</text>
</svg>
<figcaption>A prefix that one run holds whole is complete as soon as the run passes it. Only the prefixes at a
run's two ends wait for other ranges.</figcaption>
</figure>

## Read the run

[The run report](https://swath.varve.io/runs/noaa-gestofs-pds/){target="_self"} is generated from the NOAA run's trace by
[`tools/explainer`](../../tools/explainer/README.md). It shows how the starting ranges turned out, the rate of
the run over time, the cut points it chose, a map of every range over the keys and time, a replay of the run,
and a ledger that rebuilds the ranges from the trace and checks that they cover the bucket once.

If you have a bucket whose keys this engine cannot divide well, [open an issue](https://github.com/varveio/swath/issues).
The shape of its key distribution is enough; names and contents are not needed.
