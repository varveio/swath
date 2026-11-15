<script setup lang="ts">
// The home page run's listing requests in flight over time, on the largest starting range and elsewhere, and
// that range's splits (run-figures.json, from the run's trace).
import run from './run-figures.json'

const L = 34
const R = 462
const TOP = 22
const H = 136 // the plot's height for 64 requests
const BASE = TOP + H
const x = (s: number) => L + (s / run.end_s) * (R - L)
const y = (v: number) => BASE - (v / run.workers) * H

function area(lower: number[], upper: number[]): string {
  const pts = (vals: number[]) => vals.map((v, i) => `${x((i + 0.5) * run.bin_s).toFixed(1)},${y(v).toFixed(1)}`)
  const top = pts(upper)
  const bottom = pts(lower).reverse()
  return `M${top.join(' L')} L${bottom.join(' L')} Z`
}

const zero = run.in_flight_heavy.map(() => 0)
const total = run.in_flight_heavy.map((v, i) => v + run.in_flight_other[i])
const heavyArea = area(zero, run.in_flight_heavy)
const otherArea = area(run.in_flight_heavy, total)
const peakSplits = Math.max(...run.heavy_splits)
const rug = run.heavy_splits
  .map((c, i) => ({ x: x(i * run.bin_s), h: (c / peakSplits) * 18 }))
  .filter((r) => r.h > 0)
const ticks = Array.from({ length: Math.floor(run.end_s / 10) + 1 }, (_, i) => i * 10)
const fmt = (s: number) => s.toFixed(1)
</script>

<template>
  <figure class="swath-figure wide">
    <svg viewBox="0 0 480 232" role="img" aria-labelledby="fg-inflight-title fg-inflight-desc">
      <title id="fg-inflight-title">Listing requests in flight over the run, and where they went</title>
      <desc id="fg-inflight-desc">For the first 13 seconds about 63 of the 64 workers list the other starting ranges and
        one lists the largest. From the first split at {{ fmt(run.first_split_s) }} s, the largest range's share
        grows until, from about 30 s, every request in flight is on it. The last range ends at
        {{ fmt(run.last_range_s) }} s; the run ends at {{ fmt(run.end_s) }} s.</desc>
      <rect class="band" :x="x(run.last_range_s)" :y="TOP" :width="x(run.end_s) - x(run.last_range_s)" :height="H" />
      <path class="other" :d="otherArea" />
      <path class="heavy" :d="heavyArea" />
      <line class="axis" :x1="L" :x2="R" :y1="BASE" :y2="BASE" />
      <line class="grid" :x1="L" :x2="R" :y1="TOP" :y2="TOP" />
      <text class="label sm" :x="L - 6" :y="TOP + 4" text-anchor="end">64</text>
      <text class="label sm" :x="L - 6" :y="BASE + 4" text-anchor="end">0</text>
      <text v-for="t in ticks" :key="t" class="label sm" :x="x(t)" :y="BASE + 18" text-anchor="middle">{{ t }} s</text>
      <line class="mark" :x1="x(run.first_split_s)" :x2="x(run.first_split_s)" :y1="TOP - 8" :y2="BASE" />
      <text class="sm" :x="x(run.first_split_s) + 4" :y="TOP - 10">first split, {{ fmt(run.first_split_s) }} s</text>
      <text class="sm" :x="x(run.end_s)" :y="TOP - 10" text-anchor="end">last range {{ fmt(run.last_range_s) }} s</text>
      <rect v-for="(r, i) in rug" :key="i" class="heavy" :x="r.x" :y="210 - r.h" width="1.6" :height="r.h" />
      <line class="axis" :x1="L" :x2="R" y1="210" y2="210" />
      <text class="label sm" :x="L" y="226">splits of the largest starting range ({{ run.heavy.splits.toLocaleString('en-US') }})</text>
    </svg>
    <ul class="fg-legend">
      <li><span class="sw heavy" />requests listing the largest starting range</li>
      <li><span class="sw other" />requests listing the other {{ run.seed_keys.length - 1 }}</li>
      <li><span class="sw band" />after the last range: draining, publication and finalization</li>
    </ul>
    <figcaption>The home page's run, from its trace, in quarter-second steps. For 13 seconds one worker listed the
      range that held 64.5% of the bucket while the others had starting ranges of their own; once those ran out,
      every split fed an idle worker, and all {{ run.workers }} listed pieces of it.</figcaption>
  </figure>
</template>

<style scoped>
.heavy {
  fill: var(--swath-fig-signal);
}
.other {
  fill: var(--vp-c-brand-1);
  opacity: 0.45;
}
.band {
  fill: var(--vp-c-text-3);
  opacity: 0.16;
}
.axis {
  stroke: var(--vp-c-text-3);
}
.grid {
  stroke: var(--vp-c-divider);
  stroke-dasharray: 3 3;
}
.mark {
  stroke: var(--vp-c-text-1);
  stroke-dasharray: 3 2;
}
.fg-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 16px;
  margin: 8px 0 0;
  padding: 0;
  list-style: none;
  color: var(--vp-c-text-2);
  font-size: 13px;
}
.fg-legend li {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 0;
}
.sw {
  display: inline-block;
  width: 14px;
  height: 10px;
}
.sw.heavy {
  background: var(--swath-fig-signal);
}
.sw.other {
  background: color-mix(in srgb, var(--vp-c-brand-1) 45%, transparent);
}
.sw.band {
  background: color-mix(in srgb, var(--vp-c-text-3) 16%, transparent);
  border: 1px solid var(--vp-c-divider);
}
</style>
