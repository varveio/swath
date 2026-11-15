<script setup lang="ts">
// One frame of the miniature model (mini-model.ts): the ranges over the key space, the sorted stream and its
// parts, and the 64 workers. Rows are separate drawings under HTML labels, so the labels keep the page's text
// size at any width.
import { computed } from 'vue'
import { PAGES, type Snapshot } from './mini-model'

const props = defineProps<{ snap: Snapshot }>()

const X0 = 4
const W = 632
const x = (pages: number) => X0 + (pages / PAGES) * W
const partW = W / props.snap.parts.length

// Each range in up to three pieces: listed and already in the stream, listed and waiting for it, not listed.
const pieces = computed(() => {
  const out: { x: number; w: number; cls: string }[] = []
  const f = props.snap.frontier
  for (const r of props.snap.ranges) {
    const hue = r.heavy ? 'heavy' : 'other'
    const emittedTo = Math.min(r.cursor, Math.max(r.lo, f))
    if (emittedTo > r.lo) out.push({ x: x(r.lo), w: x(emittedTo) - x(r.lo), cls: 'emitted' })
    if (r.cursor > emittedTo) out.push({ x: x(emittedTo), w: x(r.cursor) - x(emittedTo), cls: `waiting ${hue}` })
    if (r.hi > r.cursor) out.push({ x: x(r.cursor), w: x(r.hi) - x(r.cursor), cls: `${r.owned ? 'owned' : 'queued'} ${hue}` })
  }
  return out
})

const bounds = computed(() => props.snap.ranges.slice(1).map((r) => x(r.lo)))
</script>

<template>
  <div class="mm-frame">
    <div class="mm-label">Ranges over the key space, as wide as the keys they hold</div>
    <svg viewBox="0 0 640 58" aria-hidden="true" focusable="false">
      <g v-if="snap.heavy">
        <line class="mm-bracket" :x1="x(snap.heavy[0])" :x2="x(snap.heavy[1])" y1="4" y2="4" />
        <line class="mm-bracket" :x1="x(snap.heavy[0])" :x2="x(snap.heavy[0])" y1="4" y2="10" />
        <line class="mm-bracket" :x1="x(snap.heavy[1])" :x2="x(snap.heavy[1])" y1="4" y2="10" />
      </g>
      <rect v-for="(p, i) in pieces" :key="i" :class="p.cls" :x="p.x" :width="Math.max(p.w, 0.4)" y="16" height="34" />
      <line v-for="(b, i) in bounds" :key="'b' + i" class="mm-bound" :x1="b" :x2="b" y1="16" y2="50" />
      <path v-for="(s, i) in snap.splits" :key="'s' + i" :class="['mm-split', s.heavy ? 'heavy' : 'other']"
        :d="`M${x(s.at)} 15 l-5 -8 h10 z`" :style="{ opacity: 1 - (snap.tick - s.tick) / 6 }" />
    </svg>
    <div class="mm-label">The sorted stream, its parts, and the resume point <span class="mm-k">K</span></div>
    <svg viewBox="0 0 640 50" aria-hidden="true" focusable="false">
      <rect class="mm-track" :x="X0" :width="W" y="2" height="12" />
      <rect class="mm-stream" :x="X0" :width="Math.max(x(snap.frontier) - X0, 0)" y="2" height="12" />
      <rect v-for="(state, k) in snap.parts" :key="'p' + k" :class="['mm-part', state]"
        :x="X0 + k * partW + 2" :width="partW - 4" y="20" height="18" />
      <g v-if="snap.resume > 0">
        <line class="mm-resume" :x1="x(snap.resume)" :x2="x(snap.resume)" y1="18" y2="48" />
      </g>
    </svg>
    <div class="mm-label">The {{ snap.workers.length }} workers</div>
    <svg viewBox="0 0 640 40" aria-hidden="true" focusable="false">
      <circle v-for="(w, i) in snap.workers" :key="'w' + i" :class="['mm-worker', w]"
        :cx="14 + (i % 32) * 19.6" :cy="i < 32 ? 10 : 30" r="6.5" />
    </svg>
  </div>
</template>

<style scoped>
.mm-frame svg {
  display: block;
  width: 100%;
  height: auto;
}
.mm-label {
  margin: 10px 0 4px;
  color: var(--vp-c-text-2);
  font-size: 13px;
  line-height: 1.4;
}
.mm-frame .mm-label:first-child {
  margin-top: 0;
}
.mm-k {
  font-family: var(--vp-font-family-mono);
  font-weight: 600;
  color: var(--swath-fig-alert);
}
.emitted {
  fill: var(--vp-c-text-3);
  opacity: 0.3;
}
.waiting.other {
  fill: var(--vp-c-brand-1);
}
.waiting.heavy {
  fill: var(--swath-fig-signal);
}
.owned.other,
.queued.other {
  fill: var(--vp-c-brand-1);
}
.owned.heavy,
.queued.heavy {
  fill: var(--swath-fig-signal);
}
.owned {
  opacity: 0.25;
}
.queued {
  opacity: 0.1;
}
.mm-bound {
  stroke: var(--vp-c-bg);
  stroke-width: 0.8;
}
.mm-bracket {
  stroke: var(--swath-fig-signal);
  stroke-width: 1.5;
}
.mm-split.heavy {
  fill: var(--swath-fig-signal);
}
.mm-split.other {
  fill: var(--vp-c-brand-1);
}
.mm-track {
  fill: none;
  stroke: var(--vp-c-border);
}
.mm-stream {
  fill: var(--vp-c-text-2);
}
.mm-part {
  fill: none;
  stroke: var(--vp-c-border);
  stroke-dasharray: 3 2;
}
.mm-part.written {
  fill: var(--vp-c-brand-1);
  fill-opacity: 0.2;
  stroke: var(--vp-c-brand-1);
}
.mm-part.journaled {
  fill: var(--vp-c-brand-1);
  fill-opacity: 0.45;
  stroke: var(--vp-c-brand-1);
  stroke-dasharray: none;
}
.mm-part.published {
  fill: var(--vp-c-brand-1);
  stroke: var(--vp-c-brand-1);
  stroke-dasharray: none;
}
.mm-resume {
  stroke: var(--swath-fig-alert);
  stroke-width: 2;
}
.mm-worker.idle {
  fill: none;
  stroke: var(--vp-c-text-3);
  stroke-width: 1.2;
}
.mm-worker.other {
  fill: var(--vp-c-brand-1);
}
.mm-worker.heavy {
  fill: var(--swath-fig-signal);
}
</style>
