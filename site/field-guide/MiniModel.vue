<script setup lang="ts">
// The field guide's animated miniature model (mini-model.ts, drawn by ModelFrame.vue). It plays when it scrolls
// into view; Pause, Step and Restart drive it by hand. With prefers-reduced-motion it shows four still frames
// instead of playing.
import { computed, onBeforeUnmount, onMounted, ref, shallowRef } from 'vue'
import ModelFrame from './ModelFrame.vue'
import { Model, type Snapshot } from './mini-model'

const TICKS_PER_SECOND = { seed: 1, list: 8, publish: 2.5, done: 1 }
// Pauses, in seconds, on the moments the text below the drawing explains.
const HOLD: Record<string, number> = { seeded: 2, 'first-split': 2.5, published: 1.5, listed: 1.5 }

const STATUS: Record<string, string> = {
  start: 'Before listing, the whole key space is one range. The seed lists the top level\'s folder names and cuts there.',
  seeded: 'The seed made 96 starting ranges at folder names. One of them, bracketed, holds 60% of the keys, and nothing tells swath so.',
  list: 'Workers take queued ranges and list them one page per round trip. Every page is in key order.',
  'first-split': 'A worker is idle and nothing is queued: the owner of the largest range splits what it has not listed at a cut point, hands both halves on, and idle workers take them.',
  split: 'Owners keep splitting while workers are idle. Rows listed ahead of the stream wait, in memory and then on disk.',
  published: 'The stream passed the end of a part: the part is written, its journal record reaches disk, and only then does it join the dataset.',
  listed: 'Listing has ended. The stream catches up, and the remaining parts are written, recorded and published in key order.',
  manifest: 'Every part is published. The manifest, written last, names them all: now the directory holds a dataset.',
}

const model = shallowRef(new Model())
const snap = shallowRef<Snapshot>(model.value.snapshot())
const status = ref(STATUS.start)
const playing = ref(false)
const reduced = ref(false)
const frames = shallowRef<{ snap: Snapshot; caption: string }[]>([])
const root = ref<HTMLElement | null>(null)

let raf = 0
let last = 0
let owed = 0
let holdUntil = 0
let observer: IntersectionObserver | null = null
let firstPublished = false

// Shows the status line for a frame's event; true when it is news (the first part published, not every one).
function announce(s: Snapshot): boolean {
  if (s.event === 'published') {
    if (firstPublished) return false
    firstPublished = true
  }
  if (STATUS[s.event]) {
    status.value = STATUS[s.event]
    return true
  }
  if (s.phase === 'list' && s.tick === 2) status.value = STATUS.list
  else if (s.splitCount > 0 && s.tick === model.value.firstSplit + 20) status.value = STATUS.split
  return false
}

function advance(now: number) {
  model.value.step()
  const s = model.value.snapshot()
  snap.value = s
  if (announce(s) && HOLD[s.event]) holdUntil = now + HOLD[s.event] * 1000
  if (model.value.finished) stop()
}

function frame(now: number) {
  raf = requestAnimationFrame(frame)
  if (!last) last = now
  const dt = (now - last) / 1000
  last = now
  if (now < holdUntil) return
  owed += dt * TICKS_PER_SECOND[model.value.phase]
  while (owed >= 1 && playing.value) {
    owed -= 1
    advance(now)
    if (now < holdUntil) {
      owed = 0
      break
    }
  }
}

function play() {
  if (model.value.finished) restart()
  playing.value = true
  last = 0
  owed = 1
  cancelAnimationFrame(raf)
  raf = requestAnimationFrame(frame)
}

function stop() {
  playing.value = false
  cancelAnimationFrame(raf)
}

function toggle() {
  if (playing.value) stop()
  else play()
}

function step() {
  stop()
  if (model.value.finished) restart()
  advance(0)
  holdUntil = 0
}

function restart() {
  stop()
  model.value = new Model()
  firstPublished = false
  snap.value = model.value.snapshot()
  status.value = STATUS.start
}

const playLabel = computed(() => (playing.value ? 'Pause' : snap.value.phase === 'done' ? 'Play again' : 'Play'))

// The still frames for reduced motion: after the seed, just after the first split, with every worker on the
// heavy range, and at the end.
function stills() {
  const m = new Model()
  const out: { snap: Snapshot; caption: string }[] = []
  const take = (caption: string) => out.push({ snap: m.snapshot(), caption })
  m.step()
  take('1. ' + STATUS.seeded)
  while (m.firstSplit < 0) m.step()
  for (let i = 0; i < 3; i++) m.step()
  take('2. ' + STATUS['first-split'])
  while (m.snapshot().workers.some((w) => w !== 'heavy')) m.step()
  take('3. Every worker now lists a piece of the heavy range. ' + STATUS.split)
  while (!m.finished) m.step()
  take('4. ' + STATUS.manifest)
  return out
}

onMounted(() => {
  reduced.value = window.matchMedia('(prefers-reduced-motion: reduce)').matches
  if (reduced.value) {
    frames.value = stills()
    return
  }
  observer = new IntersectionObserver((entries) => {
    if (entries.some((e) => e.isIntersecting)) {
      if (snap.value.tick === 0) play()
      observer?.disconnect()
    }
  }, { threshold: 0.4 })
  if (root.value) observer.observe(root.value)
})

onBeforeUnmount(() => {
  stop()
  observer?.disconnect()
})
</script>

<template>
  <figure ref="root" class="swath-model" aria-labelledby="fg-model-caption">
    <template v-if="!reduced">
      <div class="mm-bar">
        <button type="button" class="mm-button" @click="toggle">{{ playLabel }}</button>
        <button type="button" class="mm-button" @click="step">Step</button>
        <button type="button" class="mm-button" @click="restart">Restart</button>
        <span class="mm-tick">round trip {{ snap.tick }}</span>
      </div>
      <p class="mm-status" role="status">{{ status }}</p>
      <ModelFrame :snap="snap" />
      <dl class="mm-readout">
        <div><dt>Pages listed</dt><dd>{{ snap.pages.toLocaleString('en-US') }}</dd></div>
        <div><dt>Ranges listing or queued</dt><dd>{{ snap.live }}</dd></div>
        <div><dt>Splits</dt><dd>{{ snap.splitCount }}</dd></div>
        <div><dt>Workers busy</dt><dd>{{ snap.busy }} of {{ snap.workers.length }}</dd></div>
        <div><dt>Parts published</dt><dd>{{ snap.parts.filter((p) => p === 'published').length }} of {{ snap.parts.length }}</dd></div>
        <div><dt>Manifest</dt><dd>{{ snap.manifest ? 'written' : 'not yet' }}</dd></div>
      </dl>
    </template>
    <ol v-else class="mm-stills">
      <li v-for="(f, i) in frames" :key="i">
        <p class="mm-status">{{ f.caption }}</p>
        <ModelFrame :snap="f.snap" />
      </li>
    </ol>
    <ul class="mm-legend" aria-label="Key">
      <li><span class="sw waiting" />listed, waiting for the stream</li>
      <li><span class="sw owned" />not listed yet</li>
      <li><span class="sw emitted" />in the sorted stream</li>
      <li><span class="sw heavy" />the heavy starting range and its pieces</li>
      <li><span class="sw tri" />a split's cut point</li>
      <li><span class="sw part" />a part: written, recorded, published</li>
      <li><span class="sw idle" />an idle worker</li>
    </ul>
    <figcaption id="fg-model-caption">A model, not a measurement: 64 workers, 96 made-up starting ranges, one page
      per round trip for every worker, and each split cuts what is left at its middle. It makes the kinds of
      decisions swath makes, in the same order. <span class="mm-k">K</span> is where a resume would start: the
      last key of the last part whose journal record is on disk.</figcaption>
  </figure>
</template>

<style scoped>
.swath-model {
  margin: 24px 0;
  padding: 16px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-soft);
}
.mm-bar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
}
.mm-button {
  padding: 4px 14px;
  border: 1px solid var(--vp-c-brand-1);
  border-radius: 8px;
  color: var(--vp-c-brand-1);
  font-size: 14px;
  font-weight: 600;
  line-height: 24px;
}
.mm-button:hover {
  background: var(--vp-c-brand-soft);
}
.mm-button:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 2px;
}
.mm-tick {
  margin-left: auto;
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-size: 13px;
}
.mm-status {
  min-height: 3em;
  margin: 12px 0;
  font-size: 14px;
  line-height: 1.5;
}
.mm-readout {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 8px 16px;
  margin: 16px 0 0;
}
.mm-readout dt {
  color: var(--vp-c-text-2);
  font-size: 12px;
  line-height: 1.4;
}
.mm-readout dd {
  margin: 0;
  font-family: var(--vp-font-family-mono);
  font-size: 15px;
}
.mm-stills {
  margin: 0;
  padding: 0;
  list-style: none;
}
.mm-stills li + li {
  margin-top: 24px;
}
.mm-stills .mm-status {
  margin-top: 0;
  min-height: 0;
}
.mm-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 16px;
  margin: 16px 0 0;
  padding: 0;
  list-style: none;
  color: var(--vp-c-text-2);
  font-size: 12px;
}
.mm-legend li {
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
.sw.waiting {
  background: var(--vp-c-brand-1);
}
.sw.owned {
  background: color-mix(in srgb, var(--vp-c-brand-1) 25%, transparent);
}
.sw.emitted {
  background: color-mix(in srgb, var(--vp-c-text-3) 30%, transparent);
}
.sw.heavy {
  background: var(--swath-fig-signal);
}
.sw.tri {
  width: 10px;
  background: var(--vp-c-brand-1);
  clip-path: polygon(0 0, 100% 0, 50% 100%);
}
.sw.part {
  border: 1px solid var(--vp-c-brand-1);
  background: linear-gradient(90deg, color-mix(in srgb, var(--vp-c-brand-1) 20%, transparent) 33%,
    color-mix(in srgb, var(--vp-c-brand-1) 45%, transparent) 33% 66%, var(--vp-c-brand-1) 66%);
  width: 24px;
}
.sw.idle {
  width: 10px;
  height: 10px;
  border: 1.2px solid var(--vp-c-text-3);
  border-radius: 50%;
}
.mm-k {
  font-family: var(--vp-font-family-mono);
  font-weight: 600;
  color: var(--swath-fig-alert);
}
figcaption {
  margin-top: 12px;
  color: var(--vp-c-text-2);
  font-size: 14px;
  line-height: 1.5;
}
</style>
