<script setup lang="ts">
// The home page run's 257 starting ranges: an even share each, against the objects each turned out to hold
// (run-figures.json, from the run's trace).
import run from './run-figures.json'

const W = 480
const n = run.seed_keys.length
const h = run.heavy.index
let at = 0
const measured = run.seed_keys.map((k) => {
  const x = (at / run.objects) * W
  at += k
  return { x, w: (k / run.objects) * W }
})
const even = W / n
const share = ((100 * run.heavy.keys) / run.objects).toFixed(1)
const millions = (run.heavy.keys / 1e6).toFixed(1)
const rest = (100 - Number(share)).toFixed(1)
</script>

<template>
  <figure class="swath-figure wide">
    <svg viewBox="0 0 480 172" role="img" aria-labelledby="fg-seeds-title fg-seeds-desc">
      <title id="fg-seeds-title">{{ n }} starting ranges, and the one that held most of the bucket</title>
      <desc id="fg-seeds-desc">Two strips of the same {{ n }} ranges in key order. Above, each as wide as an even
        share. Below, each as wide as the objects it held: the third range takes {{ share }}% of the strip, and the
        other {{ n - 1 }} share the rest.</desc>
      <text class="label sm" x="0" y="13">An even share each: {{ (100 / n).toFixed(2) }}%</text>
      <rect v-for="i in n" :key="'e' + i" :class="i - 1 === h ? 'sig' : i % 2 ? 'dim' : 'dim2'"
        :x="(i - 1) * even" y="20" :width="even" height="24" />
      <path class="sig-soft"
        :d="`M${h * even} 44 L${(h + 1) * even} 44 L${measured[h].x + measured[h].w} 98 L${measured[h].x} 98 Z`" />
      <rect v-for="(m, i) in measured" :key="'m' + i" :class="i === h ? 'sig' : i % 2 ? 'dim' : 'dim2'"
        :x="m.x" y="98" :width="Math.max(m.w, 0.3)" height="28" />
      <text class="on-sig" :x="measured[h].x + measured[h].w / 2" y="117" text-anchor="middle">{{ millions }} million objects, {{ share }}%</text>
      <text class="label sm" x="0" y="146">What each held</text>
      <text class="label sm" x="480" y="146" text-anchor="end">the other {{ n - 1 }}: {{ rest }}%</text>
      <text class="label sm" x="0" y="166">Median {{ [...run.seed_keys].sort((a, b) => a - b)[Math.floor(n / 2)].toLocaleString('en-US') }} objects; none empty</text>
    </svg>
    <figcaption>The starting ranges of the home page's run, in key order. The seed cut at folder names, which say
      where the structure is but not how many objects lie behind it. The third range, <code>(_para3/,
      estofs.20210102/]</code>, was split {{ run.heavy.splits.toLocaleString('en-US') }} times.</figcaption>
  </figure>
</template>

<style scoped>
.dim {
  fill: var(--vp-c-text-3);
  opacity: 0.45;
}
.dim2 {
  fill: var(--vp-c-text-3);
  opacity: 0.25;
}
.sig {
  fill: var(--swath-fig-signal);
}
.sig-soft {
  fill: var(--swath-fig-signal);
  opacity: 0.15;
}
.swath-figure text.on-sig {
  fill: var(--vp-c-bg);
  font-size: 14px;
  font-weight: 600;
}
</style>
