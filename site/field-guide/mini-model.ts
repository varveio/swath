// The field guide's miniature model of a listing: a deterministic simulation, small enough to watch. It runs
// the kinds of decisions swath makes in the order swath makes them (seed, claim, owner-side split, the
// depth-first drain, commit before emit), not swath's policies: every page takes one tick, the seed's ranges
// are made up, and a split cuts what is left at its middle. MiniModel.vue draws it.

export const WORKERS = 64
export const PAGES = 9600 // the key space, in pages; the drawing's x axis is pages (keys), not key names
export const PARTS = 8
const SEEDS = 96
const HEAVY = 40 // the starting range that holds most of the keys
const HEAVY_PAGES = 5760

export interface Range {
  id: number
  lo: number // the range is (lo, hi], in pages
  hi: number
  cursor: number // listed through here
  owner: number | null
  queued: boolean
  done: boolean
  heavy: boolean // a piece of the heavy starting range
}

export type PartState = 'pending' | 'written' | 'journaled' | 'published'

export interface Part {
  lo: number
  hi: number
  state: PartState
  at: number // the tick of the last change
}

export type Phase = 'seed' | 'list' | 'publish' | 'done'

// What one frame of the drawing shows, copied out of the model.
export interface Snapshot {
  tick: number
  phase: Phase
  event: string
  ranges: { lo: number; hi: number; cursor: number; owned: boolean; heavy: boolean }[]
  heavy: [number, number] | null // the heavy starting range's bounds, once seeded
  frontier: number
  parts: PartState[]
  resume: number
  workers: ('idle' | 'heavy' | 'other')[]
  splits: Split[] // the recent ones, to mark where they cut
  manifest: boolean
  pages: number
  splitCount: number
  live: number
  busy: number
}

export interface Split {
  at: number
  tick: number
  heavy: boolean
}

// The starting ranges' sizes in pages: one heavy range among small ones of uneven size, from a fixed
// generator so every run of the model is the same.
function seedSizes(): number[] {
  let state = 7
  const next = () => ((state = (state * 48271) % 2147483647) / 2147483647)
  const small = Array.from({ length: SEEDS - 1 }, () => 0.3 + next() * next() * 2)
  const sum = small.reduce((a, b) => a + b, 0)
  const sizes = small.map((s) => Math.max(2, Math.round((s / sum) * (PAGES - HEAVY_PAGES))))
  sizes.splice(HEAVY, 0, HEAVY_PAGES)
  sizes[sizes.length - 1] += PAGES - sizes.reduce((a, b) => a + b, 0)
  return sizes
}

export class Model {
  tick = 0
  phase: Phase = 'seed'
  ranges: Range[] = []
  queue: number[] = []
  workers: (number | null)[] = Array(WORKERS).fill(null)
  parts: Part[] = Array.from({ length: PARTS }, (_, k) => ({
    lo: (k * PAGES) / PARTS, hi: ((k + 1) * PAGES) / PARTS, state: 'pending' as PartState, at: 0,
  }))
  splits: Split[] = []
  pages = 0
  frontier = 0
  manifestAt = -1
  firstSplit = -1
  event = 'seed'

  get finished() {
    return this.phase === 'done'
  }

  busy() {
    return this.workers.filter((w) => w !== null).length
  }

  // The last key of the last part whose journal record is on disk: where a resume would list from.
  resumePoint() {
    let k = 0
    for (const p of this.parts) {
      if (p.state === 'journaled' || p.state === 'published') k = p.hi
      else break
    }
    return k
  }

  snapshot(): Snapshot {
    const seeded = this.ranges.length > 0
    const heavy = this.ranges[HEAVY]
    return {
      tick: this.tick,
      phase: this.phase,
      event: this.event,
      ranges: seeded
        ? this.ranges.filter((r) => r.hi > r.lo).map((r) => ({ lo: r.lo, hi: r.hi, cursor: r.cursor, owned: r.owner !== null, heavy: r.heavy }))
        : [{ lo: 0, hi: PAGES, cursor: 0, owned: false, heavy: false }],
      heavy: seeded ? [heavy.lo, Math.max(...this.ranges.filter((r) => r.heavy).map((r) => r.hi))] : null,
      frontier: this.frontier,
      parts: this.parts.map((p) => p.state),
      resume: this.resumePoint(),
      workers: this.workers.map((id) => (id === null ? 'idle' : this.ranges[id].heavy ? 'heavy' : 'other')),
      splits: this.splits.filter((s) => this.tick - s.tick < 6),
      manifest: this.phase === 'done',
      pages: this.pages,
      splitCount: this.splits.length,
      live: seeded ? this.ranges.filter((r) => !r.done).length : 1,
      busy: this.busy(),
    }
  }

  step() {
    if (this.finished) return
    this.tick++
    if (this.phase === 'seed') {
      let lo = 0
      seedSizes().forEach((size, i) => {
        this.ranges.push({ id: i, lo, hi: lo + size, cursor: lo, owner: null, queued: true, done: false, heavy: i === HEAVY })
        this.queue.push(i)
        lo += size
      })
      this.phase = 'list'
      this.event = 'seeded'
      return
    }
    this.event = ''
    if (this.phase === 'list') this.list()
    this.drain()
    this.publish()
  }

  private list() {
    // Idle workers take queued ranges, oldest first.
    for (let w = 0; w < WORKERS && this.queue.length; w++) {
      if (this.workers[w] !== null) continue
      const r = this.ranges[this.queue.shift()!]
      r.queued = false
      r.owner = w
      this.workers[w] = r.id
    }
    // Every busy worker lists one page, then decides.
    for (let w = 0; w < WORKERS; w++) {
      const id = this.workers[w]
      if (id === null) continue
      const r = this.ranges[id]
      r.cursor = Math.min(r.hi, r.cursor + 1)
      this.pages++
      if (r.cursor >= r.hi) {
        r.done = true
        r.owner = null
        this.workers[w] = null
        continue
      }
      this.maybeSplit(w, r)
    }
    if (this.ranges.every((r) => r.done)) {
      this.phase = 'publish'
      this.event = 'listed'
    }
  }

  // The owner splits only when a worker is idle with nothing queued, only when its range is among the largest
  // left, and only when what is left is worth it: at least two pages, at least a page on either side.
  private maybeSplit(w: number, r: Range) {
    const hunger = this.workers.filter((x) => x === null).length - this.queue.length
    if (hunger <= 0) return
    const left = r.hi - r.cursor
    if (left < 2) return
    const larger = this.ranges.filter((o) => !o.done && o.owner !== null && o.hi - o.cursor > left).length
    if (larger >= hunger) return
    const m = r.cursor + Math.floor(left / 2)
    if (m - r.cursor < 1 || r.hi - m < 1) return
    const lower: Range = { id: this.ranges.length, lo: r.cursor, hi: m, cursor: r.cursor, owner: null, queued: true, done: false, heavy: r.heavy }
    const upper: Range = { id: this.ranges.length + 1, lo: m, hi: r.hi, cursor: m, owner: null, queued: true, done: false, heavy: r.heavy }
    this.ranges.push(lower, upper)
    this.queue.push(lower.id, upper.id)
    r.hi = r.cursor
    r.done = true
    r.owner = null
    this.workers[w] = null
    this.splits.push({ at: m, tick: this.tick, heavy: r.heavy })
    if (this.firstSplit < 0) {
      this.firstSplit = this.tick
      this.event = 'first-split'
    }
  }

  // The ordering stage's frontier: ranges tile the key space, so walking them in key order, the stream has
  // every row through the first range that is not finished, up to that range's cursor.
  private drain() {
    const ordered = [...this.ranges].sort((a, b) => a.lo - b.lo || a.hi - b.hi)
    let f = 0
    for (const r of ordered) {
      if (r.lo > f) break
      if (!r.done) {
        f = r.cursor
        break
      }
      f = Math.max(f, r.hi)
    }
    this.frontier = f
  }

  // A part is written once the stream has passed its end, one part per tick; its journal record reaches disk a
  // tick later, and only then is it renamed into the dataset. The manifest comes last.
  private publish() {
    let wrote = false
    for (const p of this.parts) {
      if (p.state === 'journaled' && this.tick > p.at) {
        p.state = 'published'
        p.at = this.tick
        if (!this.event) this.event = 'published'
      } else if (p.state === 'written' && this.tick > p.at) {
        p.state = 'journaled'
        p.at = this.tick
      } else if (p.state === 'pending' && !wrote && this.frontier >= p.hi) {
        p.state = 'written'
        p.at = this.tick
        wrote = true
      }
    }
    if (this.phase === 'publish' && this.parts.every((p) => p.state === 'published')) {
      if (this.manifestAt < 0) this.manifestAt = this.tick + 1
      else if (this.tick >= this.manifestAt) {
        this.phase = 'done'
        this.event = 'manifest'
      }
    }
  }
}
