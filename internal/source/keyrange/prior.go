// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/varveio/swath/internal/source"
)

// maxKeyBytes is the longest key a range-engine provider lists (S3's and
// GCS's 1,024 bytes): a longer cut names no key.
const maxKeyBytes = 1024

// SeedFrom makes the engine start from pr instead of the delimiter seed
// (source.Seeded; docs/internals/range-engine.md, "Warm start"). A prior sets
// the initial tiles and their first estimates only; the adaptive engine
// (owner splits, the gauge, admission, hunger) runs as on any run, so a
// prior can make a listing slower, never wrong: every tiling of the scope
// lists every key once, whatever the cuts.
func (p *Engine[P]) SeedFrom(pr *source.Prior) error {
	if p.opt.Sequential {
		return fmt.Errorf("%w: the sequential listing has no ranges to tile", source.ErrPrior)
	}
	if err := pr.Check(p.opt.Prefix, maxKeyBytes); err != nil {
		return err
	}
	p.opt.Prior = pr
	if pr.Counted && p.opt.PriorPageSize > p.opt.PageSize {
		p.opt.PageSize = p.opt.PriorPageSize
		p.opt.PageMax = max(p.opt.PageMax, p.opt.PageSize) // as New keeps it
		p.c.Set(cSeedFromPageSize, int64(p.opt.PageSize))
	}
	return nil
}

// Tiles is the default prior tile budget and seed folder-cut budget:
// 4 per worker, at most maxSeeds. The seed also adds its frontier and,
// when available, a closing sentinel.
func Tiles(workers int) int { return min(maxSeeds, 4*max(workers, 1)) }

// tileLimit is the explicit total tile budget, or the legacy default
// (a folder-cut budget for the seed, a tile budget for a prior).
func (p *Engine[P]) tileLimit() int {
	if p.opt.SeedTiles > 0 {
		return p.opt.SeedTiles
	}
	return Tiles(p.opt.Workers)
}

// priorTilesPerWorker is how many tiles a counted prior aims at per
// worker: two, so every worker starts with a tile and the end of the run
// has some to balance, where four (Tiles) make tiles of few pages each,
// and each tile's last page is partly past its end.
const priorTilesPerWorker = 2

// tilePages is the target pages per counted-prior tile: the
// prior's expected keys over priorTilesPerWorker tiles per worker, in
// whole pages, at least one. A coarse first cut can exceed the target.
func tilePages(expected float64, workers, page int) int {
	return max(1, int(math.Ceil(expected/float64(priorTilesPerWorker*max(workers, 1)*page))))
}

// priorTile is one bounded tile a prior makes: (previous, hi] and the keys
// the prior expects in it (est; counted false: none).
type priorTile struct {
	hi  string
	est float64
}

// tilePrior merges a prior's cuts into at most k-1 bounded tiles of about
// equal expected mass (the open frontier after the last is the k-th):
//   - a cut at or below after (a resume's last published key) is dropped,
//     with its keys: they are published;
//   - a cut the provider does not accept as a bound becomes its longest
//     accepted prefix when that still sorts after the previous cut and
//     inside the scope, else it is dropped and its keys join the next cut's
//     interval (unsafe counts both);
//   - with fill > 0 (counted), a tile ends at the last cut that keeps its
//     mass at most fill, or at its first cut when that alone exceeds it,
//     so a tile of whole pages ends where its last page does; if that
//     makes more than k-1 tiles, the tiles are cut as below instead;
//   - else, with more cuts than tiles, a cut is kept where the running
//     mass (rows, or one per cut without counts) first reaches the next
//     multiple of total/(k-1);
//   - the last cut is always kept, so the frontier starts after the
//     prior's last key.
func tilePrior(pr *source.Prior, k int, fill float64, after, scope string, safe func(string) bool, safeStart func(string) string) (tiles []priorTile, unsafe int) {
	type cand struct {
		key  string
		mass float64
	}
	var cands []cand
	pending, last := 0.0, after
	for _, c := range pr.Cuts {
		w := 1.0
		if pr.Counted {
			w = float64(c.Rows)
		}
		if c.Key <= after {
			continue
		}
		key := c.Key
		if !safe(key) {
			unsafe++
			key = safeStart(key)
			if key <= last || !strings.HasPrefix(key, scope) {
				pending += w
				continue
			}
		}
		cands = append(cands, cand{key, pending + w})
		pending, last = 0, key
	}
	if k <= 1 {
		return nil, unsafe // the open frontier alone covers the scope
	}
	bounded := k - 1
	if fill > 0 {
		acc := 0.0
		for i, c := range cands {
			if acc > 0 && acc+c.mass > fill {
				tiles = append(tiles, priorTile{hi: cands[i-1].key, est: acc})
				acc = 0
			}
			acc += c.mass
		}
		if n := len(cands); n > 0 {
			tiles = append(tiles, priorTile{hi: cands[n-1].key, est: acc})
		}
		if len(tiles) <= bounded {
			return tiles, unsafe
		}
		tiles = nil
	}
	var total float64
	for _, c := range cands {
		total += c.mass
	}
	cum, next, acc := 0.0, 1, 0.0
	for i, c := range cands {
		cum += c.mass
		acc += c.mass
		keep := len(cands) <= bounded || i == len(cands)-1
		if !keep && next < bounded && cum >= float64(next)*total/float64(bounded) {
			keep = true
			for next < bounded && cum >= float64(next)*total/float64(bounded) {
				next++
			}
		}
		if keep {
			tiles = append(tiles, priorTile{hi: c.key, est: acc})
			acc = 0
		}
	}
	return tiles, unsafe
}

// expandPrior tiles the scope (or a resumed (K, ⊤], cut around reused
// certificates) at the prior's cuts and emits no rows: each bounded tile starts with the prior's expected
// keys as its estimate (the rank's remainder until its first page, which
// replaces it with the page's own measure), and the open frontier after
// the last cut lists what the prior did not know.
func (p *Engine[P]) expandPrior(e source.Emitter[Range, P]) error {
	pr := p.opt.Prior
	after := ""
	if p.opt.Resume != nil {
		after = p.opt.Resume.After
	}
	// A counted prior's tiles are whole pages each (tilePages), less the
	// one key a seek after a listed name can take from the first page
	// (Azure's startFrom returns the tile's lo, which is dropped).
	fill := 0.0
	if pr.Counted {
		expected := 0.0
		for _, c := range pr.Cuts {
			if c.Key > after {
				expected += float64(c.Rows)
			}
		}
		q := tilePages(expected, p.opt.Workers, p.opt.PageSize)
		fill = float64(q*p.opt.PageSize - 1)
		p.c.Set(cSeedFromTilePages, int64(q))
	}
	tiles, unsafe := tilePrior(pr, p.tileLimit(), fill, after, p.opt.Prefix, p.safe, p.safeStart)
	p.lock()
	p.c.Set(cSeedFromParts, int64(pr.Parts))
	p.c.Set(cSeedFromIndexed, int64(pr.IndexedParts))
	p.c.Set(cSeedFromIndexFailed, int64(pr.IndexFailed))
	p.c.Set(cSeedFromCuts, int64(len(pr.Cuts)))
	p.c.Set(cSeedFromUnsafe, int64(unsafe))
	out := make([]Range, 0, len(tiles)+1)
	lo := after
	var expected float64
	for _, t := range tiles {
		r := Range{lo: lo, hi: t.hi, id: p.ids.Add(1), born: fromSeed}
		if pr.Counted {
			r.est, r.estOK, r.prior = t.est, true, true
			expected += t.est
			p.tiles = append(p.tiles, tileAccount{est: t.est})
			r.tile = int32(len(p.tiles))
		}
		out = append(out, r)
		lo = t.hi
	}
	out = append(out, Range{lo: lo, open: true, id: p.ids.Add(1), born: fromSeed})
	if s := p.safeStart(after); s != after {
		out[0].lo, out[0].after = s, after
	}
	p.c.Set(cSeedFromTiles, int64(len(out)))
	p.c.Set(cSeedFromExpected, int64(expected))
	// On a resume with reused certificates the prior tiles only the live
	// gaps: each tile is cut around the certificates' spans, which become
	// reused ranges the order stage replays. A prior never confers
	// coverage: no key range is skipped because the prior listed it.
	out = p.aroundReuse(out)
	if p.opt.Resume != nil {
		p.c.Set(cResumedRanges, int64(len(out)))
	}
	p.account(time.Now())
	p.queued += p.queue(out)
	p.c.Max(cMaxQueued, int64(p.queued))
	p.unlock()
	for _, t := range out {
		if err := e.Child(t); err != nil {
			return err
		}
	}
	return nil
}

// tileAccount is a prior tile's expectation and the keys its ranges have
// listed (under mu).
type tileAccount struct {
	est  float64
	keys int64
}

// priorErrors classifies every prior tile by the keys its ranges listed
// (once the run is done, the tile's exact count) against the prior's
// expectation, mu held: within a factor of priorErrorFactor, grown past
// it, or shrunk below it, and the largest growth. Read at every Stats, so
// a run report taken while ranges still list shows the tiles so far.
func (p *Engine[P]) priorErrors() {
	if len(p.tiles) == 0 {
		return
	}
	var within, grown, shrunk, most int64
	for _, t := range p.tiles {
		ratio := float64(t.keys) / max(t.est, 1)
		switch {
		case ratio > priorErrorFactor:
			grown++
		case ratio < 1/priorErrorFactor:
			shrunk++
		default:
			within++
		}
		most = max(most, int64(math.Round(ratio)))
	}
	p.c.Set(cSeedFromErrWithin, within)
	p.c.Set(cSeedFromErrGrown, grown)
	p.c.Set(cSeedFromErrShrunk, shrunk)
	p.c.Set(cSeedFromErrMaxGrown, most)
}

// priorErrorFactor is the error a prior tile may show and still count as
// within the prior: a factor of 2 either way. A fresh prior is exact (a
// rerun's tiles hold the rows the dataset counted); a factor of 2 is a
// segment that doubled or halved since, where the tile's work is off by a
// tile's worth and the adaptive splits start to matter.
const priorErrorFactor = 2.0
