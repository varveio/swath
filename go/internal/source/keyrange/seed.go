// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/varveio/swath/go/internal/source"
)

const (
	seedDelimiter = "/"
	maxSeeds      = 1000 // the most cuts a seed makes
	// seedPages caps one folder's delimiter chain. A page holds about 1,000
	// folders, so 4 pages are 4x the most cuts a seed keeps (maxSeeds) and
	// 16x the 4×workers of 64 workers; folders past them stay in the last
	// tile, which splits as any range does.
	seedPages = 4
	// seedLevels is the most levels the seed descends below the top: the
	// single-root date layout (warehouse/events/year=Y/month=M/day=D/)
	// needs four to reach its day folders.
	seedLevels = 4
	// seedRequests caps the seed's requests (also counted in requests): a
	// level of descent under up to 63 narrow folders, or the four levels of
	// a date layout (1+1+1+1+12), about one worker's pages for a second.
	seedRequests = 64
	// seedRoundTrips caps the seed's waves of requests, each one request
	// latency on the run's critical path before any range lists: the top
	// level's page cap plus two levels, or the five waves of the date
	// layout. Java's serial 256-probe descent spent 13.5 s of a 39 s run on
	// its chain; a wave here sends its requests at once.
	seedRoundTrips = 6
)

// seedStop is why the seed stopped descending.
type seedStop uint8

const (
	stopWide       seedStop = iota // a level held >= workers folders
	stopLeaf                       // a level held no folder to descend into
	stopLevels                     // seedLevels descended
	stopRequests                   // the next level, or an empty page's follow-up, would pass seedRequests
	stopRoundTrips                 // seedRoundTrips spent
	stopFailed                     // a listing failed, or the run was canceled
	stopOnePage                    // the first data page was not truncated: the scope fits in one page
	stopFlat                       // a level with no folder, after a chain past a folderless page was not followed
)

// seedNode is one folder the seed lists with the delimiter: its common
// prefixes are cuts and the next level's folders.
type seedNode struct {
	prefix string
	after  string // start-after inside the folder ("" from its start): a resume's safe start
}

// seedChain is one folder's delimiter chain as the seed follows it.
type seedChain struct {
	q         *Query
	prefixes  []string
	pages     int
	truncated bool // more pages: not yet listed, or the chain stopped at the cap or a budget
	// From the chain's last page that held an entry: its greatest prefix,
	// and whether it ends with a prefix (no object after it on that page).
	lastPrefix string
	endsPrefix bool
	// flat: the chain's last page held no folder (objects only), so it is
	// not followed: on a flat level each further page is a round trip that
	// lists objects the tiles list anyway, and finds no cut (the red team's
	// flat leaves spent 4 to 6 waves for none).
	flat bool
}

// seedBudget is what the seed has spent: every logical request, the
// empty continuing pages list follows included, and the round trips on
// the critical path (a wave costs its longest chain of requests). cut
// marks a follow-up refused, cutBy the budget it ran into first (its zero,
// stopWide, is never one).
type seedBudget struct {
	mu              sync.Mutex
	requests, trips int
	cut             bool
	cutBy           seedStop
}

// follow charges one more request to a chain that has sent k in the wave
// that started after round trip t0, or refuses it when it would pass
// seedRequests or seedRoundTrips.
func (b *seedBudget) follow(k *int, t0 int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.requests >= seedRequests:
		b.cut, b.cutBy = true, cmp.Or(b.cutBy, stopRequests)
	case t0+*k >= seedRoundTrips:
		b.cut, b.cutBy = true, cmp.Or(b.cutBy, stopRoundTrips)
	default:
		b.requests++
		*k++
		return true
	}
	return false
}

// expandSeed tiles the scope from delimiter listings of its folder
// hierarchy and emits no rows (the tiles list every key, including the
// listings' own objects):
//   - the top level is paginated by its continuation token, up to
//     seedPages pages;
//   - while a level holds fewer folders than workers, the next level under
//     each of them is listed, all at once, within the budgets (seedLevels,
//     seedRequests, seedRoundTrips);
//   - every folder found is a cut (each inside its parent folder's tile),
//     filtered by safe, sorted, and subsampled to min(maxSeeds, 4×workers);
//   - on a resume after K the listings start after K's safe start, every
//     folder above K is listed at the top, and only cuts > K are kept: the
//     first tile is the resume tile (K, c1], which drops keys up to K; a cut
//     at or below K would list published keys again.
//
// Swath's hybrid seed descent (a walk that classified each truncated level,
// sampled ambiguous folders and cut dense flat folders into radix bands)
// was ported, measured and deleted: it gained nothing on the recordings and
// cost 30-60 % more requests on the shallow ones (docs/measurements.md,
// "Policy switches"; seedplan.go in git). This one only paginates and
// descends narrow levels, in parallel waves, under fixed budgets.
func (p *Engine[P]) expandSeed(ctx context.Context, e source.Emitter[Range, P]) error {
	after := ""
	if p.opt.Resume != nil {
		after = p.opt.Resume.After
	}
	start := p.safeStart(after)
	began := time.Now()
	nodes := p.seedRoots(after, start)
	var b seedBudget
	var cuts []string
	var top seedChain
	var stop seedStop
	unsafe, levels, flat := 0, 0, 0
	firstKeys, discarded := 0, false
	var perLevel [seedLevels + 1]int64
	// publish reports what the seed spent and why it stopped, on every
	// exit: a failed or canceled seed's requests are a resume's runtime too.
	publish := func() {
		p.seedNs.Store(max(1, int64(time.Since(began))))
		p.c.Set(cSeedPrefixes, int64(len(top.prefixes)))
		p.c.Set(cSeedPages, int64(top.pages))
		p.c.Set(cSeedRequests, int64(b.requests))
		p.c.Set(cSeedRoundTrips, int64(b.trips))
		p.c.Set(cSeedLevels, int64(levels))
		for l, n := range perLevel {
			p.c.Set(seedLevelCuts[l], n)
		}
		p.c.Inc(seedStopBy[stop])
		p.c.Add(cSeedUnsafe, int64(unsafe))
		p.c.Add(cSeedFlatChains, int64(flat))
		p.c.Set(cSeedFirstPageKeys, int64(firstKeys))
		if discarded {
			p.c.Set(cSeedFirstPageDiscarded, 1)
		}
		if top.truncated {
			p.c.Set(cSeedTruncated, 1)
		}
	}
	for level := 0; ; level++ {
		var chains []*seedChain
		var err error
		if level == 0 && p.opt.SeedFirstPage {
			var page *Page[P]
			var fits bool
			chains, page, fits, err = p.seedFirst(ctx, nodes, &b, start, after)
			top = *chains[0]
			if err == nil && fits {
				firstKeys, err = p.seedEmit(page, start, after, e)
			} else if err == nil {
				discarded = true
			}
			if err == nil && fits {
				stop = stopOnePage
				p.lock()
				publish()
				p.unlock()
				return nil
			}
		} else {
			chains, err = p.seedLevel(ctx, nodes, &b)
			if level == 0 {
				top = *chains[0]
			}
		}
		if err != nil {
			stop = stopFailed
			p.lock()
			publish()
			p.unlock()
			return err
		}
		var found []string
		flatCut := false
		for _, c := range chains {
			if c.flat && c.truncated {
				flat++
				flatCut = true
			}
			for _, k := range c.prefixes {
				switch {
				case k <= after: // a resume: at or below K
				case !p.safe(k):
					unsafe++
				default:
					found = append(found, k)
				}
			}
		}
		slices.Sort(found) // the folders' lists are disjoint, each ascending
		found = slices.Compact(found)
		cuts = append(cuts, found...)
		next := make([]seedNode, len(found))
		for i, k := range found {
			next[i] = seedNode{prefix: k}
		}
		perLevel[level] = int64(len(next))
		switch {
		case b.cut: // an empty page's follow-up: the level is not listed whole
			stop = b.cutBy
		case len(next) == 0 && flatCut:
			stop = stopFlat
		case len(next) == 0:
			stop = stopLeaf
		case len(next) >= p.opt.Workers:
			stop = stopWide
		case level == seedLevels:
			stop = stopLevels
		case b.trips >= seedRoundTrips:
			stop = stopRoundTrips
		case b.requests+len(next) > seedRequests:
			stop = stopRequests
		default:
			nodes, levels = next, levels+1
			continue
		}
		break
	}
	slices.Sort(cuts)
	cuts = slices.Compact(cuts)
	p.lock()
	publish()
	if limit := Tiles(p.opt.Workers); len(cuts) > limit {
		picked := make([]string, 0, limit)
		for i := range limit {
			picked = append(picked, cuts[(i*len(cuts)+limit/2)/limit])
		}
		cuts = picked
		p.c.Set(cSeedSubsampled, 1)
	}
	p.c.Set(cSeedCuts, int64(len(cuts)))
	cuts = detach(cuts) // they become the tiles' bounds: none may pin a seed page
	// Closing sentinel: when the top level was listed whole and its last
	// page ends with a prefix pk, bound the last tile at prefixCeil(pk) so it
	// is not the open frontier (the bounded midpoint then applies to it).
	// Cuts found under pk lie below that ceiling.
	var sentinel string
	if k := len(cuts); k > 0 {
		u, ok := prefixCeil(top.lastPrefix)
		switch {
		case top.truncated:
			p.c.Inc(cSentinelDeclinedTruncated)
		case !top.endsPrefix:
			p.c.Inc(cSentinelDeclinedObjectLast)
		case !ok || u <= cuts[k-1] || u <= after:
			p.c.Inc(cSentinelDeclinedNoCeiling)
		case !p.safe(u):
			p.c.Inc(cSentinelDeclinedUnsafe)
		default:
			sentinel = u
			p.c.Set(cSeedSentinel, 1)
		}
	}
	tiles := make([]Range, 0, len(cuts)+2)
	lo := after
	for _, c := range cuts {
		tiles = append(tiles, Range{lo: lo, hi: c, id: p.ids.Add(1), born: fromSeed})
		lo = c
	}
	if sentinel != "" {
		tiles = append(tiles, Range{lo: lo, hi: sentinel, id: p.ids.Add(1), born: fromSeed})
		lo = sentinel
	}
	tiles = append(tiles, Range{lo: lo, open: true, id: p.ids.Add(1), born: fromSeed})
	// K is a real key and may not be a bound the provider accepts: the
	// resume tile starts at its longest accepted prefix and drops keys up to
	// K there.
	if start != after {
		tiles[0].lo, tiles[0].after = start, after
	}
	if p.opt.Resume != nil {
		p.c.Set(cResumedRanges, int64(len(tiles)))
		p.c.Set(cSeedResumed, 1)
	}
	p.account(time.Now())
	p.queued += len(tiles)
	p.c.Max(cMaxQueued, int64(p.queued))
	p.unlock()
	for _, t := range tiles {
		if err := e.Child(t); err != nil {
			return err
		}
	}
	return nil
}

// seedFirst is the seed's first wave: the top level's first delimiter
// pages and, in the same round trip, the scope's first data page (from
// start, the request the first tile would send). When that page is not
// truncated (fits) the scope fits in one page: the delimiter requests still
// in flight are cancelled and their error ignored, and the seed lists
// nothing more. Otherwise the page is discarded (seed.first_page_discarded:
// one request more) and the tiling is the seed's alone, from the scope's
// start: emitting the page and starting the first tile after it left the
// 12.6M-key megascenes list with a serial tail (12 s to 20 s, mean active
// 57 to 35) for the same requests and splits.
func (p *Engine[P]) seedFirst(ctx context.Context, nodes []seedNode, b *seedBudget, start, after string) (chains []*seedChain, page *Page[P], fits bool, err error) {
	q := p.query()
	q.After = start
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ferr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		page, ferr = p.list(ctx, q)
		if ferr == nil && !page.Truncated {
			cancel()
		}
	}()
	chains, err = p.seedLevel(sctx, nodes, b)
	<-done
	if ferr != nil {
		return chains, nil, false, ferr
	}
	if fits = !page.Truncated; fits {
		return chains, page, true, nil
	}
	if page.Token == "" {
		return chains, nil, false, fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
	}
	return chains, page, false, err
}

// seedEmit emits the seed's first data page when it holds the whole scope:
// its keys above after (a resume's K), checked strictly ascending above
// start; it returns how many it emitted.
func (p *Engine[P]) seedEmit(page *Page[P], start, after string, e source.Emitter[Range, P]) (int, error) {
	rows := page.Rows
	c := start
	if p.d.Groups {
		// Not truncated: every group on the page is complete.
		if _, _, err := p.groupOrder(rows, start, false, start, "", true); err != nil {
			return 0, err
		}
		rows = nil
	}
	for i := range rows {
		if (c != "" || i > 0) && rows[i].Key <= c {
			return 0, fmt.Errorf("%w: listing out of order in (%q, ⊤]: %q after %q", p.d.invalid(), start, rows[i].Key, c)
		}
		c = rows[i].Key
	}
	rows = page.Rows
	skip := 0
	for skip < len(rows) && after != "" && rows[skip].Key <= after {
		skip++
	}
	rows = rows[skip:]
	if len(rows) > 0 {
		if err := p.emit(rows, e); err != nil {
			return 0, err
		}
	}
	p.covered(start, "", true)
	return len(rows), nil
}

// seedRoots are the top level's folders: the scope; on a resume after K
// also every folder above K inside the scope, shallowest first (each lists
// the siblings after K's branch at its level), as many as the request
// budget holds. A folder the provider would not accept as a bound is
// skipped: every folder below it holds it, so none could be a cut.
func (p *Engine[P]) seedRoots(after, start string) []seedNode {
	scope := p.opt.Prefix
	roots := []seedNode{{prefix: scope}}
	for i := len(scope); i < len(after) && len(roots) < seedRequests; i++ {
		if after[i] != seedDelimiter[0] {
			continue
		}
		if f := after[:i+1]; p.safe(f) {
			roots = append(roots, seedNode{prefix: f})
		}
	}
	for i := range roots {
		if len(start) > len(roots[i].prefix) {
			roots[i].after = start // K's safe start lies inside the folder
		}
	}
	return roots
}

// seedLevel lists every node's delimiter chain, wave by wave: a wave sends
// the next page of every unfinished chain at once (one round trip) through
// the engine's client and admission. A chain ends at its last page, at
// seedPages pages, or when the next wave, or the next empty page it
// follows, would pass a budget. The chains are returned with what they
// listed also when a page fails, and their pages are counted either way.
func (p *Engine[P]) seedLevel(ctx context.Context, nodes []seedNode, b *seedBudget) ([]*seedChain, error) {
	chains := make([]*seedChain, len(nodes))
	for i, n := range nodes {
		q := p.query()
		q.Prefix, q.After, q.Delimiter, q.Probe = n.prefix, n.after, seedDelimiter, true
		chains[i] = &seedChain{q: q, truncated: true}
	}
	var err error
	for range seedPages {
		var active []*seedChain
		for _, c := range chains {
			if c.truncated && !c.flat {
				active = append(active, c)
			}
		}
		if len(active) == 0 || b.cut || b.trips >= seedRoundTrips || b.requests+len(active) > seedRequests {
			break
		}
		t0 := b.trips
		b.requests += len(active)
		sent := make([]int, len(active)) // each chain's requests in this wave
		g, gctx := errgroup.WithContext(ctx)
		for i, c := range active {
			sent[i] = 1
			g.Go(func() error {
				return p.seedPage(gctx, c, func() bool { return b.follow(&sent[i], t0) })
			})
		}
		err = g.Wait()
		b.trips = t0 + slices.Max(sent)
		if err != nil {
			break
		}
	}
	pages := 0
	for _, c := range chains {
		pages += c.pages
	}
	p.c.Add(cPages, int64(pages))
	return chains, err
}

// seedPage lists the next page of c's chain; follow charges each empty
// continuing page list follows.
func (p *Engine[P]) seedPage(ctx context.Context, c *seedChain, follow func() bool) error {
	out, err := p.listFollowing(ctx, c.q, follow)
	if err != nil {
		return err
	}
	if out.Truncated && out.Token == "" {
		return fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
	}
	c.pages++
	c.prefixes = append(c.prefixes, out.Prefixes...)
	c.truncated, c.q.Token = out.Truncated, out.Token
	c.flat = len(out.Prefixes) == 0
	if n := len(out.Prefixes); n > 0 || len(out.Rows) > 0 {
		c.endsPrefix = false
		if n > 0 {
			c.lastPrefix = out.Prefixes[n-1]
			c.endsPrefix = len(out.Rows) == 0 || out.Rows[len(out.Rows)-1].Key < c.lastPrefix
		}
	}
	return nil
}
