// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"math"
	"runtime"
	"slices"
	"sort"
	"sync"
	"time"
)

// The window cache's defaults: 192 windows of 12,500 rows, 2.4 million
// decoded rows at most. A sequential walk of 1,000-key pages then reads
// the fixture once per twelve pages. A walk holds two windows while it
// reads the last quarter of one, its own and the next one read ahead
// (readAhead); 192 keep both for a fleet of 96 walks, the Java server's
// 96 windows of one apiece, without evicting a window a walk still reads
// when the fleet's walks reach their windows' ends together.
const (
	DefaultPrefetchWindowRows = 12_500
	DefaultPrefetchMaxWindows = 192
)

const (
	// fillRampFactor grows a walk's fill from page to page until it
	// reaches the window size: a one-shot reader never pays for rows it
	// will not read, and a walk converges on whole windows in a few pages.
	fillRampFactor = 4
	// anchorsPerWindow continuation anchors are kept per window, enough
	// for a fleet of max-windows walks to keep its own. An anchor is a
	// hint: losing one costs one fill that does not ramp.
	anchorsPerWindow = 4
)

// Prefetch configures the window cache in front of a sorted fixture's
// range reads.
type Prefetch struct {
	// Off serves every range read from the fixture.
	Off bool
	// WindowRows is the rows a walk's fill ramps to; zero selects
	// DefaultPrefetchWindowRows.
	WindowRows int
	// MaxWindows bounds the windows held, and with WindowRows the rows
	// held (MaxWindows × WindowRows); zero selects
	// DefaultPrefetchMaxWindows.
	MaxWindows int
}

// PrefetchMeters receives the window cache's events. A nil func is skipped.
type PrefetchMeters struct {
	// Hit is a range read answered from cached windows.
	Hit func(struct{})
	// Miss is a range read that filled from the fixture: reason
	// continuation (its lower bound was the tail of a page served before,
	// so the fill ramps) or cold (any other).
	Miss func(reason string)
	// Fill is one fill's duration (reader slot wait included) and the
	// rows it asked for.
	Fill func(d time.Duration, requested int)
	// Anchor is a continuation anchor registered, claimed or
	// evicted_before_claim.
	Anchor func(event string)
	// RampCeiling is the ceiling a claimed anchor's ramp may reach.
	RampCeiling func(rows int)
	// Eviction is a window evicted to make room; rowBudget reports that
	// the row bound (not only the window count) required it.
	Eviction func(rowBudget bool)
	// Selection is how a read was served: single_exact_bound,
	// single_contained_bound, two_window_join, insufficient or no_cover.
	Selection func(reason string)
	// Join is why a two-window join was declined, or hit.
	Join func(reason string)
	// Uncached is a read or a window kept out of the cache:
	// request_exceeds_window, rounded_exceeds_row_budget,
	// window_exceeds_row_budget.
	Uncached func(reason string)
}

// PrefetchStats are the window cache's live counts.
type PrefetchStats struct {
	Windows, Anchors int
	Rows             int64
}

// bound is a lower or upper key bound; a nil key is open.
type bound struct {
	key       []byte
	inclusive bool
}

// windowKey identifies a window: its fill's upper bound, projection and
// lower bound. The position is part of it, so walks at different places
// in the keyspace with the same bounds keep their own windows.
type windowKey struct {
	to, lower        string
	hasTo, hasLower  bool
	owner, inclusive bool
}

func keyOf(to []byte, proj Projection, lower []byte, inclusive bool) windowKey {
	return windowKey{to: string(to), hasTo: to != nil, owner: proj.Owner, lower: string(lower), hasLower: lower != nil, inclusive: inclusive}
}

// window is one fill's rows, immutable once published: the contiguous
// rows of [lower, to) from lower on. final reports the fill returned fewer
// rows than it asked for, so no row of [lower, to) follows the last.
type window struct {
	key   windowKey
	rows  packed
	lower bound
	to    []byte
	proj  Projection
	final bool
	// fill is the rows the window's read asked for; walk reports a
	// continuation's or a read-ahead's window, whose reader reads on.
	fill int
	walk bool
}

// coversLower reports whether every row at or after from (after it when
// !inclusive) up to the window's last row is in the window.
func (w *window) coversLower(from []byte, inclusive bool) bool {
	if w.lower.key == nil {
		return true
	}
	if from == nil {
		return false
	}
	switch c := bytes.Compare(from, w.lower.key); {
	case c > 0:
		return true
	case c < 0:
		return false
	}
	return w.lower.inclusive || !inclusive
}

// probe is the rows [start, end) of a window a read qualifies for;
// complete reports that no qualifying row follows end.
type probe struct {
	start, end int
	complete   bool
}

func (p probe) size() int { return p.end - p.start }

func (w *window) probe(from []byte, inclusive bool, to []byte) (probe, bool) {
	if !w.coversLower(from, inclusive) {
		return probe{}, false
	}
	start := w.startIndex(from, inclusive)
	end := w.rows.len()
	if to != nil {
		end = w.startIndex(to, true)
	}
	return probe{start: start, end: max(start, end), complete: end < w.rows.len() || w.final}, true
}

// startIndex is the first row at or after from (after it when !inclusive).
func (w *window) startIndex(from []byte, inclusive bool) int {
	if from == nil {
		return 0
	}
	return sort.Search(w.rows.len(), func(i int) bool {
		c := bytes.Compare(w.rows.key(i), from)
		return c > 0 || c == 0 && inclusive
	})
}

// slice unpacks up to limit rows of [start, end).
func (w *window) slice(start, end, limit int) []Object {
	end = min(start+limit, end)
	return w.rows.objects(make([]Object, 0, end-start), start, end)
}

// upperContains reports whether a window filled up to cached holds every
// row a read up to requested may need.
func upperContains(cached, requested []byte) bool {
	return cached == nil || requested != nil && bytes.Compare(cached, requested) >= 0
}

// compareLower orders lower bounds, an open bound first.
func compareLower(a, b []byte) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	return bytes.Compare(a, b)
}

// prefetcher is a window cache in front of a range reader: a sequential
// walk's fill decodes a window of rows once for the pages it reads next,
// and the pages after it are sliced from the window without a reader slot.
// The fixture is immutable, so a slice of a window is byte-identical to a
// read of the same bounds; a read the cached windows cannot prove whole is
// read from the fixture. One mutex guards the window list and the anchors;
// windows are immutable, so a hit reads one outside it, and fills run
// outside it, so concurrent walks never queue on another's fill.
type prefetcher struct {
	read func(ctx context.Context, from []byte, inclusive bool, to []byte, limit int, proj Projection) (packed, error)
	// direct reads unpacked and uncached, for a read whose rows overflow
	// a packed arena (errPackedOverflow).
	direct     func(ctx context.Context, from []byte, inclusive bool, to []byte, limit int, proj Projection) ([]Object, error)
	windowRows int
	maxWindows int
	maxRows    int64
	maxAnchors int
	meters     PrefetchMeters

	ctx    context.Context // the read-aheads', ended by close
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// aheadSlots bounds the read-aheads decoding at once to half the
	// processors: a fleet's read-aheads start together, and taking every
	// reader slot and processor at once would stall the pages being
	// served from windows, the work they exist to keep smooth.
	aheadSlots chan struct{}

	mu      sync.Mutex
	ahead   map[windowKey]chan struct{} // read-aheads in flight, closed when done
	windows []*window                   // least recently used first
	live    int64
	anchors map[string]*list.Element
	order   *list.List // of *anchor, least recently used first
}

type anchor struct {
	key  string
	fill int
}

func newPrefetcher(cfg Prefetch, read func(context.Context, []byte, bool, []byte, int, Projection) (packed, error),
	direct func(context.Context, []byte, bool, []byte, int, Projection) ([]Object, error)) *prefetcher {
	rows, windows := cfg.WindowRows, cfg.MaxWindows
	if rows <= 0 {
		rows = DefaultPrefetchWindowRows
	}
	if windows <= 0 {
		windows = DefaultPrefetchMaxWindows
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &prefetcher{
		read: read, direct: direct, windowRows: rows, maxWindows: windows, maxRows: int64(rows) * int64(windows),
		maxAnchors: int(min(int64(windows)*anchorsPerWindow, math.MaxInt32)),
		anchors:    make(map[string]*list.Element), order: list.New(), ahead: make(map[windowKey]chan struct{}),
		ctx: ctx, cancel: cancel, aheadSlots: make(chan struct{}, max(1, runtime.GOMAXPROCS(0)/2)),
	}
}

// stats returns the live counts.
func (p *prefetcher) stats() PrefetchStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PrefetchStats{Windows: len(p.windows), Anchors: len(p.anchors), Rows: p.live}
}

func call[T any](f func(T), v T) {
	if f != nil {
		f(v)
	}
}

// serve answers a range read from the windows, or fills one.
func (p *prefetcher) serve(ctx context.Context, from []byte, inclusive bool, to []byte, limit int, proj Projection) ([]Object, error) {
	// A walk that reaches its window's end while the next window's
	// read-ahead is in flight waits for it rather than read the same rows.
	if from != nil && !inclusive {
		p.mu.Lock()
		done := p.ahead[keyOf(to, proj, from, false)]
		p.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	m := &p.meters
	if limit > p.windowRows {
		call(m.Uncached, "request_exceeds_window")
		call(m.Miss, "cold")
		filled, err := p.fill(ctx, from, inclusive, to, limit, proj)
		if errors.Is(err, errPackedOverflow) {
			return p.direct(ctx, from, inclusive, to, limit, proj)
		}
		if err != nil {
			return nil, err
		}
		return filled.objects(make([]Object, 0, filled.len()), 0, filled.len()), nil
	}
	all := p.snapshot()
	var candidates []*window
	for _, w := range all {
		if w.proj == proj && upperContains(w.to, to) {
			candidates = append(candidates, w)
		}
	}
	// The tightest lower bound first.
	slices.SortStableFunc(candidates, func(a, b *window) int { return compareLower(b.lower.key, a.lower.key) })
	covered := false
	var partials []*window
	for _, w := range candidates {
		pr, ok := w.probe(from, inclusive, to)
		if !ok || pr.size() == 0 && !pr.complete {
			continue
		}
		covered = true
		if pr.size() >= limit || pr.complete {
			page := w.slice(pr.start, pr.end, limit)
			if bytes.Equal(w.to, to) && (w.to == nil) == (to == nil) {
				call(m.Selection, "single_exact_bound")
			} else {
				call(m.Selection, "single_contained_bound")
			}
			call(m.Hit, struct{}{})
			p.mu.Lock()
			p.touch(w)
			p.registerAnchors(page, p.ceilingRows(limit))
			p.readAhead(w, pr.start+len(page), limit)
			p.mu.Unlock()
			return page, nil
		}
		partials = append(partials, w)
	}
	// Joins only after every single window had its chance: a tighter
	// short window must not hide an older sufficient one.
	decline := ""
	for _, w := range partials {
		pr, _ := w.probe(from, inclusive, to)
		page, second, reason := p.join(w, pr, all, to, limit, proj)
		if page != nil {
			call(m.Join, "hit")
			call(m.Selection, "two_window_join")
			call(m.Hit, struct{}{})
			p.mu.Lock()
			p.touch(w)
			p.touch(second)
			p.registerAnchors(page, p.ceilingRows(limit))
			p.mu.Unlock()
			return page, nil
		}
		if decline == "" {
			decline = reason
		}
	}
	if decline != "" {
		call(m.Join, decline)
	}
	if covered {
		call(m.Selection, "insufficient")
	} else {
		call(m.Selection, "no_cover")
	}
	requested, continuation := p.rampedFill(from, inclusive, limit)
	if continuation && int64(p.ceilingRows(limit)) > p.maxRows {
		requested = limit
		call(m.Uncached, "rounded_exceeds_row_budget")
	}
	if continuation {
		call(m.Miss, "continuation")
	} else {
		call(m.Miss, "cold")
	}
	filled, err := p.fill(ctx, from, inclusive, to, requested, proj)
	if errors.Is(err, errPackedOverflow) {
		// Rows this large are not kept: the read is served as asked,
		// unpacked, and its walk fills afresh from its next page.
		return p.direct(ctx, from, inclusive, to, limit, proj)
	}
	if err != nil {
		return nil, err
	}
	n := min(limit, filled.len())
	served := filled.objects(make([]Object, 0, n), 0, n)
	final := filled.len() < requested
	// A final window stays useful to walks behind this one even when this
	// read took all of it; a cold read with nothing left over is a
	// one-shot probe and is not kept.
	var w *window
	if filled.len() > 0 && (filled.len() > n || final) {
		from, to = bytes.Clone(from), bytes.Clone(to)
		w = &window{key: keyOf(to, proj, from, inclusive), rows: filled, lower: bound{from, inclusive}, to: to, proj: proj,
			final: final, fill: requested, walk: continuation}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if w != nil {
		p.publish(w)
		p.readAhead(w, n, limit)
	}
	next := p.nextFill(requested, limit)
	if int64(p.ceilingRows(limit)) > p.maxRows {
		next = limit
	}
	p.registerAnchors(served, next)
	return served, nil
}

// readAhead starts the fill of the window after w once a walk's page
// leaves less than a quarter of w unread, so the walk finds its next
// window filled. Walks paced alike (a latency profile, a fleet started
// together) otherwise reach their windows' ends at the same moment, and
// their fills queue for the reader slots at once, each page waiting on
// the others' decodes. The fill reads what the walk's next fill would
// (its ramp included) from w's last key on, under the store's own
// context, takes a reader slot as any fill does, and is kept as a
// window like one; a failed fill is dropped, and the walk then fills
// for itself. Called under mu.
func (p *prefetcher) readAhead(w *window, served, limit int) {
	if !w.walk || w.final || w.rows.len()-served > w.fill/4 || p.ctx.Err() != nil {
		return
	}
	last := bytes.Clone(w.rows.key(w.rows.len() - 1))
	key := keyOf(w.to, w.proj, last, false)
	if p.ahead[key] != nil {
		return
	}
	for _, o := range p.windows {
		if o.proj == w.proj && upperContains(o.to, w.to) && o.coversLower(last, false) {
			if pr, _ := o.probe(last, false, w.to); pr.size() > 0 || pr.complete {
				return
			}
		}
	}
	done := make(chan struct{})
	p.ahead[key] = done
	n, to, proj := p.nextFill(w.fill, limit), w.to, w.proj
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var filled packed
		err := p.ctx.Err()
		select {
		case p.aheadSlots <- struct{}{}:
			filled, err = p.fill(p.ctx, last, false, to, n, proj)
			<-p.aheadSlots
		case <-p.ctx.Done():
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.ahead, key)
		close(done)
		if err == nil && filled.len() > 0 && p.ctx.Err() == nil {
			p.publish(&window{key: key, rows: filled, lower: bound{last, false}, to: to, proj: proj,
				final: filled.len() < n, fill: n, walk: true})
		}
	}()
}

// close stops the read-aheads and waits for them.
func (p *prefetcher) close() {
	p.cancel()
	p.wg.Wait()
}

// fill reads from the fixture.
func (p *prefetcher) fill(ctx context.Context, from []byte, inclusive bool, to []byte, n int, proj Projection) (packed, error) {
	started := time.Now()
	rows, err := p.read(ctx, from, inclusive, to, n, proj)
	if f := p.meters.Fill; f != nil {
		f(time.Since(started), n)
	}
	return rows, err
}

// snapshot returns the windows; they are immutable, so the read continues
// outside the lock.
func (p *prefetcher) snapshot() []*window {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.windows)
}

// join serves a read from first's partial slice and the next rows of
// another window that covers the key after first's last qualifying row.
func (p *prefetcher) join(first *window, pr probe, candidates []*window, to []byte, limit int, proj Projection) ([]Object, *window, string) {
	last := first.rows.key(pr.end - 1)
	needed := limit - pr.size()
	var best *window
	var bestProbe probe
	gap, projection, upper, insufficient := false, false, false, false
	for _, second := range candidates {
		if second == first {
			continue
		}
		if !second.coversLower(last, false) {
			if second.proj == proj && upperContains(second.to, to) && compareLower(second.lower.key, last) > 0 {
				gap = true
			}
			continue
		}
		if second.proj != proj {
			projection = true
			continue
		}
		if !upperContains(second.to, to) {
			upper = true
			continue
		}
		sp, ok := second.probe(last, false, to)
		if !ok || sp.size() < needed && !sp.complete {
			insufficient = true
			continue
		}
		if best == nil || sp.size() > bestProbe.size() {
			best, bestProbe = second, sp
		}
	}
	if best == nil {
		switch {
		case insufficient:
			return nil, nil, "insufficient_second"
		case upper:
			return nil, nil, "upper_bound"
		case projection:
			return nil, nil, "projection"
		case gap:
			return nil, nil, "gap"
		}
		return nil, nil, "no_second"
	}
	take := min(needed, bestProbe.size())
	page := make([]Object, 0, pr.size()+take)
	page = first.rows.objects(page, pr.start, pr.end)
	page = best.rows.objects(page, bestProbe.start, bestProbe.start+take)
	return page, best, ""
}

// touch marks w most recently used, unless it was evicted meanwhile.
// Called under mu.
func (p *prefetcher) touch(w *window) {
	if i := slices.Index(p.windows, w); i >= 0 {
		p.windows = append(slices.Delete(p.windows, i, i+1), w)
	}
}

// publish adds w, replacing a window of the same key, evicting the least
// recently used windows past either bound. Called under mu.
func (p *prefetcher) publish(w *window) {
	n := int64(w.rows.len())
	if n > p.maxRows {
		call(p.meters.Uncached, "window_exceeds_row_budget")
		return
	}
	if i := slices.IndexFunc(p.windows, func(o *window) bool { return o.key == w.key }); i >= 0 {
		p.live -= int64(p.windows[i].rows.len())
		p.windows = slices.Delete(p.windows, i, i+1)
	}
	for len(p.windows) > 0 && (len(p.windows) >= p.maxWindows || p.live+n > p.maxRows) {
		rowBudget := p.live+n > p.maxRows
		p.live -= int64(p.windows[0].rows.len())
		p.windows[0] = nil
		p.windows = p.windows[1:]
		call(p.meters.Eviction, rowBudget)
	}
	p.windows = append(p.windows, w)
	p.live += n
}

// rampedFill is how many rows a miss reads: never fewer than limit, and
// more only for a paginating client. Such a client identifies itself: its
// next lower bound (an S3 start-after or continuation token, exclusive) is
// a key at the tail of the page it was just served, which was registered
// as an anchor carrying the next fill's size. A probe or a seek into
// unvisited keyspace reads what it asked for.
func (p *prefetcher) rampedFill(from []byte, inclusive bool, limit int) (int, bool) {
	if from == nil || inclusive {
		return limit, false
	}
	p.mu.Lock()
	e, ok := p.anchors[string(from)]
	if ok {
		p.order.Remove(e)
		delete(p.anchors, string(from))
	}
	p.mu.Unlock()
	if !ok {
		return limit, false
	}
	call(p.meters.Anchor, "claimed")
	call(p.meters.RampCeiling, p.ceilingRows(limit))
	return max(limit, min(e.Value.(*anchor).fill, p.ceilingRows(limit))), true
}

// ceilingRows is the window size rounded up to a multiple of limit, so a
// walk's pages tile its windows.
func (p *prefetcher) ceilingRows(limit int) int {
	rounded := (int64(p.windowRows) + int64(limit) - 1) / int64(limit) * int64(limit)
	return int(min(rounded, math.MaxInt32))
}

func (p *prefetcher) nextFill(requested, limit int) int {
	return int(min(int64(p.ceilingRows(limit)), int64(requested)*fillRampFactor, math.MaxInt32))
}

// registerAnchors registers the last two keys of a served page (the pager
// reads one row past the page to detect truncation, so either can be the
// next lower bound) with the next fill's size. Called under mu.
func (p *prefetcher) registerAnchors(page []Object, fill int) {
	for i := max(0, len(page)-2); i < len(page); i++ {
		key := string(page[i].Key)
		if e, ok := p.anchors[key]; ok {
			e.Value.(*anchor).fill = fill
			p.order.MoveToBack(e)
		} else {
			p.anchors[key] = p.order.PushBack(&anchor{key: key, fill: fill})
		}
		call(p.meters.Anchor, "registered")
		for len(p.anchors) > p.maxAnchors {
			oldest := p.order.Front()
			p.order.Remove(oldest)
			delete(p.anchors, oldest.Value.(*anchor).key)
			call(p.meters.Anchor, "evicted_before_claim")
		}
	}
}
