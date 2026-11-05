// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"context"

	"github.com/varveio/swath/go/internal/source"
)

// expandSeed runs the seed descent (seedplan.go) and emits its tiles; it
// emits no rows (the tiles list every key, including the probes' objects).
// The walk asks for one probe at a time; where its next probes are known
// (the folders queued at the same depth, the rest of a sample) they are
// fetched together, up to one per worker, so a level costs about one round
// trip. Results the walk does not ask for are counted and dropped; the plan
// is the one the walk would make probing serially.
func (p *Engine[P]) expandSeed(ctx context.Context, e source.Emitter[Range, P]) error {
	if !p.opt.SeedDescent {
		return p.expandSeedPage(ctx, e)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type fetch struct {
		done chan struct{}
		out  seedOutcome
		err  error
	}
	fetches := map[seedProbe]*fetch{}
	get := func(q seedProbe) *fetch {
		f := &fetch{done: make(chan struct{})}
		fetches[q] = f
		go func() {
			defer close(f.done)
			in := p.query()
			in.Prefix, in.After, in.Delimiter, in.Max, in.Probe = q.prefix, q.after, seedDelimiter, seedPage, true
			out, err := p.list(ctx, in)
			if err != nil {
				f.err = err
				return
			}
			f.out = seedOutcome{prefixes: out.Prefixes, capped: out.Truncated, objects: len(out.Rows)}
			for _, o := range out.Rows {
				if !f.out.hasLast || o.Key > f.out.last {
					f.out.last, f.out.hasLast = o.Key, true
				}
			}
			for _, cp := range out.Prefixes {
				if !f.out.hasLast || cp > f.out.last {
					f.out.last, f.out.hasLast = cp, true
				}
			}
			p.mu.Lock()
			p.c.Inc(cPages)
			p.mu.Unlock()
		}()
		return f
	}
	d := newDescent(p.opt.Prefix, p.opt.Workers)
	q, res := d.start()
	used := map[seedProbe]bool{}
	for res == nil {
		f := fetches[q]
		if f == nil {
			f = get(q)
			for _, a := range d.lookahead(p.opt.Workers - 1) {
				if fetches[a] == nil {
					get(a)
				}
			}
		}
		select {
		case <-f.done:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		if f.err != nil {
			return f.err
		}
		if q.prefix == p.opt.Prefix && q.after == "" {
			p.mu.Lock()
			p.c.Set(cSeedPrefixes, int64(len(f.out.prefixes)))
			p.mu.Unlock()
		}
		used[q] = true
		q, res = d.next(f.out)
	}
	cancel()
	for _, f := range fetches {
		<-f.done
	}
	var cuts []string
	p.mu.Lock()
	p.c.Set(cSeedProbes, int64(res.probes))
	p.c.Set(cSeedProbesUnused, int64(len(fetches)-len(used)))
	p.c.Set(cSeedRadixBands, int64(res.synthesized))
	for reason, n := range res.marks {
		if id, ok := seedMarks[reason]; ok {
			p.c.Add(id, int64(n))
		}
	}
	for class, n := range res.levels {
		if id, ok := seedLevels[class]; ok {
			p.c.Add(id, int64(n))
		}
	}
	for _, c := range res.cuts {
		if p.safe(c) {
			cuts = append(cuts, c)
		} else {
			p.c.Inc(cSeedUnsafe)
		}
	}
	p.c.Set(cSeedCuts, int64(len(cuts)))
	tiles := make([]Range, 0, len(cuts)+1)
	lo := ""
	for _, c := range cuts {
		tiles = append(tiles, Range{lo: lo, hi: c, id: p.ids.Add(1), born: fromSeed})
		lo = c
	}
	tiles = append(tiles, Range{lo: lo, open: true, id: p.ids.Add(1), born: fromSeed})
	p.queued += len(tiles)
	p.c.Max(cMaxQueued, int64(p.queued))
	p.mu.Unlock()
	for _, t := range tiles {
		if err := e.Child(t); err != nil {
			return err
		}
	}
	return nil
}
