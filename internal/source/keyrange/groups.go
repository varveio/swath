// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"slices"
	"strings"

	"github.com/varveio/swath/internal/schema"
)

// Group mode (Description.Groups): a versions listing returns a key once
// per version, as consecutive rows (the key's group), and a page may end
// inside a group. Every consumer downstream of the engine treats "the last
// key seen" as finished: a split starts its lower child after it, a part
// cut and the journal record it as published, a resume lists after it. So
// within one chain (an expandRange call, or the sequential chain) the
// engine emits a row only when its group is proven complete, by a later
// key or the chain's end, and holds the trailing group of a continuing
// page back for the next page (at most one page of rows, kept within the
// chain: no state crosses a split or a retry).
//
// A page that is all one key cannot be held (the next may be the same key
// again, for as many pages as the key has versions): it is emitted as it
// arrives and the chain's group is open. While open the range does not
// split (a split's lower child would start after the key and lose the
// rest of its versions), and its pages must carry a token the chain has
// not sent within the token window, or the page makes no progress. The
// group closes at a later key, the range's bound or the chain's end.
//
// A split happens only at the last emitted key, which is complete: both
// children are ordinary fresh ranges, and the lower child lists the held
// rows again through its own seek (groups.split_relisted_rows).

// groups is one chain's group state.
type groups[P any] struct {
	held  []schema.Row[P] // the trailing group of the last page, not yet emitted
	owned bool            // held was copied out of its page (Description.Detach)
	// emitted: the chain has emitted a row, so its cursor is a complete
	// key it listed (before that it is the range's lo, and a split there
	// would hand on the whole range again without progress).
	emitted bool
	open    bool  // the last emitted key's group may continue
	run     int64 // rows of the open group emitted so far
}

// order checks a page's rows in group mode: nondecreasing, the first above
// prev, or equal to it where same (prev's group may continue); rows past
// hi (bounded) are trimmed: kept is the rows within, reached that one lay
// beyond. lo, hi and open name the range in errors (open: (lo, ⊤]).
func (p *Engine[P]) groupOrder(rows []schema.Row[P], prev string, same bool, lo, hi string, open bool) (kept int, reached bool, err error) {
	for i := range rows {
		k := rows[i].Key
		if i > 0 {
			prev, same = rows[i-1].Key, true
		}
		if (prev != "" || i > 0) && (k < prev || (k == prev && !same)) {
			if open {
				return 0, false, fmt.Errorf("%w: listing out of order in (%q, ⊤]: %q after %q", p.d.invalid(), lo, k, prev)
			}
			return 0, false, fmt.Errorf("%w: listing out of order in (%q, %q]: %q after %q", p.d.invalid(), lo, hi, k, prev)
		}
		if !open && k > hi {
			return i, true, nil
		}
	}
	return len(rows), false, nil
}

// prev is the key a chain's next row is checked against (c, the last
// emitted key, or the held group's) and whether it may repeat.
func (g *groups[P]) prev(c string) (string, bool) {
	if len(g.held) > 0 {
		return g.held[0].Key, true
	}
	return c, g.open
}

// take is a checked page's rows (within the range) as the chain emits
// them: the held rows first, then the page's, all of them when the chain
// is done, else all but the trailing group, which is held (or, when the
// page is all one key, everything, the group left open). An empty
// continuing page changes nothing. c is the last emitted key before the
// page; last is after it (c when none). fresh: the page showed a key above
// every key before it (progress).
//
// Partial pages are detached on both sides: neither an outgoing batch's
// hidden row headers nor its strings may pin excluded rows or their arena.
func (p *Engine[P]) take(g *groups[P], rows []schema.Row[P], done bool, c string) (emit []schema.Row[P], last string, fresh bool) {
	top, _ := g.prev(c)
	fresh = len(rows) > 0 && (top == "" || rows[len(rows)-1].Key > top)
	if len(rows) == 0 && !done {
		return nil, c, false // an empty continuing page: nothing changes
	}
	defer func() {
		if len(emit) > 0 {
			g.emitted = true
		}
	}()
	old, owned := g.held, g.owned
	all := rows
	if len(old) > 0 {
		// A new slice: the held rows and the page's are separate arrays,
		// and the emitter takes ownership of what it is handed.
		all = make([]schema.Row[P], 0, len(old)+len(rows))
		all = append(append(all, old...), rows...)
	}
	g.held, g.owned = nil, false
	if len(all) == 0 {
		if done {
			g.open = false
		}
		return nil, c, fresh
	}
	k := all[len(all)-1].Key
	j := len(all) - 1
	for j > 0 && all[j-1].Key == k {
		j--
	}
	first := p.measureGroups(all, c, g.open, g.run)
	g.run = 0
	if done {
		g.open = false
		return all, k, fresh
	}
	if j == 0 && len(old) == 0 && !(g.open && k == c) {
		// All one new key: held whole (one page), so the key before it is
		// complete and the range may split there; a small page size makes
		// every page one key.
		g.open, g.held = false, slices.Clone(all)
		p.c.Add(cGroupsHeldRows, int64(len(all)))
		return nil, c, fresh
	}
	if j == 0 {
		// The held or open key again: emitted as it arrives (holding it
		// would grow past a page), its group left open.
		g.open, g.run = true, first
		p.c.Inc(cGroupsOpenPages)
		return all, k, fresh
	}
	g.open = false
	emit, held := all[:j:j], all[j:]
	p.c.Add(cGroupsHeldRows, int64(len(held)))
	if j == len(old) && owned {
		// Reuse the previously detached allocation, without combining it
		// with the next page's excluded headers.
		emit = old
	} else {
		emit = p.d.Detach(emit)
	}
	g.held, g.owned = p.d.Detach(held), true
	return emit, all[j-1].Key, fresh
}

// distinct is rows' keys without consecutive repeats (the density and
// pivot inputs count keys, not versions).
func distinct[P any](rows []schema.Row[P]) []string {
	keys := make([]string, 0, len(rows))
	for i := range rows {
		if i == 0 || rows[i].Key != rows[i-1].Key {
			keys = append(keys, rows[i].Key)
		}
	}
	return keys
}

// unsplittable: the chain may not split now: its last emitted key's group
// may continue, or it has emitted nothing (its cursor is still lo).
func (g *groups[P]) unsplittable() bool { return g.open || !g.emitted }

// measureGroups scans every run, including groups wholly inside a page.
// prior belongs only to the leading run when it continues the previous page.
// The return value is the leading run's cumulative size (take's hot key).
func (p *Engine[P]) measureGroups(rows []schema.Row[P], prev string, open bool, prior int64) int64 {
	var first int64
	for i := 0; i < len(rows); {
		j := i + 1
		for j < len(rows) && rows[j].Key == rows[i].Key {
			j++
		}
		n := int64(j - i)
		if i == 0 && open && rows[i].Key == prev {
			n += prior
		}
		if i == 0 {
			first = n
		}
		p.c.Max(cGroupsMaxRows, n)
		i = j
	}
	return first
}

// chainState follows the raw native chain before inclusive/resume drops.
// It is request-local, never provider-global, and never handed to a split.
type chainState struct {
	last    string
	seen    bool
	ordinal int64
	idle    int
	win     *tokenWindow
}

func (p *Engine[P]) observe(in *Query, out *Page[P]) error {
	g := in.chain
	top := g.last
	for i := range out.Rows {
		r := &out.Rows[i]
		if (g.seen || in.After != "") && (r.Key < g.last || (!g.seen && r.Key == in.After && !p.inclusive(in.After))) {
			return fmt.Errorf("%w: raw listing out of order: %q after %q", p.d.invalid(), r.Key, g.last)
		}
		if p.d.GroupOrdinal != nil {
			ordinal := p.d.GroupOrdinal(&r.Data)
			if g.seen && r.Key == g.last && ordinal <= g.ordinal {
				return fmt.Errorf("%w: ordinal %d of %q after %d", p.d.invalid(), ordinal, r.Key, g.ordinal)
			}
			g.ordinal = ordinal
		}
		g.last, g.seen = r.Key, true
	}
	fresh := len(out.Rows) > 0 && g.last > top
	g.last = strings.Clone(g.last)
	if out.Truncated {
		if out.Token == "" {
			return fmt.Errorf("%w: truncated page without a continuation token", p.d.invalid())
		}
		if fresh {
			g.win, g.idle = nil, 0
		} else {
			if g.win == nil {
				g.win = newTokenWindow(strings.Clone(in.Token))
			}
			if !g.win.add(strings.Clone(out.Token)) {
				return fmt.Errorf("%w: a continuing page makes no progress (its token repeats)", p.d.invalid())
			}
			g.idle++
		}
	}
	if fresh || !out.Truncated || g.idle <= EmptyChainLive {
		p.frontier.Add(1)
	} else {
		p.c.Inc(cEmptyChainPages)
	}
	return nil
}
