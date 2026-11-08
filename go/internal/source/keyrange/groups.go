// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"slices"
	"strings"

	"github.com/varveio/swath/go/internal/schema"
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
	open  bool            // the last emitted key's group may continue
	run   int64           // rows of the open group emitted so far
	win   *tokenWindow    // tokens of continuing pages that showed no greater key
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
// The emitted rows and the held rows share no array, and where a page is
// split between them the smaller side by rows is copied out of the page
// (Description.Detach): a few emitted rows must not pin a page whose rest
// is held (the order stage charges only the rows it is handed), nor a few
// held rows a page that was emitted. The held side then views at most one
// page beside the one in flight.
func (p *Engine[P]) take(g *groups[P], rows []schema.Row[P], done bool, c string) (emit []schema.Row[P], last string, fresh bool) {
	top, _ := g.prev(c)
	fresh = len(rows) > 0 && (top == "" || rows[len(rows)-1].Key > top)
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
	// groups.max_rows: the batch's first group, continuing the open
	// group's emitted rows when it repeats its key, and its trailing one.
	lead := 1
	for lead < len(all) && all[lead].Key == all[0].Key {
		lead++
	}
	first := int64(lead)
	if g.open && all[0].Key == c {
		first += g.run
	}
	p.c.Max(cGroupsMaxRows, max(first, int64(len(all)-j)))
	g.run = 0
	if done {
		g.open = false
		return all, k, fresh
	}
	if j == 0 {
		// All one key: emitted as it arrives, its group left open.
		g.open, g.run = true, first
		p.c.Inc(cGroupsOpenPages)
		return all, k, fresh
	}
	g.open = false
	emit, held := all[:j:j], all[j:]
	p.c.Add(cGroupsHeldRows, int64(len(held)))
	switch fromPage := j - len(old); {
	case fromPage == 0 && owned:
		// The emitted rows are the previous held rows, already copied.
		g.held = slices.Clone(held)
	case fromPage < len(held) && p.d.Detach != nil:
		emit = p.d.Detach(emit)
		g.held = slices.Clone(held)
	case p.d.Detach != nil:
		g.held, g.owned = p.d.Detach(held), true
	default:
		g.held = slices.Clone(held)
	}
	return emit, all[j-1].Key, fresh
}

// progress checks a continuing page that showed no key above the chain's
// last (a hot key's page, or one whose rows a resume dropped): its token
// must not be among the window of such pages' tokens since the last fresh
// page, or the chain cycles. A fresh page clears the window.
func (p *Engine[P]) progress(g *groups[P], fresh bool, sent, token string) error {
	if fresh {
		g.win = nil
		return nil
	}
	if g.win == nil {
		g.win = newTokenWindow(strings.Clone(sent))
	}
	if !g.win.add(strings.Clone(token)) {
		return fmt.Errorf("%w: a continuing page within one key's group makes no progress (its token repeats)", p.d.invalid())
	}
	return nil
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
