// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"bytes"
	"context"

	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
)

// item is a Blob (object set) or a rolled BlobPrefix spelled name.
type item struct {
	name   []byte
	object *store.Object
}

func (it item) isPrefix() bool { return it.object == nil }

// source reads up to limit items of [from, upper) in byte order.
type source func(ctx context.Context, from []byte, inclusive bool, upper []byte, limit int) ([]item, error)

// probe returns the first store key at or after from in byte order (after
// it when inclusive is false), or nil when there is none.
type probe func(ctx context.Context, from []byte, inclusive bool) ([]byte, error)

// maxBatch is the most items one store read asks for.
const maxBatch = 4096

// cursor reads items in provider order from byte ranges of the byte-sorted
// store.
//
// Each range is read in byte order, a batch at a time. An item is next in
// provider order unless firstSwap finds an EE/EF byte at position j whose
// F0..F4 siblings under the same j-byte parent Q are still inside the
// range. The rest of the range then splits in three: [Q+F0, Q+F5) is read
// first, then the EE/EF branch from this item up to Q+F0, then
// [Q+F5, upper). Items already read go to the part they belong to, so a
// split costs a store read only when a part has not been read yet. A range
// with no such pair of branches is read exactly as in byte order.
//
// A bound or a name with many lead bytes yields hundreds of nested parts,
// and most are usually empty. Parts are queued in provider order, which is
// not byte order, so when a read comes back empty one probe for the first
// key at or after the lowest pending store position settles every part
// below that key wherever it is queued, and parts split off later inherit
// what it found. A read that finds rows costs what it would without the
// split; each empty one adds at most one probe.
type cursor struct {
	read   source
	probe  probe
	parts  []*part // a stack: the next part in provider order is last
	splits int
}

func newCursor(read source, pr probe, ranges []span) *cursor {
	c := &cursor{read: read, probe: pr}
	for i := len(ranges) - 1; i >= 0; i-- {
		r := ranges[i]
		c.parts = append(c.parts, &part{from: r.from, inclusive: r.inclusive, upper: r.upper})
	}
	return c
}

func (c *cursor) top() *part { return c.parts[len(c.parts)-1] }

func (c *cursor) pop() *part {
	p := c.top()
	c.parts = c.parts[:len(c.parts)-1]
	return p
}

// next returns the next item in provider order, ok=false at the end.
// wanted sizes a store read when one is needed: how many items the caller
// still expects to take.
func (c *cursor) next(ctx context.Context, wanted int) (item, bool, error) {
	for len(c.parts) > 0 {
		p := c.top()
		if len(p.buffered) == 0 {
			if p.exhausted {
				c.pop()
				continue
			}
			if err := p.fill(ctx, c.read, min(max(wanted, 2), maxBatch)); err != nil {
				return item{}, false, err
			}
			if len(p.buffered) == 0 {
				if err := c.probeLowest(ctx); err != nil {
					return item{}, false, err
				}
			}
			continue
		}
		it := p.buffered[0]
		at := firstSwap(it.name, p.upper)
		if at < 0 {
			p.buffered = p.buffered[1:]
			return it, true, nil
		}
		c.split(c.pop(), it.name, at)
	}
	return item{}, false, nil
}

func (c *cursor) split(p *part, name []byte, at int) {
	c.splits++
	f0, f5 := child(name, at, 0xf0), child(name, at, 0xf5)
	tail := p.upper == nil || bytes.Compare(f5, p.upper) < 0
	branch := &part{from: p.from, inclusive: p.inclusive, upper: f0}
	supplementary := &part{from: f0, inclusive: true, upper: p.upper}
	children := []*part{branch, supplementary}
	var rest *part
	if tail {
		supplementary.upper = f5
		rest = &part{from: f5, inclusive: true, upper: p.upper}
		children = append(children, rest)
	}
	last := p.buffered[len(p.buffered)-1].name
	for _, it := range p.buffered {
		switch {
		case bytes.Compare(it.name, f0) < 0:
			branch.buffered = append(branch.buffered, it)
		case rest == nil || bytes.Compare(it.name, f5) < 0:
			supplementary.buffered = append(supplementary.buffered, it)
		default:
			rest.buffered = append(rest.buffered, it)
		}
	}
	for _, ch := range children {
		if p.exhausted || ch.upper != nil && bytes.Compare(last, ch.upper) >= 0 {
			ch.exhausted = true
		} else if len(ch.buffered) > 0 {
			ch.from, ch.inclusive = p.from, p.inclusive
		}
	}
	if p.probed {
		for _, ch := range children {
			ch.learn(p.from, p.inclusive, p.next)
		}
	}
	if rest != nil {
		c.parts = append(c.parts, rest)
	}
	c.parts = append(c.parts, branch, supplementary)
}

// probeLowest probes once from the lowest store position still to be read
// and not yet probed, then applies the key found to every part: below it a
// part is exhausted, holding it a part needs no probe.
func (c *cursor) probeLowest(ctx context.Context) error {
	var lowest *part
	for i := len(c.parts) - 1; i >= 0; i-- {
		p := c.parts[i]
		if !p.exhausted && !p.probed && (lowest == nil || p.startsBelow(lowest)) {
			lowest = p
		}
	}
	if lowest == nil {
		return nil
	}
	from, inclusive := lowest.from, lowest.inclusive
	key, err := c.probe(ctx, from, inclusive)
	if err != nil {
		return err
	}
	for _, p := range c.parts {
		p.learn(from, inclusive, key)
	}
	return nil
}

// part is a byte range still to be read in provider order: buffered holds
// items read in byte order and not yet returned, from is where the next
// store read starts.
type part struct {
	buffered  []item
	upper     []byte
	from      []byte
	inclusive bool
	exhausted bool
	// probed means next is the first store key at or after from (nil for
	// none).
	probed bool
	next   []byte
}

// startsBelow reports whether p's store position is below o's, an
// inclusive one first on a tie.
func (p *part) startsBelow(o *part) bool {
	c := bytes.Compare(p.from, o.from)
	return c < 0 || c == 0 && p.inclusive && !o.inclusive
}

// learn applies a probe from at: key is the first store key at or after it
// (after it when inclusive is false), nil for none. A part whose store
// position is not below at is exhausted when its range ends at or before
// key.
func (p *part) learn(at []byte, inclusive bool, key []byte) {
	c := bytes.Compare(p.from, at)
	if p.exhausted || c < 0 || c == 0 && p.inclusive && !inclusive {
		return
	}
	if key == nil || p.upper != nil && bytes.Compare(p.upper, key) <= 0 {
		p.exhausted = true
		return
	}
	if pos := bytes.Compare(key, p.from); pos > 0 || pos == 0 && p.inclusive {
		p.probed, p.next = true, key
	}
}

func (p *part) fill(ctx context.Context, read source, limit int) error {
	p.probed = false
	got, err := read(ctx, p.from, p.inclusive, p.upper, limit)
	if err != nil {
		return err
	}
	p.buffered = append(p.buffered, got...)
	if len(got) < limit {
		p.exhausted = true
		return nil
	}
	last := got[len(got)-1]
	if !last.isPrefix() {
		p.from, p.inclusive = last.name, false
		return nil
	}
	after, end := s3.Successor(last.name)
	if end {
		p.exhausted = true
		return nil
	}
	p.from, p.inclusive = after, true
	return nil
}
