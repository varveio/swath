// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package order lists an Azure Blob container in unsigned byte order over
// List Blobs, which lists in UTF-16 code-unit order: the two disagree only
// where the first differing scalars of two names are one in U+E000..U+FFFF
// (lead bytes EE..EF) and one at U+10000 and above (F0..F4). It is the dual
// of the replay's cursor (replay/azure/cursor.go), which reads a
// byte-sorted store in Azure's order.
//
// A query's scope is cut into byte intervals (spans) that are also
// intervals in Azure's order and are entered at their minimum in both; a
// span is read by startFrom and markers until an item that is not a member
// ends it. Inside a span an item at U+10000 and above whose sibling group
// at U+E000..U+FFFF is still pending splits the span lazily, so items read
// once are emitted in byte order without another read when the service has
// already passed them. The specification is
// notes/plans/2026-11-08-azblob-order-adapter-spec.md in the notes
// repository; section references below are to it.
package order

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/counters"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/utf16order"
)

// Lister is the call the adapter makes (*client.Client).
type Lister interface {
	List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error)
}

// The adapter's counters' offsets in CounterDefs.
const (
	CSpans = iota
	CSplits
	CSeeks
	CProbes
	COverlapRows
	CEmptyPages
	CStackMax
	CBufferRowsMax
	CResponses
	CChainsEvicted
	CChainsMax
)

// CounterDefs are the adapter's counters (§6), for the provider to
// register after its own.
var CounterDefs = []counters.Def{
	{Key: "order_spans", Kind: counters.Count, Unit: "spans", Help: "byte-order spans beyond each chain's first: cuts at bounds holding names at U+E000 and above, and split children"},
	{Key: "order_splits", Kind: counters.Count, Unit: "splits", Help: "spans split at a name at U+10000 and above whose U+E000..U+FFFF siblings were pending"},
	{Key: "order_seeks", Kind: counters.Count, Unit: "requests", Help: "listing requests that entered a span by startFrom"},
	{Key: "order_probes", Kind: counters.Count, Unit: "spans", Help: "span reads that ended without a member"},
	{Key: "order_overlap_rows", Kind: counters.Count, Unit: "items", Help: "listed items discarded past a span's end"},
	{Key: "order_empty_pages", Kind: counters.Count, Unit: "pages", Help: "empty continuing pages followed inside a span's read"},
	{Key: "order_stack_max", Kind: counters.Max, Unit: "spans", Help: "the most spans a chain held"},
	{Key: "order_buffer_rows_max", Kind: counters.Max, Unit: "items", Help: "the most items a chain held read and not yet emitted"},
	{Key: "order_responses", Kind: counters.Count, Unit: "responses", Help: "listing responses the adapter consumed, empty continuing pages and the work of failed pages included"},
	{Key: "order_chains_evicted", Kind: counters.Count, Unit: "chains", Help: "idle chains dropped, least recently used first, past MaxChains; their tokens are refused"},
	{Key: "order_chains_max", Kind: counters.Max, Unit: "chains", Help: "the most chains an adapter held registered"},
}

// MaxChains is the most chains one adapter keeps registered; past it the
// least recently used idle chain is evicted and its token refused
// (ErrToken). The engine abandons chains routinely (a structure probe
// drops its continuation, a split drops its parent's), so without a cap
// they would accumulate until Release. 64 is far above what one range has
// in flight: its listing chain plus a few probes.
const MaxChains = 64

// maxNameUnits is the longest Azure name, in UTF-16 code units.
const maxNameUnits = 1024

// units is s's length in UTF-16 code units.
func units(s string) int {
	n := 0
	for _, r := range s {
		n++
		if r >= 0x10000 {
			n++
		}
	}
	return n
}

// stackBound is the most spans a chain over these bounds can hold; a
// deeper stack is an invariant violation (§6). §3.1 cuts at most three
// points per scalar of each bound, so the first spans number at most
// 3 × (units(after) + units(through)) + 1. A split adds E, S and the rest,
// whose successor rewrite re-cuts at most two pieces per scalar of Q,
// so at most 2 + 2 × maxNameUnits spans; splits nest at most once per
// scalar of a name, maxNameUnits levels.
func stackBound(after, through string) int {
	return 3*(units(after)+units(through)) + 1 + maxNameUnits*(2+2*maxNameUnits)
}

var (
	// ErrToken: a continuation token that is not the last one its chain
	// returned, or that names no live chain.
	ErrToken = errors.New("order: unknown or stale continuation token")
	// ErrInvariant: the adapter's state contradicts itself, which only a
	// page outside the listing contract (out of Azure's order) or a bug
	// can cause. It is returned as a protocol violation wrapping the
	// client's invalid page, so the engine fails the range as it fails any
	// invalid page.
	ErrInvariant = errors.New("order: invariant violated")
)

func invariant(format string, args ...any) error {
	return source.Violation(fmt.Errorf("%w (%w): %s", ErrInvariant, client.ErrInvalidPage, fmt.Sprintf(format, args...)))
}

// Page is one page in byte order: blobs and, with a delimiter, BlobPrefix
// roll-ups (Item.Prefix), together ascending in unsigned byte order. Rows
// plus prefixes never exceed the query's Max.
type Page struct {
	Items     []client.Item
	Truncated bool
	Token     string // the continuation token when Truncated
}

// Adapter serves keyrange queries in byte order over a Lister. Each chain
// of pages (a first query and the tokens it returns) is held in the
// adapter's registry until it returns its last page or Release; safe for
// concurrent use by different chains.
type Adapter struct {
	l         Lister
	container string
	set       *counters.Set
	base      counters.ID

	mu     sync.Mutex
	chains map[uint64]*chain
	next   uint64
	clock  uint64 // ticks on every commit: a chain's used is its last tick

	// trace, when set (tests), sees every span created.
	trace func(span)
}

// New builds an adapter listing container through l. The counters of
// CounterDefs are added to set from ID base (the provider's registration);
// a nil set gives the adapter its own.
func New(l Lister, container string, set *counters.Set, base counters.ID) *Adapter {
	if set == nil {
		set, base = counters.New(CounterDefs), 0
	}
	return &Adapter{l: l, container: container, set: set, base: base, chains: map[uint64]*chain{}}
}

// Counter is the value of counter i of CounterDefs.
func (a *Adapter) Counter(i int) int64 { return a.set.Load(a.base + counters.ID(i)) }

// Len is the number of chains registered.
func (a *Adapter) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.chains)
}

// Release drops every registered chain (the provider's Expand, however it
// returns); their tokens are refused afterwards.
func (a *Adapter) Release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	clear(a.chains)
}

func (a *Adapter) add(i int, n int64) { a.set.Add(a.base+counters.ID(i), n) }
func (a *Adapter) max(i int, n int64) { a.set.Max(a.base+counters.ID(i), n) }

// chain is one chain's read state (§4.3): the spans still to emit, a stack
// in byte order whose top is last.
type chain struct {
	prefix, delimiter string
	after, through    string
	bounded           bool

	stack []span
	limit int // stackBound of the chain's bounds
	ids   int
	seq   uint64
	used  uint64
	last  string // the last name emitted
	any   bool   // a name was emitted
}

func (c *chain) same(q *keyrange.Query) bool {
	return c.prefix == q.Prefix && c.delimiter == q.Delimiter && c.after == q.After && c.through == q.Through && c.bounded == q.Bounded
}

// clone is a copy-on-write view: span values copied, buffers clipped so an
// append in the view never writes where the original can see.
func (c *chain) clone() *chain {
	v := *c
	v.stack = make([]span, len(c.stack), len(c.stack)+4)
	copy(v.stack, c.stack)
	for i := range v.stack {
		b := v.stack[i].buf
		v.stack[i].buf = b[:len(b):len(b)]
	}
	return &v
}

// newSpan gives s an id and reports it to the trace.
func (a *Adapter) newSpan(c *chain, s span) span {
	c.ids++
	s.id = c.ids
	if a.trace != nil {
		a.trace(s)
	}
	return s
}

func (c *chain) index(id int) int {
	for i := range c.stack {
		if c.stack[i].id == id {
			return i
		}
	}
	return -1
}

// Fetch lists one page of q in byte order (keyrange.Fetch's contract,
// §2). A failed or cancelled read discards the page's work: the same query
// and token may be asked again and return the page a fault-free run would.
func (a *Adapter) Fetch(ctx context.Context, q *keyrange.Query) (*Page, error) {
	limit := q.Max
	if limit <= 0 || limit > client.MaxPageSize {
		limit = client.MaxPageSize
	}
	var id uint64
	var base, c *chain
	if q.Token == "" {
		c = &chain{prefix: q.Prefix, delimiter: q.Delimiter, after: q.After, through: q.Through, bounded: q.Bounded,
			limit: stackBound(q.After, q.Through)}
		spans := decompose(normalise(q.Prefix, q.After, q.Through, q.Bounded), q.Through)
		for i := len(spans) - 1; i >= 0; i-- {
			c.stack = append(c.stack, a.newSpan(c, spans[i]))
		}
		if len(spans) > 1 {
			a.add(CSpans, int64(len(spans)-1))
		}
		a.max(CStackMax, int64(len(c.stack)))
		if len(c.stack) > c.limit {
			return nil, invariant("%d spans, bound %d", len(c.stack), c.limit)
		}
	} else {
		var seq uint64
		var err error
		if id, seq, err = parseToken(q.Token); err != nil {
			return nil, err
		}
		a.mu.Lock()
		base = a.chains[id]
		a.mu.Unlock()
		if base == nil || base.seq != seq || !base.same(q) {
			return nil, ErrToken
		}
		c = base.clone()
	}
	page, err := a.fill(ctx, c, limit)
	if err != nil {
		return nil, err
	}
	c.seq++
	a.mu.Lock()
	defer a.mu.Unlock()
	if base != nil && a.chains[id] != base {
		return nil, ErrToken
	}
	switch {
	case page.Truncated:
		if id == 0 {
			a.next++
			id = a.next
		}
		a.clock++
		c.used = a.clock
		a.chains[id] = c
		page.Token = strconv.FormatUint(id, 10) + ":" + strconv.FormatUint(c.seq, 10)
		for len(a.chains) > MaxChains {
			a.evict()
		}
		a.max(CChainsMax, int64(len(a.chains)))
	case id != 0:
		delete(a.chains, id)
	}
	return page, nil
}

// evict drops the least recently used chain; a.mu is held.
func (a *Adapter) evict() {
	var oldest uint64
	var lru *chain
	for id, c := range a.chains {
		if lru == nil || c.used < lru.used {
			oldest, lru = id, c
		}
	}
	delete(a.chains, oldest)
	a.add(CChainsEvicted, 1)
}

func parseToken(t string) (id, seq uint64, err error) {
	a, b, ok := strings.Cut(t, ":")
	if ok {
		if id, err = strconv.ParseUint(a, 10, 64); err == nil {
			seq, err = strconv.ParseUint(b, 10, 64)
		}
	}
	if !ok || err != nil || id == 0 {
		return 0, 0, ErrToken
	}
	return id, seq, nil
}

// fill assembles one page (§4.3, page assembly): members from the top
// span's buffer, reading or splitting as needed, until limit or the end.
func (a *Adapter) fill(ctx context.Context, c *chain, limit int) (*Page, error) {
	p := &Page{}
	for len(p.Items) < limit && len(c.stack) > 0 {
		top := &c.stack[len(c.stack)-1]
		if len(top.buf) == 0 {
			if top.st == exhausted {
				c.stack = c.stack[:len(c.stack)-1]
				continue
			}
			if err := a.read(ctx, c, len(c.stack)-1, limit); err != nil {
				return nil, err
			}
			continue
		}
		it := top.buf[0]
		split, err := a.split(c, it.Name)
		if err != nil {
			return nil, err
		}
		if split {
			continue
		}
		if c.any && it.Name <= c.last {
			return nil, invariant("%q emitted after %q", it.Name, c.last)
		}
		top.buf = top.buf[1:]
		c.last, c.any = it.Name, true
		p.Items = append(p.Items, it)
	}
	for n := len(c.stack); n > 0 && c.stack[n-1].st == exhausted && len(c.stack[n-1].buf) == 0; n-- {
		c.stack = c.stack[:n-1]
	}
	p.Truncated = len(c.stack) > 0
	return p, nil
}

// read issues the next read of span i: a seek when it is unread, else its
// marker with the query the marker was issued for.
func (a *Adapter) read(ctx context.Context, c *chain, i, limit int) error {
	sp := &c.stack[i]
	if sp.st == unread {
		from := seekFrom(sp.lo)
		wire := from
		if from == c.prefix {
			wire = "" // the same start, and no startFrom to refuse
		}
		sp.sk = seek{from: from, wire: wire, excl: !sp.loInc}
		sp.st = reading
		a.add(CSeeks, 1)
	}
	out, err := a.l.List(ctx, &client.ListInput{Container: a.container, Prefix: c.prefix, Delimiter: c.delimiter,
		StartFrom: sp.sk.wire, Marker: sp.sk.marker, MaxResults: limit})
	if err != nil {
		return err
	}
	a.add(CResponses, 1)
	if len(out.Items) == 0 && out.NextMarker != "" {
		a.add(CEmptyPages, 1)
	}
	a.consume(c, i, out.Items, out.NextMarker)
	rows := 0
	for k := range c.stack {
		rows += len(c.stack[k].buf)
	}
	a.max(CBufferRowsMax, int64(rows))
	return nil
}

// consume applies §4.1 to one response for span i.
func (a *Adapter) consume(c *chain, i int, items []client.Item, marker string) {
	sp := &c.stack[i]
	for k := range items {
		n := items[k].Name
		sp.frontier = n
		if sp.sk.excl && n == sp.sk.from {
			continue // rule 1: the exclusive endpoint
		}
		if items[k].Prefix && !sp.sk.accepted && n < sp.lo {
			continue // rule 2: the roll-up of the subtree the seek landed in
		}
		if !sp.contains(n, c.prefix) {
			a.end(sp) // rule 3: past every member
			if !a.transfer(c, i, items[k:], marker) {
				a.add(COverlapRows, int64(len(items)-k))
			}
			return
		}
		sp.buf = append(sp.buf, items[k])
		sp.sk.accepted = true
	}
	if marker == "" {
		a.end(sp)
	} else {
		sp.sk.marker = marker
	}
}

func (a *Adapter) end(sp *span) {
	if !sp.sk.accepted {
		a.add(CProbes, 1)
	}
	sp.st, sp.sk.marker = exhausted, ""
}

// transfer hands the rest of an E child's response, from its first
// non-member, to the rest sibling that holds it (§4.3, E ends): the read
// serves both. The rest spans before that one in Azure's order were passed
// by the stream with no member, so they are exhausted. It reports false,
// leaving the rest spans to seek, when span i is not an E child, a rest
// span has state of its own, or none holds the item.
func (a *Adapter) transfer(c *chain, i int, items []client.Item, marker string) bool {
	e := &c.stack[i]
	if len(e.rest) == 0 {
		return false
	}
	at := make([]int, len(e.rest))
	to := -1
	for k, id := range e.rest {
		j := c.index(id)
		if j < 0 || c.stack[j].st != unread || len(c.stack[j].buf) > 0 {
			return false
		}
		at[k] = j
		if c.stack[j].contains(items[0].Name, c.prefix) {
			to = j
		}
	}
	if to < 0 {
		return false
	}
	floor := seekFrom(c.stack[to].lo)
	for _, j := range at {
		if utf16order.Compare(seekFrom(c.stack[j].lo), floor) < 0 {
			c.stack[j].st = exhausted
		}
	}
	r := &c.stack[to]
	r.st, r.sk = reading, e.sk
	r.sk.marker, r.sk.accepted = "", true
	a.consume(c, to, items, marker)
	return true
}

// split splits the top span before emitting name when name has a scalar at
// U+10000 and above at a boundary j whose U+E000..U+FFFF siblings under
// Q = name[:j] intersect the span (§4.2), the first such j. The parent is
// replaced by E(Q), S(Q) and the rest beyond Q, each clipped to it; its
// buffer goes to the children by byte membership, its read to the child
// holding its frontier (§4.3, split).
func (a *Adapter) split(c *chain, name string) (bool, error) {
	i := len(c.stack) - 1
	t := c.stack[i]
	q, found := "", false
	for j := len(c.prefix); j < len(name); {
		_, size := utf8.DecodeRuneInString(name[j:])
		if v := name[j]; v >= leadS && v <= 0xF4 {
			q = name[:j]
			if t.lo < cut(q, leadS) && (t.hi == "" || cut(q, leadE) < t.hi) {
				found = true
				break
			}
		}
		j += size
	}
	if !found {
		return false, nil
	}
	a.add(CSplits, 1)
	var kids []span
	e := t.clip(cut(q, leadE), cut(q, leadS))
	hasE := !e.empty()
	if hasE {
		kids = append(kids, e)
	}
	if s := t.clip(cut(q, leadS), cut(q, leadBeyond)); !s.empty() {
		kids = append(kids, s)
	}
	var rest []span
	if t.hi == "" || cut(q, leadBeyond) < t.hi {
		rest = pieces(cut(q, leadBeyond), true, t.hi, nil, len(c.prefix))
	}
	kids = append(kids, rest...)
	for k := range kids {
		kids[k] = a.newSpan(c, kids[k])
	}
	if hasE {
		for _, r := range kids[len(kids)-len(rest):] {
			kids[0].rest = append(kids[0].rest, r.id)
		}
	}
	for _, it := range t.buf {
		k := 0
		for k < len(kids) && !kids[k].contains(it.Name, c.prefix) {
			k++
		}
		if k == len(kids) {
			return false, invariant("buffered %q is in no child of a split at %q", it.Name, q)
		}
		kids[k].buf = append(kids[k].buf, it)
	}
	if t.st == exhausted {
		for k := range kids {
			kids[k].st = exhausted
		}
	} else {
		owner := -1
		for k := range kids {
			if kids[k].contains(t.frontier, c.prefix) {
				owner = k
			}
		}
		if owner < 0 {
			return false, invariant("frontier %q is in no child of a split at %q", t.frontier, q)
		}
		floor := seekFrom(kids[owner].lo)
		for k := range kids {
			switch {
			case k == owner:
				kids[k].st, kids[k].sk, kids[k].frontier = t.st, t.sk, t.frontier
			case utf16order.Compare(seekFrom(kids[k].lo), floor) < 0:
				kids[k].st = exhausted
			case len(kids[k].buf) > 0:
				return false, invariant("a child after the owner of a split at %q holds %q", q, kids[k].buf[0].Name)
			}
		}
	}
	c.stack = c.stack[:i]
	for k := len(kids) - 1; k >= 0; k-- {
		c.stack = append(c.stack, kids[k])
	}
	if len(kids) > 1 {
		a.add(CSpans, int64(len(kids)-1))
	}
	a.max(CStackMax, int64(len(c.stack)))
	if len(c.stack) > c.limit {
		return false, invariant("%d spans, bound %d", len(c.stack), c.limit)
	}
	return true, nil
}
