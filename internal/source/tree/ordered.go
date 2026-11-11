// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
)

// The ordered mode: a store whose directory listing is ordered by name and
// seekable by name within a directory (Azure Blob on a hierarchical
// namespace). A directory's first page is read whole; a directory larger
// than a page hands its rest to the range engine as one segment
// (keyrange.Engine.Within), which splits it and reads it in parallel. The
// engine works on keys, the store on names: a file's key is its name, a
// directory's its name + "/", so a directory sorts after its siblings that
// extend its name with a byte below '/' (a-b < a/), while the store lists it
// first. fetch translates (docs/internals/tree-sources.md, "The ordered
// mode: names to keys").

// Entry is one entry of a directory as a Lister lists it.
type Entry[P any] struct {
	Name string // below the directory: not empty, no '/'
	Dir  bool
	Data P
}

// DirQuery asks for one page of a directory.
type DirQuery struct {
	Dir   string // the directory's key prefix ("" or ending in '/')
	After string // names strictly after this one; "" from the first
	Token string // the previous page's token: the chain's next page, every other field unchanged
	Max   int    // the page size asked
}

// DirPage is one page of a directory: entries ascending by name in
// unsigned byte order.
type DirPage[P any] struct {
	Entries   []Entry[P]
	Truncated bool
	Token     string // the continuation when Truncated
}

// Lister is the provider half of the ordered mode.
type Lister[P any] interface {
	// List lists one page of a directory.
	List(ctx context.Context, q *DirQuery) (*DirPage[P], error)
	// Lookup reports the entry named name in directory dir when it is a
	// directory (one request; ok false when there is none, or it is a file).
	Lookup(ctx context.Context, dir, name string) (e Entry[P], ok bool, err error)
}

// The ordered mode's counters' offsets in OrderedCounters.
const (
	cFirstPages = iota
	cDirsWhole
	cDirsSplit
	cHeldDirs
	cResolvePages
	cDroppedRows
	cCandidateDirs
	cLookups
)

// OrderedCounters are the ordered mode's engagement counters, registered
// after the provider's own on the engine's set (NewOrdered).
var OrderedCounters = []counters.Def{
	{Key: "tree.first_pages", Kind: counters.Count, Unit: "requests", Help: "directories' first pages (every directory listed costs one)"},
	{Key: "tree.dirs_whole", Kind: counters.Count, Unit: "directories", Help: "directories that fit their first page"},
	{Key: "tree.dirs_split", Kind: counters.Count, Unit: "directories", Help: "directories larger than a page, whose rest went to the range engine as a segment"},
	{Key: "tree.held_dirs", Kind: counters.Count, Unit: "directories", Help: "directory rows emitted after a sibling the store lists after them (a-b before a/)"},
	{Key: "tree.resolve_pages", Kind: counters.Count, Unit: "requests", Help: "name pages read past a page's end because every entry read was a directory row held"},
	{Key: "tree.dropped_rows", Kind: counters.Count, Unit: "rows", Help: "rows read again after a seek and dropped: at or before the range's start key"},
	{Key: "tree.candidate_dirs", Kind: counters.Count, Unit: "directories", Help: "directories behind a range's seek by name that belong to it by key (part before part-0015)"},
	{Key: "tree.lookups", Kind: counters.Count, Unit: "requests", Help: "directory lookups for such candidates outside the names already read"},
}

// OrderedRange is a range of the ordered mode: a directory (named by its
// prefix alone) or a range-engine segment of one.
type OrderedRange = Range[struct{}, keyrange.Range]

// OrderedOut is where the ordered mode puts a range's content.
type OrderedOut[P any] = Out[struct{}, keyrange.Range, P]

// Ordered is the Reader of the ordered mode, over one range engine shared
// by every directory's segment.
type Ordered[P any] struct {
	l    Lister[P]
	eng  *keyrange.Engine[P]
	base counters.ID
	page int

	mu    sync.Mutex
	known map[string]*known[P] // per directory: what has been read of it
}

var _ Reader[struct{}, keyrange.Range, int] = (*Ordered[int])(nil)

// NewOrdered builds the ordered mode over l, with a range engine of
// options eo and description d (the provider's; its Counters are followed
// by OrderedCounters). The engine's fetch is the mode's own; eo should turn
// off the seed and structure probes, which are for a flat scope.
func NewOrdered[P any](l Lister[P], eo keyrange.Options, d keyrange.Description[P]) (*Ordered[P], error) {
	o := &Ordered[P]{l: l, known: map[string]*known[P]{}}
	n := len(d.Counters)
	d.Counters = slices.Concat(d.Counters, OrderedCounters)
	eng, err := keyrange.New(eo, d, o.fetch)
	if err != nil {
		return nil, err
	}
	o.eng, o.base, o.page = eng, eng.ID(n), eng.Options().PageSize
	return o, nil
}

// Engine is the range engine the segments run on (its counters hold the
// provider's and the mode's).
func (o *Ordered[P]) Engine() *keyrange.Engine[P] { return o.eng }

func (o *Ordered[P]) add(c int, n int64) { o.eng.Set().Add(o.base+counters.ID(c), n) }

// isDirRow: in this mode a key ends in '/' exactly when it is a directory's.
func isDirRow[P any](row *schema.Row[P]) (struct{}, Walk) {
	if strings.HasSuffix(row.Key, "/") {
		return struct{}{}, Descend
	}
	return struct{}{}, Leaf
}

// Read lists a directory's first page, and hands a larger directory's rest
// to the engine; or runs a segment on the engine.
func (o *Ordered[P]) Read(ctx context.Context, r OrderedRange, out *OrderedOut[P]) error {
	if r.InSeg {
		return o.eng.Expand(ctx, r.Seg, segEmitter[P]{out})
	}
	o.add(cFirstPages, 1)
	k := o.know(r.Prefix)
	q := &keyrange.Query{Prefix: r.Prefix, Max: o.page}
	var pg *keyrange.Page[P]
	for {
		var err error
		if pg, err = o.list(ctx, q, k); err != nil {
			return err
		}
		if !pg.Truncated || len(pg.Rows) > 0 {
			break
		}
		if pg.Token == "" {
			return source.Violation(fmt.Errorf("%w: truncated directory page without a token in %q", keyrange.ErrInvalidPage, r.Prefix))
		}
		q.Token = pg.Token // an empty continuing page
	}
	if !pg.Truncated {
		o.forget(r.Prefix)
		o.add(cDirsWhole, 1)
		return out.Rows(pg.Rows, isDirRow[P])
	}
	o.add(cDirsSplit, 1)
	c := strings.Clone(pg.Rows[len(pg.Rows)-1].Key)
	if err := out.Rows(pg.Rows, isDirRow[P]); err != nil {
		return err
	}
	return out.Seg(o.eng.Within(r.Prefix, c))
}

// segEmitter is a segment's emitter for the engine: rows go through the
// walker's splice, child ranges become segments of the same directory.
type segEmitter[P any] struct{ out *OrderedOut[P] }

func (e segEmitter[P]) Rows(rows []schema.Row[P]) error { return e.out.Rows(rows, isDirRow[P]) }
func (e segEmitter[P]) Child(r keyrange.Range) error    { return e.out.Seg(r) }

// fetch is the engine's page fetch for segments.
func (o *Ordered[P]) fetch(ctx context.Context, q *keyrange.Query) (*keyrange.Page[P], error) {
	return o.list(ctx, q, o.know(q.Prefix))
}

// start is where a query's name listing starts: the names strictly after
// name; and A's candidates, the directories that sort at or before A by
// name and after it by key (A starts with the name and the next byte is
// below '/', or A is the name), shortest first. empty: the query's start is
// past the directory.
func start(dir, a string) (name string, candidates []string, empty bool) {
	if !strings.HasPrefix(a, dir) {
		return "", nil, a > dir
	}
	rest := a[len(dir):]
	name = rest
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		name = rest[:i]
	}
	for i := 1; i <= len(name); i++ {
		if i == len(rest) || (i < len(name) && rest[i] < '/') {
			candidates = append(candidates, name[:i])
		}
	}
	return name, candidates, false
}

// Tokens the mode returns to the engine. tokenNext continues a name chain:
// the chain's start key (its names start after that key's, its rows after
// the key) and the lister's token. tokenAfter restarts the name listing
// after a key, the page's last, when the page ended with directories held:
// their rows sort after it, and each is one of its candidates or a name
// after its own, read again.
const (
	tokenNext  = "n"
	tokenAfter = "a"
)

// nextToken is a tokenNext: the start key's length, ':', the key, the
// lister's token.
func nextToken(from, inner string) string {
	return tokenNext + strconv.Itoa(len(from)) + ":" + from + inner
}

// parseToken reads a token the mode returned: the start key and the
// lister's token ("" to restart after the key).
func parseToken(t string) (from, inner string, err error) {
	if after, ok := strings.CutPrefix(t, tokenAfter); ok {
		return after, "", nil
	}
	if rest, ok := strings.CutPrefix(t, tokenNext); ok {
		if l, tail, ok := strings.Cut(rest, ":"); ok {
			if n, err := strconv.Atoi(l); err == nil && n >= 0 && n <= len(tail) && tail[n:] != "" {
				return tail[:n], tail[n:], nil
			}
		}
	}
	return "", "", source.Violation(fmt.Errorf("%w: tree: a continuation token the mode did not issue", keyrange.ErrInvalidPage))
}

// list reads one page of keys in (q.After, q.Through] of directory
// q.Prefix in key order. Entries come in name order; a directory's row is
// held until an entry whose key passes it arrives. A page that ends with
// rows held ends at its last emitted key c, and its token restarts the
// listing after c: a held directory sorts at or before c by name, and is
// one of c's candidates or a name after c's, read again. Rows at or before
// the start key are dropped; the start key's candidates start held. Keys
// past q.Through are left to the engine to trim; a directory still held
// then belongs to the next range, whose candidates find it.
func (o *Ordered[P]) list(ctx context.Context, q *keyrange.Query, k *known[P]) (*keyrange.Page[P], error) {
	dir, from, token := q.Prefix, q.After, ""
	if q.Token != "" {
		var err error
		if from, token, err = parseToken(q.Token); err != nil {
			return nil, err
		}
	}
	name, cands, empty := start(dir, from)
	if empty {
		return &keyrange.Page[P]{}, nil
	}
	type held struct {
		row  schema.Row[P]
		rows int // len(rows) when it was held
	}
	var holds []held // a stack: the top, last, has the smallest key
	var rows []schema.Row[P]
	put := func(row schema.Row[P]) {
		if from != "" && row.Key <= from {
			o.add(cDroppedRows, 1)
			return
		}
		rows = append(rows, row)
	}
	release := func(below string, all bool) {
		for n := len(holds); n > 0 && (all || holds[n-1].row.Key < below); n-- {
			h := holds[n-1]
			if len(rows) > h.rows {
				o.add(cHeldDirs, 1)
			}
			put(h.row)
			holds = holds[:n-1]
		}
	}
	if token == "" {
		for _, c := range cands {
			e, ok, err := o.lookup(ctx, k, dir, c)
			if err != nil {
				return nil, err
			}
			if ok {
				o.add(cCandidateDirs, 1)
				holds = append(holds, held{row: schema.Row[P]{Key: Key(dir, c, true), Data: e.Data}})
			}
		}
	}
	in := &DirQuery{Dir: dir, After: name, Token: token, Max: q.Max}
	prev := name
	for first := true; ; first = false {
		if !first {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			o.add(cResolvePages, 1)
		}
		pg, err := o.l.List(ctx, in)
		if err != nil {
			return nil, err
		}
		for i := range pg.Entries {
			e := &pg.Entries[i]
			if e.Name == "" || strings.IndexByte(e.Name, '/') >= 0 || (prev != "" || i > 0) && e.Name <= prev {
				return nil, source.Violation(fmt.Errorf("%w: directory %q listed %q after %q", keyrange.ErrInvalidPage, dir, e.Name, prev))
			}
			prev = e.Name
			key := Key(dir, e.Name, e.Dir)
			release(key, false)
			if e.Dir {
				holds = append(holds, held{row: schema.Row[P]{Key: key, Data: e.Data}, rows: len(rows)})
			} else {
				put(schema.Row[P]{Key: key, Data: e.Data})
			}
		}
		k.read(in, pg)
		in.Token = pg.Token
		switch {
		case !pg.Truncated:
			release("", true)
			return &keyrange.Page[P]{Rows: rows}, nil
		case pg.Token == "":
			return nil, source.Violation(fmt.Errorf("%w: truncated directory page without a token in %q", keyrange.ErrInvalidPage, dir))
		case len(holds) == 0:
			return &keyrange.Page[P]{Rows: rows, Truncated: true, Token: nextToken(from, pg.Token)}, nil
		case len(rows) > 0:
			// Rows held: end at the last key emitted and restart after it.
			return &keyrange.Page[P]{Rows: rows, Truncated: true, Token: tokenAfter + strings.Clone(rows[len(rows)-1].Key)}, nil
		}
		// Only held rows so far: read on, so that the page makes progress.
	}
}

// lookup resolves a candidate: from what was read of the directory, else
// by the Lister (recorded).
func (o *Ordered[P]) lookup(ctx context.Context, k *known[P], dir, name string) (Entry[P], bool, error) {
	if e, ok, sure := k.dirAt(name); sure {
		return e, ok, nil
	}
	o.add(cLookups, 1)
	e, ok, err := o.l.Lookup(ctx, dir, name)
	if err != nil {
		return Entry[P]{}, false, err
	}
	if ok && (e.Name != name || !e.Dir) {
		return Entry[P]{}, false, source.Violation(fmt.Errorf("%w: lookup of %q in %q answered %q", keyrange.ErrInvalidPage, name, dir, e.Name))
	}
	k.found(name, e, ok)
	return e, ok, nil
}

// know is the record of directory dir, made on first use.
func (o *Ordered[P]) know(dir string) *known[P] {
	o.mu.Lock()
	defer o.mu.Unlock()
	k := o.known[dir]
	if k == nil {
		k = &known[P]{dirs: map[string]Entry[P]{}}
		o.known[dir] = k
	}
	return k
}

// forget drops a directory's record once nothing more of it will be read
// (it fit its first page). A split directory's record is kept for the run:
// its segments' candidates are resolved from it.
func (o *Ordered[P]) forget(dir string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.known, dir)
}

// known is what has been read of one directory: the name intervals listed
// and the directories in them, and candidates looked up.
type known[P any] struct {
	mu    sync.Mutex
	spans []span
	dirs  map[string]Entry[P] // directories seen or looked up
	none  []string            // candidates looked up and absent
}

// span is a name interval read whole: (lo, hi], or [lo, hi] with loIn; to
// the directory's end with end.
type span struct {
	lo, hi    string
	loIn, end bool
}

func (s span) holds(n string) bool {
	return (n > s.lo || s.loIn && n == s.lo) && (s.end || n <= s.hi)
}

// read records a page: its names (after in.After on a chain's first page,
// from its first entry on a continuation) and its directories.
func (k *known[P]) read(in *DirQuery, pg *DirPage[P]) {
	k.mu.Lock()
	defer k.mu.Unlock()
	s := span{lo: in.After, end: !pg.Truncated}
	if in.Token != "" {
		if len(pg.Entries) == 0 {
			return // a continuation's position is its token's: nothing to record
		}
		s.lo, s.loIn = pg.Entries[0].Name, true
	}
	if n := len(pg.Entries); n > 0 {
		s.hi = pg.Entries[n-1].Name
	} else {
		s.hi = s.lo
	}
	k.spans = append(k.spans, s)
	for _, e := range pg.Entries {
		if e.Dir {
			k.dirs[e.Name] = e
		}
	}
}

// dirAt reports what is known of name: sure when it lies in a read
// interval or was looked up, then whether it is a directory.
func (k *known[P]) dirAt(name string) (Entry[P], bool, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if e, ok := k.dirs[name]; ok {
		return e, true, true
	}
	if slices.Contains(k.none, name) {
		return Entry[P]{}, false, true
	}
	for _, s := range k.spans {
		if s.holds(name) {
			return Entry[P]{}, false, true
		}
	}
	return Entry[P]{}, false, false
}

// found records a lookup's answer.
func (k *known[P]) found(name string, e Entry[P], ok bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if ok {
		k.dirs[name] = e
	} else {
		k.none = append(k.none, name)
	}
}
