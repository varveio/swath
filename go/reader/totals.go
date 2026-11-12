// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"maps"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// PathTotals is the totals of one path requested from Totals. Its numbers
// are never partial or estimated: Totals fails instead, or leaves a value
// it cannot state exactly nil.
//
// Over a swath/local-entries/v1 dataset an object is a filesystem entry
// of any kind: Count counts every entry the path selects, a directory's
// own row "d/" included for "d/" (the root has no row, so "" counts the
// entries below it), and Bytes sums their st_size, a directory's own size
// included. Those are entry bytes: neither the files' content bytes nor
// the space they take on disk.
type PathTotals struct {
	// Count is the number of objects the path selects; zero for an empty
	// selection.
	Count int64
	// Bytes is the sum of the selected objects' sizes. It is nil when a
	// selected filesystem entry has no size (--metadata names), never a
	// partial sum.
	Bytes *int64
	// NewestModified is the latest modification time among the selected
	// objects. It is nil when Count is zero or any selected object has no
	// modification time.
	NewestModified *time.Time
	// Exact is the object whose key equals the path, when one exists. It
	// is also counted in Count and Bytes. It is always nil for "". For a
	// filesystem entry it holds the key, size and modification time.
	Exact *Object
}

// Totals returns one PathTotals per path, in input order. Paths are taken
// verbatim, without cleaning: "" selects every object; a path ending in
// "/" selects every object whose key begins with it, an object whose key
// equals it included; any other path selects only the object whose key
// equals it. Paths may repeat, nest and overlap; each total is
// independent and inclusive.
//
// Totals plans the rows of every path at once (seekparquet.Dataset.Plan
// of the paths not inside another, projecting size and
// last_modified), which checks every part holding one of them, and scans
// that plan as Scan does, in shards of consecutive rows read by
// runtime.GOMAXPROCS(0) goroutines at once over borrowed batches: a row
// several paths select is read once, and each shard's totals are added up
// when every shard has succeeded and their keys are in order across them
// (as Scan checks them). Each Count is the number of the path's rows the
// scan read: on a dataset its reads find in key order, the plan's count
// of its range, and the scan refuses one they find out of order. An exact
// object's ETag and storage class are read by a plan of those objects
// alone. Every selected object is checked as Stream checks it (a size, at
// least 0; a modification time in the years 1 to 9999), and a filesystem
// entry as StreamLocal does (its size may be null), failing with a
// SemanticError of KindRow; sizes whose sum does not fit an int64 fail
// with one naming size and no row. A row that fails is decoded again as
// Stream decodes it, so its error is the one Stream would give it. The
// first error any shard meets stops the others and is returned: with
// several rows that fail, which one's error is returned is unspecified,
// as with Scan.
//
// A versions schema holds no current objects: Totals refuses it with a
// SemanticError of KindSchema before it reads. The module's refusals
// (*seekparquet.RefusalError) and the Source's errors are returned
// unchanged, and a context that ends fails Totals with its error, checked
// before and after the reads; a closed Dataset fails it with
// seekparquet.ErrClosed, whatever the paths. No totals are returned with
// an error.
func (d *Dataset) Totals(ctx context.Context, paths []string) ([]PathTotals, error) {
	return d.totals(ctx, paths, runtime.GOMAXPROCS(0))
}

// totals is Totals, summing with up to workers goroutines at once.
func (d *Dataset) totals(ctx context.Context, paths []string, workers int) ([]PathTotals, error) {
	if err := d.usable(ctx); err != nil {
		return nil, err
	}
	columns := objectColumns(d.spec.ID)
	if columns == nil {
		return nil, notCurrent(d.spec.ID)
	}
	unique, of := distinct(paths)
	spans := make([]keySpan, len(unique))
	exact := make(map[string]int)
	for i, path := range unique {
		switch {
		case path == "":
			spans[i] = prefixSpan(nil)
		case strings.HasSuffix(path, "/"):
			spans[i] = prefixSpan([]byte(path))
			exact[path] = i
		default:
			spans[i] = keySpanOf([]byte(path))
			exact[path] = i
		}
	}

	sums, err := d.sumSpans(ctx, spans, []string{"size", "last_modified"}, nil, workers)
	if err != nil {
		return nil, err
	}
	totals := make([]PathTotals, len(unique))
	for i, s := range sums.sums {
		if s.overflow {
			return nil, semantic(KindRow, "size", nil, "the sizes %q selects overflow an int64 total", unique[i])
		}
		totals[i] = PathTotals{Count: s.count, Bytes: s.total(), NewestModified: s.newestTime()}
	}
	for path, o := range sums.exact {
		if i, ok := exact[path]; ok {
			totals[i].Exact = &o
		}
	}
	if err := d.fillExact(ctx, totals, columns[2:]); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([]PathTotals, len(paths))
	for i, u := range of {
		out[i] = totals[u]
		if e := out[i].Exact; e != nil {
			c := *e
			c.Key = bytes.Clone(e.Key)
			out[i].Exact = &c
		}
	}
	return out, nil
}

// fillExact reads the columns after size and last_modified (the ETag and
// the storage class, an Azure blob's access tier) of the exact objects
// found, by one plan of their keys.
func (d *Dataset) fillExact(ctx context.Context, totals []PathTotals, columns []string) error {
	at := make(map[string]*Object)
	var selections []seekparquet.Selection
	for i := range totals {
		if o := totals[i].Exact; o != nil && len(columns) > 0 {
			at[string(o.Key)] = o
			selections = append(selections, seekparquet.Selection{Span: seekparquet.KeySpan(o.Key)})
		}
	}
	if len(selections) == 0 {
		return nil
	}
	plan, err := d.sp.Plan(ctx, selections, columns, nil)
	if err != nil {
		return err
	}
	values := make([]seekparquet.Value, len(columns))
	return scanPlan(ctx, plan, nil, func(b *seekparquet.Batch) error {
		defer clear(values) // no borrowed value outlives its batch
		for i := range b.Len() {
			// A borrowed row: the decoder copies the strings it keeps.
			r := batchRow(b, i, columns, values)
			o := at[string(r.Key)]
			dec := decoder{spec: d.spec, row: &r}
			o.ETag, o.StorageClass = dec.string(columns[0]), dec.string(columns[1])
			if dec.err != nil {
				// Decoded again from an owning row, so the error owns its key.
				r = b.Row(i)
				dec = decoder{spec: d.spec, row: &r}
				dec.string(columns[0])
				dec.string(columns[1])
				return dec.err
			}
		}
		return nil
	})
}

// usable returns the context's error, or seekparquet.ErrClosed for a
// closed Dataset, from a plan of nothing, which reads nothing.
func (d *Dataset) usable(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := d.sp.Plan(ctx, nil, nil, nil)
	return err
}

// keySpan is the keys [lo, hi) one path or prefix selects; a nil hi is
// unbounded.
type keySpan struct{ lo, hi []byte }

// prefixSpan is the keys that begin with prefix, prefix itself included.
func prefixSpan(prefix []byte) keySpan {
	n := len(prefix)
	for n > 0 && prefix[n-1] == 0xff {
		n--
	}
	if n == 0 {
		return keySpan{lo: prefix}
	}
	hi := bytes.Clone(prefix[:n])
	hi[n-1]++
	return keySpan{lo: prefix, hi: hi}
}

// keySpanOf is key alone.
func keySpanOf(key []byte) keySpan { return keySpan{lo: key, hi: append(bytes.Clone(key), 0)} }

func (s keySpan) selection() seekparquet.Selection {
	if len(s.lo) == 0 {
		return seekparquet.Selection{Span: seekparquet.Span{End: s.hi}}
	}
	return seekparquet.Selection{Span: seekparquet.Span{Start: s.lo, End: s.hi}}
}

// contains reports whether s holds every key of t.
func (s keySpan) contains(t keySpan) bool {
	return bytes.Compare(s.lo, t.lo) <= 0 && (s.hi == nil || t.hi != nil && bytes.Compare(t.hi, s.hi) <= 0)
}

func selections(spans []keySpan) []seekparquet.Selection {
	out := make([]seekparquet.Selection, len(spans))
	for i, s := range spans {
		out[i] = s.selection()
	}
	return out
}

// distinct returns the distinct values of in, in first-seen order, and
// for each value of in its index among them.
func distinct[T ~string | ~[]byte](in []T) (unique []T, of []int) {
	seen := make(map[string]int, len(in))
	of = make([]int, len(in))
	for i, v := range in {
		u, ok := seen[string(v)]
		if !ok {
			u = len(unique)
			seen[string(v)] = u
			unique = append(unique, v)
		}
		of[i] = u
	}
	return unique, of
}

// count returns the number of rows of span, nil when the plan's count
// relies on a part no read has checked. Planning's reads are added to st,
// a failed plan's too.
func (d *Dataset) count(ctx context.Context, span keySpan, st *ReadStats) (*int64, error) {
	var planned ReadStats
	plan, err := d.sp.Plan(ctx, []seekparquet.Selection{span.selection()}, nil, &planned)
	addStats(st, planned)
	if err != nil {
		return nil, err
	}
	n, exact := plan.Count()
	if !exact {
		return nil, nil
	}
	return &n, nil
}

// spanSums is what sumSpans found: each span's sum, and the objects whose
// key equals a span's lower bound, by key.
type spanSums struct {
	sums  []objectSum
	exact map[string]Object
}

// sumSpans plans the rows of spans projecting columns ("size", and
// "last_modified" when it is to be summed too), as ranges of the spans not
// inside another, and scans them once, in shards of consecutive rows read
// by up to workers goroutines at once (scanSelections). Each shard adds its
// objects to every span holding them, over borrowed batches, and the
// shards' sums are added up when they all succeed. It records each object
// whose key is a span's lower bound, with what columns gives of it. The
// first error any shard meets is returned.
func (d *Dataset) sumSpans(ctx context.Context, spans []keySpan, columns []string, st *ReadStats, workers int) (spanSums, error) {
	out := spanSums{sums: make([]objectSum, len(spans)), exact: make(map[string]Object)}
	if len(spans) == 0 {
		return out, nil
	}
	order := make([]int, len(spans))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return bytes.Compare(spans[a].lo, spans[b].lo) })
	// Planning positions at both ends of every range, so a span inside
	// another is left out: its rows are planned all the same. Prefixes and
	// single keys nest or are disjoint, so one inside any span taken is
	// inside the last.
	var outer []keySpan
	for _, i := range order {
		if len(outer) == 0 || !outer[len(outer)-1].contains(spans[i]) {
			outer = append(outer, spans[i])
		}
	}
	var mu sync.Mutex
	scan := func(ctx context.Context, shard *seekparquet.Plan, st *ReadStats, keys *shardKeys) error {
		s := d.newSpanScan(spans, order, columns)
		if err := scanPlan(ctx, shard, st, func(b *seekparquet.Batch) error {
			keys.noteBatch(b)
			return s.batch(b)
		}); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		for i := range out.sums {
			out.sums[i].merge(&s.sums[i])
		}
		maps.Copy(out.exact, s.exact)
		return nil
	}
	err := d.scanSelections(ctx, selections(outer), workers, st, columns, func() shardScan { return scan })
	if err != nil {
		return spanSums{}, err
	}
	return out, nil
}

// spanScan sums the rows of one shard, which arrive in ascending key order,
// into the spans holding them.
type spanScan struct {
	d      *Dataset
	spans  []keySpan
	order  []int // spans by ascending lower bound
	next   int   // order[next:] are the spans beginning after every key seen
	active []int // the spans holding the last key seen
	timed  bool  // last_modified is projected, after size
	// The values the fast path accepts without the decoder: a null size
	// or modification time, where the schema allows one.
	nullSize, nullTime bool
	sums               []objectSum
	exact              map[string]Object
	// The batch's size and time columns, and whether the fast path reads
	// them: both present and of INT64 values (any other is decoded).
	sizes, times seekparquet.ColumnView
	fast         bool
}

func (d *Dataset) newSpanScan(spans []keySpan, order []int, columns []string) *spanScan {
	sizeSpec, _ := d.spec.Column("size")
	timeSpec, _ := d.spec.Column("last_modified")
	return &spanScan{d: d, spans: spans, order: order, timed: slices.Contains(columns, "last_modified"),
		nullSize: d.spec.ID == LocalV1 && !sizeSpec.Required, nullTime: !timeSpec.Required,
		sums: make([]objectSum, len(spans)), exact: make(map[string]Object)}
}

// batch adds a batch's rows to the spans holding them. Rows between two
// keys at which the spans holding them change are summed once, and that
// sum added to each of those spans.
func (s *spanScan) batch(b *seekparquet.Batch) error {
	s.sizes, s.times = b.Column(0), seekparquet.ColumnView{}
	if s.timed {
		s.times = b.Column(1)
	}
	s.fast = s.sizes.Kind() == seekparquet.KindInt64 && (!s.timed || s.times.Kind() == seekparquet.KindInt64)
	defer func() { s.sizes, s.times = seekparquet.ColumnView{}, seekparquet.ColumnView{} }() // hold no borrowed page
	for i, n := 0, b.Len(); i < n; {
		key := b.Key(i)
		end := s.enter(key)
		j := n
		if end != nil {
			j = i + 1 + sort.Search(n-i-1, func(k int) bool { return bytes.Compare(b.Key(i+1+k), end) >= 0 })
		}
		exact := s.next > 0 && bytes.Equal(s.spans[s.order[s.next-1]].lo, key)
		var run objectSum
		for r := i; r < j; r++ {
			v, err := s.value(b, r)
			if err != nil {
				return err
			}
			run.add(v)
			if exact = exact && bytes.Equal(b.Key(r), key); exact { // the last row of the key wins
				s.exact[string(key)] = v.object(b, r)
			}
		}
		for _, a := range s.active {
			s.sums[a].merge(&run)
		}
		i = j
	}
	return nil
}

// enter makes s.active the spans holding key, which follows every key
// entered before it, and returns the least key after it at which they
// change, nil when none does.
func (s *spanScan) enter(key []byte) []byte {
	for ; s.next < len(s.order) && bytes.Compare(s.spans[s.order[s.next]].lo, key) <= 0; s.next++ {
		s.active = append(s.active, s.order[s.next])
	}
	s.active = slices.DeleteFunc(s.active, func(i int) bool {
		return s.spans[i].hi != nil && bytes.Compare(key, s.spans[i].hi) >= 0
	})
	var end []byte // a lower bound not yet entered is above a key, so never empty
	if s.next < len(s.order) {
		end = s.spans[s.order[s.next]].lo
	}
	for _, i := range s.active {
		if hi := s.spans[i].hi; hi != nil && (end == nil || bytes.Compare(hi, end) < 0) {
			end = hi
		}
	}
	return end
}

// rowValue is an object's size and modification time, in UTC microseconds
// since the epoch; sized and dated report them present.
type rowValue struct {
	size, micros int64
	sized, dated bool
}

// value returns row r's size and time, checked as Stream checks an object
// (StreamLocal an entry). The borrowed values are taken as they are when
// they pass those checks; any other row is decoded and checked as an
// owning row (sumObject), which gives its error, or its values where the
// decoder accepts what the fast path does not (a column its part may
// lack).
func (s *spanScan) value(b *seekparquet.Batch, r int) (rowValue, error) {
	if s.fast {
		var v rowValue
		sizes := s.sizes
		null := sizes.IsNull(r)
		ok := s.nullSize && null
		if size := sizes.Int64(r); !null && size >= 0 {
			v.size, v.sized, ok = size, true, true
		}
		if ok && s.timed {
			times := s.times
			switch t := times.Int64(r); {
			case times.IsNull(r):
				ok = s.nullTime
			case objectMicrosInRange(t):
				v.micros, v.dated = t, true
			default:
				ok = false
			}
		}
		if ok {
			return v, nil
		}
	}
	o, err := s.d.sumObject(b.Row(r), s.timed)
	if err != nil {
		return rowValue{}, err
	}
	v := rowValue{sized: o.Size != nil, dated: o.LastModified != nil}
	if v.sized {
		v.size = *o.Size
	}
	if v.dated {
		v.micros = o.LastModified.UnixMicro()
	}
	return v, nil
}

// sumObject decodes and checks an owning row of a sum: its size, and its
// modification time when timed.
func (d *Dataset) sumObject(r seekparquet.Row, timed bool) (Object, error) {
	dec := decoder{spec: d.spec, row: &r}
	o := Object{Position: dec.position(), Size: dec.int64("size")}
	if timed {
		o.LastModified = dec.time("last_modified")
	}
	if dec.err != nil {
		return Object{}, dec.err
	}
	if err := checkObject(&o, d.spec.ID == LocalV1); err != nil {
		return Object{}, err
	}
	return o, nil
}

// object returns v as row r of b's Object, owning its key.
func (v rowValue) object(b *seekparquet.Batch, r int) Object {
	o := Object{Position: Position{Key: bytes.Clone(b.Key(r)), Offset: b.Offset(r)}}
	if v.sized {
		o.Size = ptrTo(v.size)
	}
	if v.dated {
		o.LastModified = ptrTo(time.UnixMicro(v.micros).UTC())
	}
	return o
}

// objectSum is the number, sizes and newest modification time of some
// objects.
type objectSum struct {
	count    int64
	bytes    int64
	overflow bool  // the sizes overflow an int64
	unsized  bool  // a filesystem entry has no size
	newest   int64 // UTC microseconds since the epoch, when dated
	dated    bool  // newest is set
	undated  bool  // an object has no modification time
}

func (s *objectSum) add(v rowValue) {
	s.count++
	if v.sized {
		s.addSize(v.size)
	} else {
		s.unsized = true
	}
	switch {
	case !v.dated:
		s.undated = true
	case !s.dated || v.micros > s.newest:
		s.newest, s.dated = v.micros, true
	}
}

// merge adds the objects of t, which are not among s's: sizes are never
// negative, so the total overflows exactly when a part of it does.
func (s *objectSum) merge(t *objectSum) {
	s.count += t.count
	if t.overflow {
		s.overflow = true
	} else {
		s.addSize(t.bytes)
	}
	s.unsized = s.unsized || t.unsized
	s.undated = s.undated || t.undated
	if t.dated && (!s.dated || t.newest > s.newest) {
		s.newest, s.dated = t.newest, true
	}
}

func (s *objectSum) addSize(size int64) {
	if s.overflow || size > math.MaxInt64-s.bytes {
		s.overflow = true
		return
	}
	s.bytes += size
}

// total is the sum of the sizes, nil when an object had none or they
// overflow an int64.
func (s *objectSum) total() *int64 {
	if s.unsized || s.overflow {
		return nil
	}
	return ptrTo(s.bytes)
}

// newestTime is the newest modification time, nil when there was no
// object or one had no time.
func (s *objectSum) newestTime() *time.Time {
	if !s.dated || s.undated {
		return nil
	}
	return ptrTo(time.UnixMicro(s.newest).UTC())
}
