// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// PathTotals is the totals of one path requested from Totals. Its numbers
// are never partial or estimated: Totals fails instead.
type PathTotals struct {
	// Count is the number of objects the path selects; zero for an empty
	// selection.
	Count int64
	// Bytes is the sum of the selected objects' sizes.
	Bytes int64
	// NewestModified is the latest modification time among the selected
	// objects. It is nil when Count is zero or any selected object has no
	// modification time.
	NewestModified *time.Time
	// Exact is the object whose key equals the path, when one exists. It
	// is also counted in Count and Bytes. It is always nil for "".
	Exact *Object
}

// Totals returns one PathTotals per path, in input order. Paths are taken
// verbatim, without cleaning: "" selects every object; a path ending in
// "/" selects every object whose key begins with it, an object whose key
// equals it included; any other path selects only the object whose key
// equals it. Paths may repeat, nest and overlap; each total is
// independent and inclusive.
//
// Totals plans the rows of every path at once (seekparquet.Dataset.
// PlanRanges, projecting size and last_modified), which checks every part
// holding one of them, and scans that plan: a row several paths select is
// decoded once. Each Count is the plan's count of the path's range, exact
// since its parts are checked. An exact object's ETag and storage class
// are read by a plan of those objects alone. Every selected object is
// checked as Stream checks it (a size, at least 0; a modification time in
// the years 1 to 9999), failing with a SemanticError of KindRow; sizes
// whose sum does not fit an int64 fail with one naming size and no row.
//
// A versions schema holds no current objects: Totals refuses it with a
// SemanticError of KindSchema before it reads. The module's refusals
// (*seekparquet.RefusalError) and the Source's errors are returned
// unchanged, and a context that ends fails Totals with its error, checked
// before and after the reads. No totals are returned with an error.
func (d *Dataset) Totals(ctx context.Context, paths []string) ([]PathTotals, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	columns := objectColumns(d.spec.ID)
	if columns == nil {
		return nil, semantic(KindSchema, "", nil, "%s holds versions, not current objects", d.spec.ID)
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

	sums, err := d.sumSpans(ctx, spans, []string{"size", "last_modified"})
	if err != nil {
		return nil, err
	}
	totals := make([]PathTotals, len(unique))
	for i, s := range sums.sums {
		n, err := d.count(ctx, spans[i])
		if err != nil {
			return nil, err
		}
		if n == nil {
			return nil, fmt.Errorf("swath reader: the count of %q relies on a part no read has checked", unique[i])
		}
		if s.overflow {
			return nil, semantic(KindRow, "size", nil, "the sizes %q selects overflow an int64 total", unique[i])
		}
		totals[i] = PathTotals{Count: *n, Bytes: s.bytes, NewestModified: s.newestTime()}
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

// fillExact reads the columns after size and last_modified (ETag and
// storage class) of the exact objects found, by one plan of their keys.
func (d *Dataset) fillExact(ctx context.Context, totals []PathTotals, columns []string) error {
	at := make(map[string]*Object)
	var ranges []seekparquet.Range
	for i := range totals {
		if o := totals[i].Exact; o != nil && len(columns) > 0 {
			at[string(o.Key)] = o
			ranges = append(ranges, seekparquet.KeyRange(o.Key, 0))
		}
	}
	if len(ranges) == 0 {
		return nil
	}
	plan, err := d.sp.PlanRanges(ctx, ranges, columns)
	if err != nil {
		return err
	}
	return plan.Scan(ctx, nil, func(r seekparquet.Row) error {
		o := at[string(r.Key)]
		dec := decoder{spec: d.spec, row: &r}
		o.ETag, o.StorageClass = dec.string("etag"), dec.string("storage_class")
		return dec.err
	})
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

func (s keySpan) rng() seekparquet.Range {
	if len(s.lo) == 0 {
		return seekparquet.Range{End: s.hi}
	}
	return seekparquet.Range{Start: s.lo, End: s.hi}
}

// contains reports whether s holds every key of t.
func (s keySpan) contains(t keySpan) bool {
	return bytes.Compare(s.lo, t.lo) <= 0 && (s.hi == nil || t.hi != nil && bytes.Compare(t.hi, s.hi) <= 0)
}

func ranges(spans []keySpan) []seekparquet.Range {
	out := make([]seekparquet.Range, len(spans))
	for i, s := range spans {
		out[i] = s.rng()
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
// relies on a part no read has checked.
func (d *Dataset) count(ctx context.Context, span keySpan) (*int64, error) {
	plan, err := d.sp.PlanRanges(ctx, []seekparquet.Range{span.rng()}, nil)
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
// "last_modified" when it is to be summed too) and scans them once, adding
// each object to every span holding its key. It records each object whose
// key is a span's lower bound, with what columns gives of it.
func (d *Dataset) sumSpans(ctx context.Context, spans []keySpan, columns []string) (spanSums, error) {
	out := spanSums{sums: make([]objectSum, len(spans)), exact: make(map[string]Object)}
	if len(spans) == 0 {
		return out, nil
	}
	plan, err := d.sp.PlanRanges(ctx, ranges(spans), columns)
	if err != nil {
		return spanSums{}, err
	}
	order := make([]int, len(spans))
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return bytes.Compare(spans[a].lo, spans[b].lo) })
	lower := make(map[string]bool, len(spans))
	for _, s := range spans {
		lower[string(s.lo)] = true
	}
	timed := slices.Contains(columns, "last_modified")
	var active []int
	next := 0
	err = plan.Scan(ctx, nil, func(r seekparquet.Row) error {
		dec := decoder{spec: d.spec, row: &r}
		o := Object{Position: dec.position(), Size: dec.int64("size")}
		if timed {
			o.LastModified = dec.time("last_modified")
		}
		if dec.err != nil {
			return dec.err
		}
		if err := checkObject(&o); err != nil {
			return err
		}
		for ; next < len(order) && bytes.Compare(spans[order[next]].lo, r.Key) <= 0; next++ {
			active = append(active, order[next])
		}
		active = slices.DeleteFunc(active, func(i int) bool {
			return spans[i].hi != nil && bytes.Compare(r.Key, spans[i].hi) >= 0
		})
		for _, i := range active {
			out.sums[i].add(&o)
		}
		if lower[string(r.Key)] {
			out.exact[string(r.Key)] = o
		}
		return nil
	})
	if err != nil {
		return spanSums{}, err
	}
	return out, nil
}

// objectSum is the sizes and newest modification time of some objects.
type objectSum struct {
	bytes    int64
	overflow bool // the sizes overflow an int64
	newest   time.Time
	dated    bool // newest is set
	undated  bool // an object has no modification time
}

func (s *objectSum) add(o *Object) {
	s.addSize(*o.Size)
	switch {
	case o.LastModified == nil:
		s.undated = true
	case !s.dated || o.LastModified.After(s.newest):
		s.newest, s.dated = *o.LastModified, true
	}
}

func (s *objectSum) addSize(size int64) {
	if s.overflow || size > math.MaxInt64-s.bytes {
		s.overflow = true
		return
	}
	s.bytes += size
}

// newestTime is the newest modification time, nil when there was no
// object or one had no time.
func (s *objectSum) newestTime() *time.Time {
	if !s.dated || s.undated {
		return nil
	}
	t := s.newest
	return &t
}
