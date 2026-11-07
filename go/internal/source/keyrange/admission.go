// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"math"
	"slices"
	"strings"
)

// Split admission: whether a split that demand asks for, and the guard
// allows, is worth its requests (docs/internals/providers.md, "Range
// engine"). A split costs each child a first request, and on S3, which has
// no end bound, a child's last page overfetches up to a page past its hi;
// it saves at most the page latencies the owner would spend listing the
// rest alone. Both gates and the rank read one estimate: the keys in an
// interval, from the density of the range's last page.

const (
	// admitRemainderPages: a bounded range with an estimated remainder
	// (c, hi] under this many pages is not split. The owner lists it in
	// at most two more requests; split, it pays the children's first
	// requests (and their overfetched tail pages) to save at most one page
	// latency, and on hierarchical keys such a remainder is where a pivot
	// lands in the gap after a folder's last key (an empty child).
	admitRemainderPages = 2
	// admitChildPages: a split whose lower child (c, m] or upper child
	// (m, hi] is estimated under this many pages is not taken. The owner's
	// next request, starting after c, covers a small lower child anyway;
	// a small upper child leaves the lower one nearly the whole remainder,
	// so the split hands out less than a page for a child's request (an
	// empty one, where the pivot lands past the remainder's last key,
	// overfetches a full page on S3).
	admitChildPages = 1
	// admitRefusals: after this many refusals in a row of one range's
	// splits, with demand each time, the next one is admitted
	// (split.admission.overridden), so a refusal costs a range at most
	// this many pages of parallelism. A correctly refused remainder (under
	// 2 pages) has ended by then; at 2 the overrides admitted most of the
	// gap pivots flat keys repeat too (four times the empty children).
	admitRefusals = 4
	// gaugeDigits is how many digits past the first difference a span
	// reads: enough for a float64's precision in any radix of 2 or more
	// past the leading digit.
	gaugeDigits = 16
)

// gauge estimates the keys in an interval (x, y] from one page of keys,
// from the page's run of keys under the prefix x and y share (the run, a
// suffix of the page; a page that crosses folders measures the density of
// folder names, not of the folder the interval lies in). Keys are read as
// numbers in the run's own alphabet: the distinct bytes past the run's
// shared prefix, a byte outside it rounding up to the next one in it (the
// radix above them all), the end of a key the digit below them. The run's
// keys per unit scale the interval's length. The alphabet, not base 256,
// keeps decimal counters in proportion (in base 256 a remainder one decimal
// digit above the page reads 25 times too long), and a pivot past the
// run's digits ("logs/p/00001l" after "logs/p/000019999") reads as the end
// of them. Where the run cannot read the interval the estimate is +Inf, so
// no gate engages: x and y share no run of two keys, or they differ above
// the run's varying digits at a position whose byte in the run is not one
// of its digits (a folder name: "…_NorESM2/" after a page of
// "…_MIROC6/12.3", "logs/q" after "logs/p/000123"). Folder gaps still
// read as keys (a page cannot know that a folder ended), so on
// hierarchical keys the estimate errs long, where the gates do not engage.
type gauge struct {
	page []string
	// The alphabet of the run page[j:] (j -1: none built).
	j      int
	ok     bool        // the run has two keys and a span
	rank   [256]uint16 // a byte's digit: 1 + the run's alphabet bytes below it
	seen   [256]bool   // the run's alphabet
	radix  float64
	shared int     // the length of the run's shared prefix
	m2     float64 // the run's span (first, last], as span returns it
	e2     int
}

// newGauge is the gauge of a page; nil with fewer than two keys.
func newGauge(page []string) *gauge {
	if len(page) < 2 {
		return nil
	}
	return &gauge{page: page, j: -1}
}

// run builds the alphabet of the run page[j:] (cached: a decision's
// estimates mostly share one run) and reports whether it can estimate.
func (g *gauge) run(j int) bool {
	if j == g.j {
		return g.ok
	}
	g.j, g.ok = j, false
	run := g.page[j:]
	if len(run) < 2 {
		return false
	}
	first, last := run[0], run[len(run)-1]
	g.shared = commonLen(first, last)
	g.seen = [256]bool{}
	for _, k := range run {
		for i := g.shared; i < len(k); i++ {
			g.seen[k[i]] = true
		}
	}
	n := uint16(0)
	for b := range 256 {
		g.rank[b] = n + 1
		if g.seen[b] {
			n++
		}
	}
	g.radix = float64(n) + 1
	g.m2, g.e2 = g.span(first, last)
	g.ok = g.m2 > 0
	return g.ok
}

// span is y - x (x < y) in the run's numbers as mantissa × radix^-e,
// where e is the byte at which they first differ. Two bytes outside the
// alphabet may round to one digit; at e the difference is still at least
// one, as x < y.
func (g *gauge) span(x, y string) (m float64, e int) {
	e = commonLen(x, y)
	s := 1.0
	for i := e; i < e+gaugeDigits; i++ {
		var dx, dy float64
		if i < len(x) {
			dx = float64(g.rank[x[i]])
		}
		if i < len(y) {
			dy = float64(g.rank[y[i]])
		}
		d := dy - dx
		if i == e {
			d = max(d, 1)
		}
		m += d * s
		s /= g.radix
	}
	return m, e
}

// keys is the estimated number of keys in (x, y] (x at or after the
// page's first key): +Inf where the page cannot read it (gauge), and
// past a float's range.
func (g *gauge) keys(x, y string) float64 {
	if y <= x {
		return 0
	}
	e := commonLen(x, y)
	prefix := x[:e]
	last := g.page[len(g.page)-1]
	if !strings.HasPrefix(last, prefix) {
		return math.Inf(1)
	}
	// The keys from prefix through last all carry it: they sort at or
	// after prefix and at or before last, which carries it.
	j, _ := slices.BinarySearch(g.page, prefix)
	if !g.run(j) {
		return math.Inf(1)
	}
	// Above the run's varying digits, a position is read in its alphabet
	// only where the run's own byte there is one of its digits.
	for i := e; i < g.shared; i++ {
		if !g.seen[g.page[j][i]] {
			return math.Inf(1)
		}
	}
	m1, e1 := g.span(x, y)
	if m1 <= 0 {
		return 0
	}
	return float64(len(g.page)-j-1) * m1 / g.m2 * math.Pow(g.radix, float64(g.e2-e1))
}
