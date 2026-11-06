// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import (
	"fmt"
	"strings"
	"testing"
)

// TestSeedObservedVsContinuing: the descent tells what a sample showed
// (its entries) from whether it continues. With a nominal page of 10, a
// folder of 12 subfolders that continues is sampled, and every weight
// probe answers alike, in three ways: full continuing pages (10 entries
// and a token), one-entry terminal pages, and one-entry continuing pages
// (a provider may cut a page short). Full and terminal samples are
// weighed by what they showed and classed by it; a one-entry continuing
// sample is weighed and classed as a full page, the estimate, and each such
// use is counted (short_continuing_estimated); full pages never count.
func TestSeedObservedVsContinuing(t *testing.T) {
	const page = 10
	sample := map[string]seedOutcome{
		"full":                {objects: page, more: true},
		"one-entry terminal":  {objects: 1},
		"one-entry continues": {objects: 1, more: true},
	}
	for name, answer := range sample {
		t.Run(name, func(t *testing.T) {
			d := newDescent("", 4, page, Safe)
			q, res := d.start()
			for range 1000 {
				if res != nil {
					break
				}
				var o seedOutcome
				switch {
				case q.prefix == "" && q.after == "": // the top: 30 folders, listed whole
					for i := range 30 {
						o.prefixes = append(o.prefixes, fmt.Sprintf("f%02d/", i))
					}
				case q.prefix == "f00/" && d.phase == phaseDescent: // 12 subfolders, and more
					for i := range 12 {
						o.prefixes = append(o.prefixes, fmt.Sprintf("f00/c%02d/", i))
					}
					o.more = true
				case strings.HasPrefix(q.prefix, "f00/c") || d.phase == phaseWeightSample:
					o = answer
				default: // another folder: two objects
					o.objects = 2
				}
				o.last, o.hasLast = q.prefix+"~", true
				q, res = d.next(o)
			}
			if res == nil {
				t.Fatal("the descent did not finish")
			}
			wantWeight := map[string]int64{"full": page, "one-entry terminal": 1, "one-entry continues": page}[name]
			if len(d.weights) == 0 {
				t.Fatal("no weight sampled")
			}
			for cut, w := range d.weights {
				if w != wantWeight {
					t.Fatalf("weight of %q: %d, want %d", cut, w, wantWeight)
				}
			}
			heavy := res.levels["heavy_cut_banded"] == 1
			if want := name != "one-entry terminal"; heavy != want || (!want && res.levels["tiny_leaf_explosion"] != 1) {
				t.Fatalf("levels %v: heavy %v, want %v", res.levels, heavy, want)
			}
			estimated := res.marks["short_continuing_estimated"]
			if want := name == "one-entry continues"; (estimated > 0) != want {
				t.Fatalf("short_continuing_estimated %d", estimated)
			} else if want && estimated != 3+len(d.weights) {
				t.Fatalf("short_continuing_estimated %d, want %d (3 children and every weight)", estimated, 3+len(d.weights))
			}
		})
	}
}
