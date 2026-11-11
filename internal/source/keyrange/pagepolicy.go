// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import "math"

// growShare is the share of a range's estimated keys left that its next
// request may ask for: half, so an estimate that errs long by up to twice
// still ends the range in pages of the nominal size (a gauge errs long on
// hierarchical keys, admission.go).
const growShare = 0.5

// GrowPage is the experimental page-growth policy (#449, diagnostic), an
// Options.PageFor asking at most maxPage. A range starts at the nominal
// size, children of a split included. After a page that came back full
// and inside the range (no bound reached: PageHint.Full), the next request
// may double the last size asked, up to maxPage, but asks no more than
// growShare of the keys estimated left: the gauge's remainder (c, hi] of a
// bounded range, or, where the page cannot read it (+Inf: the frontier, a
// bound in another folder), the keys the range listed so far, as a range
// that has listed k keys likely holds k more. Without an estimate, or
// after a short page, it asks the nominal size. The engine decides a split
// before it asks for the next page, so a split is never preceded by a
// grown request it would abandon.
func GrowPage(maxPage int) func(PageHint) int {
	return func(h PageHint) int {
		if h.First || h.Full == 0 || math.IsNaN(h.Rem) {
			return h.Nominal
		}
		left := h.Rem
		if math.IsInf(left, 1) {
			left = float64(h.Keys)
		}
		next := min(2*max(h.Asked, h.Nominal), maxPage)
		want := growShare * left
		if want <= float64(h.Nominal) {
			return h.Nominal
		}
		return min(next, int(want))
	}
}
