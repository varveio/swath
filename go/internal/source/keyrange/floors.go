// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import "math"

// The gate-chain floors, ported from Swath's owner-split governor and
// StealMath: a range is not split when the work left in it is worth less
// than a few pages, or when the child past the pivot would realize less
// than two pages of keys. Both read a position estimate in a window
// fraction, not a probe: keys are read as base-256 fractions of the window
// (lo, hi] past the bytes lo and hi share.

const (
	// fracBytes is how many bytes past the window's shared prefix a
	// fraction reads.
	fracBytes = 12
	// remainingFloorPages: a range whose estimated remaining keys are at
	// most this many pages is not split (Java SELF_SPLIT_MIN_REMAINING_PAGES).
	remainingFloorPages = 4
	// tailFloorPages: the child past the pivot must realize more than this
	// many pages of keys.
	tailFloorPages = 2
	// tailReachMin is the least share of the remaining estimate the child
	// past the pivot is credited with on a thinning keyspace (Java's
	// reach_floored tail floor, its default).
	tailReachMin = 1.0 / 16
	// densityAlpha is the weight of the newest page in the density EWMA.
	densityAlpha = 0.3
)

func commonLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// fracFrom reads key[off:off+fracBytes] as a base-256 fraction in [0, 1).
func fracFrom(key string, off int) float64 {
	f, scale := 0.0, 1.0/256
	for i := off; i < len(key) && i < off+fracBytes; i++ {
		f += float64(key[i]) * scale
		scale /= 256
	}
	return f
}

// fracIn is key's position in the window (lo, hi].
func fracIn(key, lo, hi string) float64 { return fracFrom(key, commonLen(lo, hi)) }

// spanIn is the window fraction between x and y, at least 0.
func spanIn(x, y, lo, hi string) float64 {
	return math.Max(0, fracIn(y, lo, hi)-fracIn(x, lo, hi))
}

// estRemaining extrapolates the keys left in (c, hi] from the keys listed in
// (lo, c] at the same density per window fraction; with nothing consumed
// it is the remaining fraction itself (Java StealMath.estRemaining).
func estRemaining(c, lo, hi string, keys int64) float64 {
	remaining := spanIn(c, hi, lo, hi)
	consumed := spanIn(lo, c, lo, hi)
	if consumed <= 0 {
		return remaining
	}
	return float64(keys) / consumed * remaining
}

// density is a range's page-density digest: an EWMA of keys per window
// fraction over its pages (Java WorkerState.recordPage).
type density struct{ ewma float64 }

func (d *density) observe(first, last, lo, hi string, n int) {
	if n <= 0 {
		return
	}
	span := spanIn(first, last, lo, hi)
	if span <= 0 {
		return
	}
	local := float64(n) / span
	if d.ewma <= 0 {
		d.ewma = local
	} else {
		d.ewma = densityAlpha*local + (1-densityAlpha)*d.ewma
	}
}

// ratio is the trailing density over the range's average density: below 1
// the keyspace thins ahead of the cursor. +Inf without a signal (the floor
// then treats the range as uniform).
func (d *density) ratio(c, lo, hi string, keys int64) float64 {
	if d.ewma <= 0 || keys <= 0 {
		return math.Inf(1)
	}
	consumed := spanIn(lo, c, lo, hi)
	if consumed <= 0 {
		return math.Inf(1)
	}
	return d.ewma / (float64(keys) / consumed)
}

// belowRemainingFloor: the range is not worth splitting.
func belowRemainingFloor(est float64) bool {
	return est <= remainingFloorPages*PageKeys
}

// tailBelowFloor: the child past a pivot at fraction f of the remaining
// span would realize at most tailFloorPages pages, crediting it with
// est·max(1/16, min(1, ratio) − f) keys (Java
// StealMath.childTailBelowObservedMassFloor, reach_floored).
func tailBelowFloor(est, f, ratio float64) bool {
	reach := math.Min(1, ratio) - f
	return est*math.Max(tailReachMin, reach) <= tailFloorPages*PageKeys
}

// pivotFraction is where m lies in the remaining span (c, hi], in est's
// window frame; Java places its pivot at a chosen fraction, the Go pivot is
// synthesized, so its fraction is measured.
func pivotFraction(c, m, lo, hi string) float64 {
	rem := spanIn(c, hi, lo, hi)
	if rem <= 0 {
		return 1
	}
	return math.Min(1, spanIn(c, m, lo, hi)/rem)
}
