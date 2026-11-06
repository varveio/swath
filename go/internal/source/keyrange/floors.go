// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package keyrange

import "math"

// The gate-chain floors, ported from Swath's owner-split governor and
// StealMath: a range is not split when the work left in it is worth less
// than a few pages, or when the child past the pivot would realize less
// than two pages of keys. Both read a position estimate, not a probe: the
// keys a range has produced, adjusted by the cursor's position read as a
// base-256 fraction past the bytes it shares with lo (estRemaining).

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
	// anchorBand and anchorMin bound how far the anchored geometry may lift
	// and cut the keys a range has produced (Java GEOMETRY_BAND and
	// QUARTER_MIN_GEOMETRY).
	anchorBand = 16.0
	anchorMin  = 1.0 / 4
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

// estRemaining is the floors' estimate of the keys left in (c, hi] (Java
// RateAnchoredEstimator, Swath's default position sensor): the keys the
// range has already produced (at least a page), adjusted by the
// cursor-anchored geometry within [anchorMin, anchorBand]. A cursor at hi
// leaves nothing. A range that has not finished after producing many keys
// is evidence of a big range, so measured position may lift the produced
// mass sixteenfold but cut it only to a quarter. The plain window reading
// (keys over the consumed share of (lo, hi]) is not used: on a deep shared
// prefix its consumed share underflows to zero, the keys drop out, and the
// remaining-estimate floor refused every split of a range holding most of
// a bucket.
func estRemaining(c, lo, hi string, keys, page int64) float64 {
	if c >= hi {
		return 0
	}
	g := math.Min(anchorBand, math.Max(anchorMin, anchoredGeometry(c, lo, hi)))
	return float64(max(keys, page)) * g
}

// anchoredGeometry is the remaining span over the consumed span, read from
// the byte where the cursor diverges from lo (Java
// StealMath.anchoredGeometricFactor): where the cursor has descended into a
// subtree it shares with lo, the window is that subtree, with ceiling 1.
// Neutral (1) without consumed evidence.
func anchoredGeometry(c, lo, hi string) float64 {
	if c <= lo {
		return 1
	}
	d, d0 := commonLen(lo, c), commonLen(lo, hi)
	consumed := fracFrom(c, d) - fracFrom(lo, d)
	if consumed <= 0 {
		return 1
	}
	top := 1.0
	if d <= d0 {
		top = fracFrom(hi, d)
	}
	return math.Max(0, top-fracFrom(c, d)) / consumed
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
func belowRemainingFloor(est float64, page int64) bool {
	return est <= remainingFloorPages*float64(page)
}

// tailBelowFloor: the child past a pivot at fraction f of the remaining
// span would realize at most tailFloorPages pages, crediting it with
// est·max(1/16, min(1, ratio) − f) keys (Java
// StealMath.childTailBelowObservedMassFloor, reach_floored).
func tailBelowFloor(est, f, ratio float64, page int64) bool {
	reach := math.Min(1, ratio) - f
	return est*math.Max(tailReachMin, reach) <= tailFloorPages*float64(page)
}

// pivotFraction is where m lies in the remaining span (c, hi], read in the
// cursor-anchored window estRemaining uses; Java places its pivot at a
// chosen fraction, the Go pivot is synthesized, so its fraction is measured.
func pivotFraction(c, m, lo, hi string) float64 {
	d := commonLen(lo, c)
	top := 1.0
	if d <= commonLen(lo, hi) {
		top = fracFrom(hi, d)
	}
	rem := top - fracFrom(c, d)
	if rem <= 0 {
		return 1
	}
	return math.Min(1, math.Max(0, fracFrom(m, d)-fracFrom(c, d))/rem)
}
