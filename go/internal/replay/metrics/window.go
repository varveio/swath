// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"math"
	"math/bits"
	"time"
)

// A timer's max and percentiles describe recent samples, not the meter's
// whole life: a scrape at the end of a measured window reads that window,
// not the warm-up before it. The window is the one a Micrometer timer
// publishes with its defaults (the Java server's registry), so a scrape of
// either server covers the same span: samples go into every one of
// windowBuffers histograms, the oldest is read, and every
// windowExpiry/windowBuffers the oldest is cleared and the next becomes
// the one read. A snapshot therefore covers the last 80 to 120 seconds.
const (
	windowExpiry  = 2 * time.Minute
	windowBuffers = 3
	windowStep    = windowExpiry / windowBuffers
)

// The histogram is log-linear: exact below 2^subBits nanoseconds, then
// 2^subBits buckets per power of two, so a percentile is within 1/32
// (3.1%) of the sample it stands for. Durations at or above 2^maxBits ns
// (about 2.4 hours) fall in the last bucket. Considered: a bounded
// reservoir of recent samples, which is exact but covers a sample count
// rather than a span (at a few thousand requests a second, well under a
// second); and HdrHistogram ports, a module each for what is 40 lines
// here. golang.org/x has no histogram.
const (
	subBits    = 5
	maxBits    = 43
	numBuckets = (maxBits-subBits+1)<<subBits + 1<<subBits
)

// bucketOf is the histogram bucket of v nanoseconds.
func bucketOf(v int64) int {
	if v < 0 {
		v = 0
	}
	if v >= 1<<maxBits {
		return numBuckets - 1
	}
	if v < 1<<subBits {
		return int(v)
	}
	e := bits.Len64(uint64(v)) - 1 - subBits
	return (e+1)<<subBits + int(v>>e)&(1<<subBits-1)
}

// bucketUpper is the largest value bucket i holds.
func bucketUpper(i int) int64 {
	if i < 1<<subBits {
		return int64(i)
	}
	e := i>>subBits - 1
	m := int64(i&(1<<subBits-1)) + 1<<subBits
	return (m+1)<<e - 1
}

// histogram is one window buffer.
type histogram struct {
	counts []uint32 // allocated on the first sample
	n      int64
	max    int64
}

func (h *histogram) record(v int64) {
	if h.counts == nil {
		h.counts = make([]uint32, numBuckets)
	}
	h.counts[bucketOf(v)]++
	h.n++
	h.max = max(h.max, v)
}

func (h *histogram) reset() {
	clear(h.counts)
	h.n, h.max = 0, 0
}

// percentile is the nearest-rank percentile in nanoseconds: the upper
// bound of the bucket holding the sample of that rank, never above the
// window's max. Zero with no samples.
func (h *histogram) percentile(p float64) int64 {
	if h.n == 0 {
		return 0
	}
	rank := max(int64(math.Ceil(p*float64(h.n))), 1)
	var seen int64
	for i, c := range h.counts {
		seen += int64(c)
		if seen >= rank {
			return min(bucketUpper(i), h.max)
		}
	}
	return h.max
}

// timeWindow is a timer's rotating set of histograms.
type timeWindow struct {
	ring [windowBuffers]histogram
	cur  int       // the buffer read
	next time.Time // when cur is next cleared
}

func newTimeWindow(now time.Time) *timeWindow {
	return &timeWindow{next: now.Add(windowStep)}
}

// rotate clears the buffers whose span has passed by now.
func (w *timeWindow) rotate(now time.Time) {
	if now.Sub(w.next) >= windowExpiry {
		for i := range w.ring {
			w.ring[i].reset()
		}
		w.next = now.Add(windowStep)
		return
	}
	for !now.Before(w.next) {
		w.ring[w.cur].reset()
		w.cur = (w.cur + 1) % windowBuffers
		w.next = w.next.Add(windowStep)
	}
}

func (w *timeWindow) record(now time.Time, v int64) {
	w.rotate(now)
	for i := range w.ring {
		w.ring[i].record(v)
	}
}

// read returns the window's max and the p50 and p99 in nanoseconds.
func (w *timeWindow) read(now time.Time) (maxv, p50, p99 int64) {
	w.rotate(now)
	h := &w.ring[w.cur]
	return h.max, h.percentile(0.50), h.percentile(0.99)
}
