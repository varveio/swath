// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"math"
	"math/bits"
	"time"
)

// A timer's max and percentiles, and a distribution's max, describe recent
// samples, not the meter's whole life, with the rules of the Java server's
// registry (Micrometer 1.17's SimpleMeterRegistry, cumulative mode), so a
// scrape of either server at the same moment after the same samples reads
// the same window. That registry sets every meter's expiry to its step, one
// minute, with the default buffer length of three, and keeps two windows:
//
//   - the maximum (TimeWindowMax): three slots, every sample raising all
//     three; every expiry the slot read is cleared and the next becomes
//     the one read, and after three expiries without a rotation all are
//     cleared at once. A maximum lasts two to three minutes.
//   - the percentiles (TimeWindowPercentileHistogram): three histograms,
//     every sample recorded in all three; every expiry/3 the one read is
//     cleared and the next becomes the one read, at most three rotations
//     at a time, the rotation clock catching up by one step per rotation.
//     A percentile describes the last 40 to 60 seconds.
//
// Both rotate lazily, when a sample is recorded or the window is read, on
// the wall clock in milliseconds from the meter's creation, as there.
const (
	windowExpiryMillis  = int64(time.Minute / time.Millisecond)
	windowBuffers       = 3
	histogramStepMillis = windowExpiryMillis / windowBuffers
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

// windowMax is TimeWindowMax: the maximum over a rotating ring of slots.
type windowMax struct {
	ring [windowBuffers]float64
	cur  int
	last int64 // milliseconds of the last rotation
}

func newWindowMax(nowMillis int64) windowMax { return windowMax{last: nowMillis} }

func (w *windowMax) rotate(nowMillis int64) {
	since := nowMillis - w.last
	if since < windowExpiryMillis {
		return
	}
	if since >= windowExpiryMillis*windowBuffers {
		clear(w.ring[:])
		w.cur = 0
		w.last = nowMillis - since%windowExpiryMillis
		return
	}
	for iterations := 0; ; {
		w.ring[w.cur] = 0
		w.cur = (w.cur + 1) % windowBuffers
		since -= windowExpiryMillis
		w.last += windowExpiryMillis
		iterations++
		if since < windowExpiryMillis || iterations >= windowBuffers {
			break
		}
	}
}

func (w *windowMax) record(nowMillis int64, v float64) {
	w.rotate(nowMillis)
	for i := range w.ring {
		w.ring[i] = max(w.ring[i], v)
	}
}

func (w *windowMax) read(nowMillis int64) float64 {
	w.rotate(nowMillis)
	return w.ring[w.cur]
}

// windowHistogram is TimeWindowPercentileHistogram's rotation over this
// package's histogram.
type windowHistogram struct {
	ring [windowBuffers]histogram
	cur  int
	last int64 // milliseconds of the last rotation
}

func newWindowHistogram(nowMillis int64) windowHistogram { return windowHistogram{last: nowMillis} }

// rotate clears at most windowBuffers histograms per call and moves the
// rotation clock one step per histogram cleared, however long the window
// was idle: the next call catches up further.
func (w *windowHistogram) rotate(nowMillis int64) {
	since := nowMillis - w.last
	if since < histogramStepMillis {
		return
	}
	for iterations := 0; ; {
		w.ring[w.cur].reset()
		w.cur = (w.cur + 1) % windowBuffers
		since -= histogramStepMillis
		w.last += histogramStepMillis
		iterations++
		if since < histogramStepMillis || iterations >= windowBuffers {
			break
		}
	}
}

func (w *windowHistogram) record(nowMillis int64, v int64) {
	w.rotate(nowMillis)
	for i := range w.ring {
		w.ring[i].record(v)
	}
}

// read returns the p50 and p99 in nanoseconds of the histogram read.
func (w *windowHistogram) read(nowMillis int64) (p50, p99 int64) {
	w.rotate(nowMillis)
	h := &w.ring[w.cur]
	return h.percentile(0.50), h.percentile(0.99)
}
