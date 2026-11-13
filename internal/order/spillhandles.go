// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"errors"
	"os"
	"sync"
	"syscall"
)

// Spill read handles. Every read of a run's spill file (not a recovered
// certificate's replay, durable_replay.go, which opens its own reader)
// goes through one descriptor per file, kept open between reads: a frame read at an offset
// (the read-ahead's, the segments'), a trailer read, and the drainer's
// sequential reader all lease the file's handle and read it with ReadAt
// (pread), which is safe concurrently, so one descriptor serves them all.
// Before, each such read opened and closed the file: on a tree of many
// small directories a range spills one or two frames, so a file was opened
// once per frame read and again for its trailer.
//
// Bounds. maxHandles is a retention target, not a ceiling: an idle handle
// is kept only while the descriptors open (and being opened) stay within
// it, the least recently used closed first, and a miss first makes room
// that way. A leased handle is never closed, so when every handle is
// leased a miss opens past the target: the descriptors open are at most
// the larger of maxHandles and the concurrent readers, which the readers
// themselves bound (the drainer's maxReaders, the read-ahead's aheadReads,
// the segment workers listing.segmentWorkers admits, which may exceed 16
// when the descriptor limit allows). That is the bound descriptor
// admission already priced before the cache; it adds none. A spill open
// that fails for want of descriptors (EMFILE, ENFILE), a read miss's or a
// frame's write (spiller.writeFrames), closes every idle handle and is
// retried once, and the cache stops keeping idle handles for the rest of
// the run (the low-descriptor fallback: one open per read, as before).
//
// Lifetime. A handle is retired before its file is removed or handed back
// to its durable cache (drained), when the file is truncated (an ENOSPC
// rollback), and every cached handle when the run stops (cancellation,
// failure, success) or the output's identity check fails. A retired handle
// leaves the cache at once: an idle one is closed then, a leased one stays
// usable by its holders and is closed at its last release, and no new
// lease reaches it. A lease after a stop opens the file uncached, for
// itself alone. Retirement does not revoke reads: cancellation and the
// output's ownership checks are what stop a stopped or replaced run from
// publishing.

// maxHandles is the descriptors the handle cache keeps open while any is
// idle (a variable for tests): the spill descriptors admission reserves.
var maxHandles = MaxSpillReaders

// HandleStats count the spill read handles' use (the report's
// resources.spill.read_handles).
type HandleStats struct {
	// Hits are leases served by an open handle, Misses leases that opened
	// the file; Raced counts misses whose open lost to another's for the
	// same file (the duplicate is closed at once).
	Hits   int64 `json:"hits"`
	Misses int64 `json:"misses"`
	Raced  int64 `json:"raced"`
	// Evictions are the handles taken out of the cache, by reason.
	Evictions HandleEvictions `json:"evictions"`
	// PeakOpen is the most descriptors the cache held open at once, leased
	// or idle (opens under way not counted).
	PeakOpen int64 `json:"peak_open"`
}

// HandleEvictions count the handles taken out of the cache: Capacity, an
// idle handle past maxHandles (the least recently used); Retired, its file
// drained; Truncated, its file truncated (an ENOSPC rollback); LowFD, idle
// when an open failed for want of descriptors; Closed, open when the run
// stopped (or its output was replaced).
type HandleEvictions struct {
	Capacity  int64 `json:"capacity"`
	Retired   int64 `json:"retired"`
	Truncated int64 `json:"truncated"`
	LowFD     int64 `json:"low_fd"`
	Closed    int64 `json:"closed"`
}

type evictReason int

const (
	evictCapacity evictReason = iota
	evictRetired
	evictTruncated
	evictLowFD
	evictClosed
)

// handles is the spill read handles (mu).
type handles struct {
	mu      sync.Mutex
	m       map[*spillFile]*handle
	open    int // descriptors open: leased, idle, or retired and still leased
	opening int // misses whose open is under way (their room made already)
	max     int // idle handles are kept while open+opening <= max
	tick    uint64
	closed  bool          // the run stopped: no handle is kept any more
	g       func(d int64) // the open readers gauge (nil in tests)
	st      HandleStats
}

// handle is one file's read descriptor and its leases.
type handle struct {
	fh   *os.File
	refs int
	used uint64
	gone bool // retired: out of the cache, closed at its last release
}

func newHandles(g func(int64)) *handles {
	return &handles{m: map[*spillFile]*handle{}, max: maxHandles, g: g}
}

// lease is f's handle, opened by open on a miss; release it when done.
func (c *handles) lease(f *spillFile, open func() (*os.File, error)) (*handle, error) {
	c.mu.Lock()
	if h := c.m[f]; h != nil {
		h.refs++
		c.tick++
		h.used = c.tick
		c.st.Hits++
		c.mu.Unlock()
		return h, nil
	}
	c.st.Misses++
	// Room is made before the open, which counts as under way until it
	// returns: concurrent misses cannot together pass the target with
	// idle handles open.
	c.evict(evictCapacity, c.max-1)
	c.opening++
	c.mu.Unlock()
	fh, err := open()
	if lowFD(err) {
		c.lowFD()
		fh, err = open()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opening--
	if err != nil {
		return nil, err
	}
	c.opened(1)
	h := &handle{fh: fh, refs: 1}
	if c.closed || f.drained {
		// Stopped, or f retired while this open was under way: the handle
		// serves this lease alone and is closed at its release.
		h.gone = true
		return h, nil
	}
	if o := c.m[f]; o != nil { // another lease opened f meanwhile
		c.st.Raced++
		c.closeFD(h)
		o.refs++
		c.tick++
		o.used = c.tick
		return o, nil
	}
	c.tick++
	h.used = c.tick
	c.m[f] = h
	c.evict(evictCapacity, c.max)
	return h, nil
}

// lowFD reports whether err is an open refused for want of descriptors.
func lowFD(err error) bool {
	return err != nil && (errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE))
}

// lowFD is the low-descriptor fallback, for any spill open refused for want
// of descriptors (a read miss, a frame's write): every idle handle is
// closed and none is kept from now on, so the open can be retried once.
func (c *handles) lowFD() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.max = 0
	c.evict(evictLowFD, 0)
}

// release ends a lease: a retired handle is closed at its last, an idle one
// past the cap too.
func (c *handles) release(h *handle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.refs--; h.refs > 0 {
		return
	}
	if h.gone {
		c.closeFD(h)
		return
	}
	c.evict(evictCapacity, c.max)
}

// retire takes f's handle out of the cache: closed now if idle, else at its
// last release.
func (c *handles) retire(f *spillFile, why evictReason) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if why == evictRetired {
		f.drained = true // no handle of f is kept from now on
	}
	if h := c.m[f]; h != nil {
		c.drop(f, h, why)
	}
}

// stop retires every cached handle and keeps none from now on (the run
// stopped, or its output was replaced): an idle one is closed now, a leased
// one at its last release; a later lease opens uncached.
func (c *handles) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for f, h := range c.m {
		c.drop(f, h, evictClosed)
	}
}

// evict closes the least recently used idle handles while more than limit
// descriptors are open or being opened (mu held).
func (c *handles) evict(why evictReason, limit int) {
	for c.open+c.opening > limit {
		var lf *spillFile
		var lh *handle
		for f, h := range c.m {
			if h.refs == 0 && (lh == nil || h.used < lh.used) {
				lf, lh = f, h
			}
		}
		if lh == nil {
			return
		}
		c.drop(lf, lh, why)
	}
}

// drop takes h, f's handle, out of the cache, counted by why (mu held).
func (c *handles) drop(f *spillFile, h *handle, why evictReason) {
	delete(c.m, f)
	switch why {
	case evictCapacity:
		c.st.Evictions.Capacity++
	case evictRetired:
		c.st.Evictions.Retired++
	case evictTruncated:
		c.st.Evictions.Truncated++
	case evictLowFD:
		c.st.Evictions.LowFD++
	case evictClosed:
		c.st.Evictions.Closed++
	}
	if h.refs == 0 {
		c.closeFD(h)
	} else {
		h.gone = true
	}
}

func (c *handles) closeFD(h *handle) {
	h.fh.Close()
	h.fh = nil
	c.opened(-1)
}

func (c *handles) opened(d int) {
	c.open += d
	c.st.PeakOpen = max(c.st.PeakOpen, int64(c.open))
	if c.g != nil {
		c.g(int64(d))
	}
}

// stats is a snapshot of the counters.
func (c *handles) stats() HandleStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.st
}

// openFDs is the descriptors open (tests).
func (c *handles) openFDs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.open
}
