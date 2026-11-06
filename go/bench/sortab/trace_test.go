// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sortab is the deciding experiment for ordered emission (the
// decision record's section 7): one provider trace listed through (A) the
// real pipeline (order stage, part router, journal, resume) and (B) a
// run-based external sort recovered from swath PR #248 commit 7d99970
// (sortout/), writing through the same part router and writer. The package
// has only test files: nothing here can be linked into the command
// (TestNotLinked checks the command's dependencies).
package sortab

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/s3"
)

// Shape is a deterministic synthetic keyspace: Ranges sibling ranges under
// the root, each a chain of LIST pages of PageKeys keys answered after a
// per-page latency. Keys are generated from their global index, never
// materialized.
//
//   - slowfirst: range 0 holds SlowKeys keys at SlowLatency per page; the
//     other ranges share the rest at Latency (a slow frontier, hundreds of
//     fast siblings finishing ahead of it).
//   - slowlast: the same with the slow range last.
//   - flat: equal ranges, Latency everywhere.
//   - skewed: range sizes ∝ 1/(rank+1)^Skew in a seeded order, Latency.
//
// Jitter (0..1) scales each page's latency by a deterministic factor in
// [1-Jitter, 1+Jitter].
type Shape struct {
	Name          string  `json:"name"`
	Keys          int64   `json:"keys"`
	Ranges        int     `json:"ranges"`
	PageKeys      int     `json:"page_keys"`
	LatencyMs     float64 `json:"latency_ms"`
	SlowLatencyMs float64 `json:"slow_latency_ms"`
	SlowKeys      int64   `json:"slow_keys"`
	Skew          float64 `json:"skew"`
	Jitter        float64 `json:"jitter"`
	Seed          uint64  `json:"seed"`
}

// layout is a shape's ranges: range i holds global key indices
// [start[i], start[i+1]); slow is the slow range or -1.
type layout struct {
	Shape
	start []int64
	slow  int
}

func (s Shape) layout() (*layout, error) {
	if s.Keys < 1 || s.Ranges < 1 || s.PageKeys < 1 || int64(s.Ranges) > s.Keys {
		return nil, fmt.Errorf("shape %+v: keys, ranges and page size must be positive, ranges <= keys", s)
	}
	l := &layout{Shape: s, start: make([]int64, s.Ranges+1), slow: -1}
	size := make([]int64, s.Ranges)
	even := func(n int64, from, to int) { // n keys over ranges [from, to)
		k := int64(to - from)
		for i := from; i < to; i++ {
			size[i] = n / k
			if int64(i-from) < n%k {
				size[i]++
			}
		}
	}
	switch s.Name {
	case "slowfirst", "slowlast":
		if s.Ranges < 2 || s.SlowKeys < 1 || s.SlowKeys >= s.Keys-int64(s.Ranges)+1 {
			return nil, fmt.Errorf("shape %s: needs 2+ ranges and 1 <= slow_keys < keys-ranges+1", s.Name)
		}
		l.slow = 0
		if s.Name == "slowlast" {
			l.slow = s.Ranges - 1
		}
		size[l.slow] = s.SlowKeys
		if l.slow == 0 {
			even(s.Keys-s.SlowKeys, 1, s.Ranges)
		} else {
			even(s.Keys-s.SlowKeys, 0, s.Ranges-1)
		}
	case "flat":
		even(s.Keys, 0, s.Ranges)
	case "skewed":
		rng := rand.New(rand.NewPCG(s.Seed, 1))
		perm := rng.Perm(s.Ranges)
		w := make([]float64, s.Ranges)
		var sum float64
		for i := range w {
			w[i] = 1 / math.Pow(float64(perm[i]+1), s.Skew)
			sum += w[i]
		}
		rest := s.Keys - int64(s.Ranges) // every range holds at least one key
		var used int64
		for i := range size {
			size[i] = 1 + int64(float64(rest)*w[i]/sum)
			used += size[i]
		}
		size[s.Ranges-1] += s.Keys - used
	default:
		return nil, fmt.Errorf("unknown shape %q (slowfirst, slowlast, flat, skewed)", s.Name)
	}
	for i, n := range size {
		l.start[i+1] = l.start[i] + n
	}
	return l, nil
}

// rangeOf is the range holding global index i.
func (l *layout) rangeOf(i int64) int {
	lo, hi := 0, len(l.start)-2
	for lo < hi {
		m := (lo + hi + 1) / 2
		if l.start[m] <= i {
			lo = m
		} else {
			hi = m - 1
		}
	}
	return lo
}

// keyDigits is the width of the global index at the end of every key.
const keyDigits = 12

// Key is global index i's key: "r%04d/obj-%012d", the range number then
// the index, so key order is index order.
func (l *layout) Key(i int64) string {
	b := make([]byte, 0, 32)
	return string(l.appendKey(b, i))
}

func (l *layout) appendKey(b []byte, i int64) []byte {
	r := l.rangeOf(i)
	b = append(b, 'r')
	b = appendPadded(b, int64(r), 4)
	b = append(b, "/obj-"...)
	return appendPadded(b, i, keyDigits)
}

func appendPadded(b []byte, v int64, w int) []byte {
	s := strconv.AppendInt(nil, v, 10)
	for n := len(s); n < w; n++ {
		b = append(b, '0')
	}
	return append(b, s...)
}

// indexOf parses a key's global index.
func indexOf(key string) (int64, error) {
	if len(key) < keyDigits {
		return 0, fmt.Errorf("key %q is not a trace key", key)
	}
	return strconv.ParseInt(key[len(key)-keyDigits:], 10, 64)
}

func mix(x uint64) uint64 { // splitmix64
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// Object is global index i's payload: deterministic size, time and ETag.
func (l *layout) Object(i int64) s3.Object {
	h1 := mix(uint64(i) ^ l.Seed)
	h2 := mix(h1)
	var e [16]byte
	binary.LittleEndian.PutUint64(e[:8], h1)
	binary.LittleEndian.PutUint64(e[8:], h2)
	return s3.Object{Size: int64(h1 % 50_000_000), Mtime: 1_700_000_000_000_000 + int64(h2%(365*86400*1_000_000)),
		ETag: hex.EncodeToString(e[:]), StorageClass: "STANDARD"}
}

// pageLatency is the latency of the page of range r starting at index i.
func (l *layout) pageLatency(r int, i int64) time.Duration {
	ms := l.LatencyMs
	if r == l.slow {
		ms = l.SlowLatencyMs
	}
	if l.Jitter > 0 {
		u := float64(mix(uint64(i)^l.Seed^0x5bd1e995)>>11) / (1 << 53)
		ms *= 1 + l.Jitter*(2*u-1)
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// Range is one range of the trace: the root (R < 0) or sibling R listed
// from global index From.
type Range struct {
	R    int
	From int64
}

// Provider lists a trace. Both arms use it unchanged; they differ only in
// what consumes its emitters. Every page is logged (range, first index,
// end index) to Log before its rows are handed on, so the driver can count
// requests and keys listed per attempt, killed or not.
type Provider struct {
	l *layout
	// Start, if set, says where range r (keys [lo, hi)) is listed from on
	// this attempt and whether it is skipped: a resume. nil lists all.
	Start func(r int, lo, hi int64) (from int64, skip bool)
	// KillAt, if positive, SIGKILLs the process once this many keys have
	// been listed in this attempt (the crash at a chosen point): keys of
	// the slow range when the shape has one, so the kill falls at the same
	// trace position in both arms (A's suffix may be held back by its
	// spill cap while the slow range lists), else keys of all ranges.
	KillAt int64
	// Done, if set, is called when range r has returned (B tracks it).
	Done func(r int)

	listed   atomic.Int64
	slowKeys atomic.Int64 // keys of the slow range listed
	requests atomic.Int64
	ranges   atomic.Int64 // ranges finished
	logMu    sync.Mutex
	log      *os.File
}

// NewProvider opens a provider over shape s, logging pages to logPath
// ("" logs nothing).
func NewProvider(s Shape, logPath string) (*Provider, error) {
	l, err := s.layout()
	if err != nil {
		return nil, err
	}
	p := &Provider{l: l}
	if logPath != "" {
		if p.log, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *Provider) Root() Range { return Range{R: -1} }

func (p *Provider) Expand(ctx context.Context, r Range, e source.Emitter[Range, s3.Object]) error {
	l := p.l
	if r.R < 0 {
		for i := range l.Ranges {
			lo, hi := l.start[i], l.start[i+1]
			from := lo
			if p.Start != nil {
				var skip bool
				if from, skip = p.Start(i, lo, hi); skip {
					continue
				}
			}
			if err := e.Child(Range{R: i, From: from}); err != nil {
				return err
			}
		}
		return nil
	}
	defer func() {
		p.ranges.Add(1)
		if p.Done != nil {
			p.Done(r.R)
		}
	}()
	end := l.start[r.R+1]
	i := r.From
	for {
		// One request per page; a chain that starts at its end still asks
		// once (an empty page, as a start-after at the end would).
		t := time.NewTimer(l.pageLatency(r.R, i))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		n := min(int64(l.PageKeys), end-i)
		p.requests.Add(1)
		p.logPage(r.R, i, i+n)
		if n <= 0 {
			return nil
		}
		rows := make([]schema.Row[s3.Object], n)
		arena := make([]byte, 0, int(n)*28)
		for k := range rows {
			a := len(arena)
			arena = l.appendKey(arena, i+int64(k))
			rows[k] = schema.Row[s3.Object]{Key: unsafeString(arena[a:]), Data: l.Object(i + int64(k))}
		}
		got := p.listed.Add(n)
		if r.R == l.slow {
			got = p.slowKeys.Add(n)
		} else if l.slow >= 0 {
			got = 0
		}
		if p.KillAt > 0 && got >= p.KillAt {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {} // the signal is on its way
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
		i += n
		if i >= end {
			return nil
		}
	}
}

func (p *Provider) logPage(r int, lo, hi int64) {
	if p.log == nil {
		return
	}
	b := strconv.AppendInt(nil, int64(r), 10)
	b = append(b, ' ')
	b = strconv.AppendInt(b, lo, 10)
	b = append(b, ' ')
	b = strconv.AppendInt(b, hi, 10)
	b = append(b, '\n')
	p.logMu.Lock()
	_, _ = p.log.Write(b) // one write per page: it survives a SIGKILL
	p.logMu.Unlock()
}

// Listed is the keys listed in this attempt.
func (p *Provider) Listed() int64 { return p.listed.Load() }

// Requests is the pages requested in this attempt.
func (p *Provider) Requests() int64 { return p.requests.Load() }
