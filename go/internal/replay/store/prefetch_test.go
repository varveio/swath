// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sameFields compares every field of two Objects by value.
func sameFields(a, b *Object) bool {
	sameTime := a.LastModified == nil && b.LastModified == nil ||
		a.LastModified != nil && b.LastModified != nil && a.LastModified.Equal(*b.LastModified) &&
			a.LastModified.Location() == b.LastModified.Location()
	return string(a.Key) == string(b.Key) && (a.Key == nil) == (b.Key == nil) && eqInt(a.Size, b.Size) && sameTime &&
		eqStr(a.ETag, b.ETag) && eqStr(a.StorageClass, b.StorageClass) && eqStr(a.OwnerID, b.OwnerID) &&
		eqStr(a.OwnerDisplayName, b.OwnerDisplayName) && eqStr(a.ChecksumAlgorithm, b.ChecksumAlgorithm) &&
		eqStr(a.ChecksumType, b.ChecksumType)
}

// prefetchMeterCounts counts the window cache's events.
type prefetchMeterCounts struct {
	hits, joins, evictions, claimed, cold, continuation, uncached atomic.Int64
}

func (c *prefetchMeterCounts) meters() Meters {
	return Meters{Prefetch: PrefetchMeters{
		Hit: func(struct{}) { c.hits.Add(1) },
		Join: func(reason string) {
			if reason == "hit" {
				c.joins.Add(1)
			}
		},
		Eviction: func(bool) { c.evictions.Add(1) },
		Anchor: func(event string) {
			if event == "claimed" {
				c.claimed.Add(1)
			}
		},
		Miss: func(reason string) {
			if reason == "cold" {
				c.cold.Add(1)
			} else {
				c.continuation.Add(1)
			}
		},
		Uncached: func(string) { c.uncached.Add(1) },
	}}
}

// TestWindowCacheMatchesTheFixture serves every conformance fixture
// through a small window cache (37-row windows, five of them, so reads
// join windows, evict them and outgrow them) and through the bare reader,
// with interleaved walks, seeks, probes and bounded reads in both
// projections, and requires every answer field by field equal: a slice of
// a window is the read of the same bounds. Eight walkers then read the
// cached store at once (the race detector checks the cache's locking).
func TestWindowCacheMatchesTheFixture(t *testing.T) {
	ctx := context.Background()
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			bare, _, err := Open(ctx, f.path, Options{Prefetch: Prefetch{Off: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer bare.Close()
			opened, _, err := Open(ctx, f.path, Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			cached := opened.(*Sorted)
			var counts prefetchMeterCounts
			cached.SetMeters(counts.meters())
			rows, _ := reference(t, f.path)
			keyAt := func(r *rand.Rand) []byte {
				k := []byte(rows[r.IntN(len(rows))].key)
				switch r.IntN(4) {
				case 0:
					return append(k, 0)
				case 1:
					return k[:len(k)-1]
				}
				return k
			}
			check := func(how string, from []byte, inclusive bool, to []byte, limit int, proj Projection) []Object {
				t.Helper()
				want, err := bare.Rows(ctx, from, inclusive, to, limit, proj)
				if err != nil {
					t.Fatal(err)
				}
				got, err := cached.Rows(ctx, from, inclusive, to, limit, proj)
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) {
					t.Fatalf("%s from %q/%v to %q limit %d owner %v: %d rows, want %d", how, from, inclusive, to, limit, proj.Owner, len(got), len(want))
				}
				for i := range got {
					if !sameFields(&got[i], &want[i]) {
						t.Fatalf("%s from %q/%v to %q limit %d owner %v: row %d %+v, want %+v", how, from, inclusive, to, limit, proj.Owner, i, got[i], want[i])
					}
				}
				return got
			}
			// One walk of 7-row pages through several windows, then reads
			// of 10 rows from each key it was served: those that start
			// near a window's end join it with the next.
			var served [][]byte
			from := []byte(rows[len(rows)/2].key)
			for range 15 {
				page := check("walk", from, false, nil, 7, Projection{})
				if len(page) == 0 {
					break
				}
				for i := range page {
					served = append(served, page[i].Key)
				}
				from = page[len(page)-1].Key
			}
			for _, key := range served {
				check("join", key, false, nil, 10, Projection{})
			}
			r := rand.New(rand.NewPCG(1, uint64(len(f.name))))
			type walk struct {
				from  []byte
				to    []byte
				limit int
				proj  Projection
			}
			walks := make([]walk, 6)
			for i := range walks {
				walks[i] = walk{from: keyAt(r), limit: 1 + r.IntN(20), proj: Projection{Owner: i%3 == 0}}
				if i == 5 {
					walks[i].to = keyAt(r)
				}
			}
			steps := 1500
			if raceEnabled || testing.Short() {
				steps = 400
			}
			for range steps {
				switch r.IntN(10) {
				case 0: // a probe
					check("probe", keyAt(r), r.IntN(2) == 0, nil, 1, Projection{})
				case 1: // a seek with an upper bound, or a read longer than a window
					from, to := keyAt(r), keyAt(r)
					if r.IntN(3) == 0 {
						to = nil
					}
					check("seek", from, r.IntN(2) == 0, to, 1+r.IntN(60), Projection{Owner: r.IntN(2) == 0})
				default: // a walk's next page, resuming after its last key
					w := &walks[r.IntN(len(walks))]
					page := check("walk", w.from, false, w.to, w.limit, w.proj)
					if len(page) < w.limit {
						w.from = keyAt(r)
						continue
					}
					w.from = page[len(page)-1].Key
				}
			}
			// The local tree's seven rows fit one window: nothing to join.
			if counts.hits.Load() == 0 || counts.joins.Load() == 0 && len(rows) > 100 || counts.evictions.Load() == 0 ||
				counts.claimed.Load() == 0 || counts.uncached.Load() == 0 {
				t.Fatalf("the reads did not reach every path: hits %d joins %d evictions %d claimed %d uncached %d",
					counts.hits.Load(), counts.joins.Load(), counts.evictions.Load(), counts.claimed.Load(), counts.uncached.Load())
			}
			stats, ok := cached.PrefetchStats()
			if !ok || stats.Windows > 5 || stats.Rows > 5*37 {
				t.Fatalf("live windows %d rows %d, bound 5 and %d", stats.Windows, stats.Rows, 5*37)
			}
			// Concurrent walks over the cached store.
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for g := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					r := rand.New(rand.NewPCG(uint64(g), 7))
					from := keyAt(r)
					for range 60 {
						want, err := bare.Rows(ctx, from, false, nil, 10, Projection{})
						if err != nil {
							errs <- err
							return
						}
						got, err := cached.Rows(ctx, from, false, nil, 10, Projection{})
						if err != nil {
							errs <- err
							return
						}
						if len(got) != len(want) {
							errs <- fmt.Errorf("walker %d: %d rows, want %d", g, len(got), len(want))
							return
						}
						for i := range got {
							if !sameFields(&got[i], &want[i]) {
								errs <- fmt.Errorf("walker %d row %d differs", g, i)
								return
							}
						}
						if len(got) < 10 {
							from = keyAt(r)
							continue
						}
						from = got[len(got)-1].Key
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
		})
	}
}

// TestWindowCacheHitsTakeNoReaderSlot fills a window, then holds every
// reader slot and reads the window's next pages: they are answered from
// the window at once, as the reader slots bound only fixture reads.
func TestWindowCacheHitsTakeNoReaderSlot(t *testing.T) {
	ctx := context.Background()
	f := fixtures(t)[0]
	opened, _, err := Open(ctx, f.path, Options{Readers: 1, Prefetch: Prefetch{WindowRows: 100, MaxWindows: 4}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	s := opened.(*Sorted)
	// A first page registers its tail; the continuation fill ramps to 4x.
	page, err := s.Rows(ctx, nil, true, nil, 10, Projection{})
	if err != nil {
		t.Fatal(err)
	}
	page, err = s.Rows(ctx, page[len(page)-1].Key, false, nil, 10, Projection{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.sem.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	defer s.sem.Release(1)
	short, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for range 3 {
		page, err = s.Rows(short, page[len(page)-1].Key, false, nil, 10, Projection{})
		if err != nil || len(page) != 10 {
			t.Fatalf("a page the window holds waited for a reader slot: %d rows, %v", len(page), err)
		}
	}
	// The page after the window needs the fixture, and so the slot.
	quick, cancelQuick := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelQuick()
	if _, err := s.Rows(quick, page[len(page)-1].Key, false, nil, 10, Projection{}); err == nil {
		t.Fatal("a fill past the window was served without a reader slot")
	}
}

// TestWindowCacheReadsAhead walks 60 pages of 10 rows through 100-row
// windows: after its first page (cold) and its first continuation fill,
// every window the walk enters was read ahead while it read the last
// quarter of the one before, so the walk's later pages are all hits, and
// the read-aheads' fills are counted as fills.
func TestWindowCacheReadsAhead(t *testing.T) {
	ctx := context.Background()
	f := fixtures(t)[0]
	opened, _, err := Open(ctx, f.path, Options{Prefetch: Prefetch{WindowRows: 100, MaxWindows: 4}})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	s := opened.(*Sorted)
	var counts prefetchMeterCounts
	var fills atomic.Int64
	m := counts.meters()
	m.Prefetch.Fill = func(time.Duration, int) { fills.Add(1) }
	s.SetMeters(m)
	var from []byte
	for page := range 60 {
		rows, err := s.Rows(ctx, from, from == nil, nil, 10, Projection{})
		if err != nil || len(rows) != 10 {
			t.Fatalf("page %d: %d rows, %v", page, len(rows), err)
		}
		from = rows[len(rows)-1].Key
	}
	if counts.cold.Load() != 1 || counts.continuation.Load() != 1 || counts.hits.Load() != 58 || fills.Load() < 7 {
		t.Fatalf("cold %d continuation %d hits %d fills %d; want 1, 1, 58 and the read-aheads' fills",
			counts.cold.Load(), counts.continuation.Load(), counts.hits.Load(), fills.Load())
	}
}

// syntheticKeys serves a prefetcher's fills from the keys k00000000 up,
// packed key only. A fill whose context lacks walkCtx (a read-ahead,
// under the store's context) blocks until release is closed or the
// context ends; blocked counts those waiting.
type syntheticKeys struct {
	rows    int
	release chan struct{}
	blocked atomic.Int64
}

type walkCtxKey struct{}

func (k *syntheticKeys) read(ctx context.Context, from []byte, inclusive bool, to []byte, limit int, _ Projection) (packed, error) {
	if ctx.Value(walkCtxKey{}) == nil {
		k.blocked.Add(1)
		select {
		case <-k.release:
		case <-ctx.Done():
		}
		k.blocked.Add(-1)
		if err := ctx.Err(); err != nil {
			return packed{}, err
		}
	}
	start := 0
	if from != nil {
		start = sort.Search(k.rows, func(i int) bool {
			c := bytes.Compare([]byte(fmt.Sprintf("k%08d", i)), from)
			return c > 0 || c == 0 && inclusive
		})
	}
	sink := s3Objects.packer(nil, limit, 0)
	for i := start; i < k.rows && i-start < limit; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		if to != nil && bytes.Compare(key, to) >= 0 {
			break
		}
		sink.Keys([][]byte{key})
	}
	return sink.packed()
}

// TestReadAheadsPendingStayWithinTheirSlots advances 40 disjoint walks to
// their read-ahead thresholds while every read-ahead's fill is blocked:
// the read-aheads registered (and their goroutines) never exceed the
// read-ahead slots, the walks' own pages are not held up, and evicting
// the windows the blocked read-aheads read on from cancels them.
func TestReadAheadsPendingStayWithinTheirSlots(t *testing.T) {
	keys := &syntheticKeys{rows: 1_000_000, release: make(chan struct{})}
	p := newPrefetcher(Prefetch{WindowRows: 100, MaxWindows: 1000}, keys.read, nil)
	defer p.close()
	defer close(keys.release)
	slots := cap(p.aheadSlots)
	ctx := context.WithValue(context.Background(), walkCtxKey{}, true)
	const walks = 40
	for w := range walks {
		// A cold page, a continuation that ramps to a 40-row window, and
		// two hits that leave its last quarter: the read-ahead threshold.
		from, inclusive := []byte(fmt.Sprintf("k%08d", w*10_000)), true
		for page := range 4 {
			rows, err := p.serve(ctx, from, inclusive, nil, 10, Projection{})
			if err != nil || len(rows) != 10 {
				t.Fatalf("walk %d page %d: %d rows, %v", w, page, len(rows), err)
			}
			from, inclusive = rows[len(rows)-1].Key, false
		}
		p.mu.Lock()
		pending := len(p.ahead)
		p.mu.Unlock()
		if pending > slots {
			t.Fatalf("after %d walks: %d read-aheads pending, slots %d", w+1, pending, slots)
		}
	}
	p.mu.Lock()
	pending := len(p.ahead)
	p.mu.Unlock()
	if pending != slots {
		t.Fatalf("%d read-aheads pending, want the %d slots taken", pending, slots)
	}
	// Evicting every window abandons the blocked read-aheads.
	p.mu.Lock()
	for i := range p.maxWindows {
		p.publish(&window{key: windowKey{lower: fmt.Sprint("evict", i), hasLower: true}})
	}
	p.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p.mu.Lock()
		pending = len(p.ahead)
		p.mu.Unlock()
		if pending == 0 && keys.blocked.Load() == 0 && len(p.aheadSlots) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after eviction: %d read-aheads pending, %d fills blocked, %d slots taken", pending, keys.blocked.Load(), len(p.aheadSlots))
		}
		time.Sleep(time.Millisecond)
	}
}
