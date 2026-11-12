// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func (c *ByteCache) state() (used int64, entries int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.entries
}

// TestByteCacheAdmissionPanicRelease: an unwound admission returns both
// its reserved bytes and its entry slot, leaving the cache usable.
func TestByteCacheAdmissionPanicRelease(t *testing.T) {
	c, _ := NewByteCache(8)
	obj := cacheObject{scope: "s", name: "a", identity: "1"}
	data := []byte("12345678")
	c.admitReserved = func() {
		if used, n := c.state(); used != int64(len(data)) || n != 1 {
			t.Errorf("before panic: %d bytes, %d entries; want 8, 1", used, n)
		}
		panic("admission panic")
	}
	func() {
		defer func() {
			if p := recover(); p != "admission panic" {
				t.Errorf("panic = %v, want admission panic", p)
			}
		}()
		c.admit(obj, 0, data)
		t.Error("admission did not panic")
	}()
	if used, n := c.state(); used != 0 || n != 0 {
		t.Fatalf("after panic: %d bytes, %d entries; want 0, 0", used, n)
	}
	if c.lru.Len() != 0 || len(c.objects) != 0 {
		t.Fatal("the failed admission published an entry")
	}
	c.admitReserved = nil
	c.admit(obj, 0, data)
	p := make([]byte, len(data))
	if !c.copyOut(obj, 0, p) || !bytes.Equal(p, data) {
		t.Fatal("the cache could not admit and serve data after the panic")
	}
	if used, n := c.state(); used != int64(len(data)) || n != 1 {
		t.Fatalf("after admission: %d bytes, %d entries; want 8, 1", used, n)
	}
}

// TestByteCacheBound: the bytes the cache owns, reservations and dropped
// entries still being copied from included, never pass its limit; an
// extent larger than the limit is not kept; LRU order decides eviction.
func TestByteCacheBound(t *testing.T) {
	c, _ := NewByteCache(1000)
	obj := cacheObject{scope: "s", name: "a", identity: "1"}
	data := bytes.Repeat([]byte("x"), 2000)
	c.admit(obj, 0, data) // larger than the cache
	if used, n := c.state(); used != 0 || n != 0 {
		t.Fatalf("an oversized extent was kept: %d bytes, %d entries", used, n)
	}
	for i := range 4 {
		c.admit(obj, int64(i)*1000, data[:400])
	}
	if used, n := c.state(); used != 800 || n != 2 {
		t.Fatalf("after four 400-byte admissions: %d bytes, %d entries; want 800, 2", used, n)
	}
	p := make([]byte, 10)
	if c.copyOut(obj, 0, p) || c.copyOut(obj, 1000, p) || !c.copyOut(obj, 2000, p) || !c.copyOut(obj, 3390, p) {
		t.Fatal("the two oldest entries were not the ones dropped")
	}
	if c.copyOut(obj, 3395, p) {
		t.Fatal("an extent past an entry's end was served")
	}
	// The entry at 2000, used last but one, goes before the one at 3000.
	c.copyOut(obj, 3000, p)
	c.admit(obj, 5000, data[:400])
	if c.copyOut(obj, 2000, p) || !c.copyOut(obj, 3000, p) {
		t.Fatal("eviction did not follow use")
	}

	// A dropped entry still being copied from keeps counting.
	c.mu.Lock()
	e := c.find(obj, 3000, 1)
	e.readers++
	c.mu.Unlock()
	c.admit(obj, 6000, data[:700]) // drops both: 3000 is still read
	c.mu.Lock()
	dropped, used := e.dropped, c.used
	c.mu.Unlock()
	if !dropped || used > 1000 {
		t.Fatalf("dropped %v, %d bytes: the read entry must stay counted and the bound kept", dropped, used)
	}
	c.mu.Lock()
	if e.readers--; e.readers == 0 && e.dropped {
		c.used -= int64(len(e.data))
	}
	c.mu.Unlock()
}

// TestByteCacheConcurrentBound: a pending admission's bytes count against
// concurrent admissions, before it publishes an entry that can be evicted.
func TestByteCacheConcurrentBound(t *testing.T) {
	c, _ := NewByteCache(4096)
	data := bytes.Repeat([]byte("y"), 3072)
	obj := cacheObject{scope: "s", name: "pending", identity: "1"}
	reserved, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	c.admitReserved = func() {
		if first.CompareAndSwap(false, true) {
			close(reserved)
			<-release
		}
	}
	done := make(chan struct{})
	go func() { c.admit(obj, 0, data); close(done) }()
	<-reserved
	defer func() { close(release); <-done }()
	if used, n := c.state(); used != 3072 || n != 1 {
		t.Fatalf("pending admission: %d bytes, %d entries; want 3072, 1", used, n)
	}

	var workers sync.WaitGroup
	for g := range 8 {
		workers.Go(func() {
			other := cacheObject{scope: "s", name: fmt.Sprint(g), identity: "1"}
			c.admit(other, 0, data[:2048]) // cannot fit alongside the reservation
		})
	}
	workers.Wait()
	if used, n := c.state(); used != 3072 || n != 1 {
		t.Fatalf("after competing admissions: %d bytes, %d entries; want 3072, 1", used, n)
	}
	c.admit(obj, 4096, data[:1024]) // the unreserved bytes remain usable
	if used, n := c.state(); used != c.max || n != 2 {
		t.Fatalf("after filling the cache: %d bytes, %d entries; want 4096, 2", used, n)
	}
}

func TestByteCacheLookupScanBound(t *testing.T) {
	c, _ := NewByteCache(1000)
	obj := cacheObject{scope: "s", name: "a", identity: "1"}
	for off := 1; off <= maxEntryScan; off++ {
		c.admit(obj, int64(off), []byte("x"))
	}
	c.admit(obj, 0, bytes.Repeat([]byte("x"), maxEntryScan+10))
	// The covering entry at zero is the last candidate within the scan.
	if !c.copyOut(obj, maxEntryScan-1, make([]byte, 3)) {
		t.Fatal("the last entry within the lookup scan was missed")
	}
	// One more nearer entry puts it outside the scan, despite covering the read.
	if c.copyOut(obj, maxEntryScan+1, make([]byte, 1)) {
		t.Fatal("lookup scanned past maxEntryScan entries")
	}
}

// TestByteCacheEntryBound: tiny extents are bounded by count as well.
func TestByteCacheEntryBound(t *testing.T) {
	c, _ := NewByteCache(1 << 30)
	obj := cacheObject{scope: "s", name: "a", identity: "1"}
	for i := range maxCacheEntries + 10 {
		c.admit(obj, int64(i)*2, []byte{1})
	}
	if _, n := c.state(); n != maxCacheEntries {
		t.Fatalf("%d entries, want at most %d", n, maxCacheEntries)
	}
}

// TestByteCacheEntryBoundConcurrent: an admission reserves its entry's
// slot with its bytes, so admissions in progress together cannot pass the
// entry bound when they publish.
func TestByteCacheEntryBoundConcurrent(t *testing.T) {
	c, _ := NewByteCache(1 << 30)
	obj := cacheObject{scope: "s", name: "a", identity: "1"}
	for i := range maxCacheEntries - 1 {
		c.admit(obj, int64(i)*2, []byte{1})
	}
	reserved, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	c.admitReserved = func() {
		if first.CompareAndSwap(false, true) { // the first admission waits between reservation and publication
			close(reserved)
			<-release
		}
	}
	done := make(chan struct{})
	go func() { c.admit(obj, 1<<40, []byte{1}); close(done) }()
	<-reserved
	c.admit(obj, 1<<41, []byte{1}) // takes the last slot, or drops one for it
	close(release)
	<-done
	if _, n := c.state(); n > maxCacheEntries {
		t.Fatalf("%d entries, over the bound of %d", n, maxCacheEntries)
	}
	c.mu.Lock()
	published := c.lru.Len()
	c.mu.Unlock()
	if published > maxCacheEntries {
		t.Fatalf("%d entries published, over the bound of %d", published, maxCacheEntries)
	}
}
