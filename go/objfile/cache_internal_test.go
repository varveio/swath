// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func (c *ByteCache) state() (used int64, entries int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.entries
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

// TestByteCacheConcurrentBound: under concurrent admissions and copies,
// the bound holds at every moment it can be observed.
func TestByteCacheConcurrentBound(t *testing.T) {
	c, _ := NewByteCache(4096)
	data := bytes.Repeat([]byte("y"), 1024)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var over int64
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if used, _ := c.state(); used > c.max {
				over = used
			}
		}
	})
	var workers sync.WaitGroup
	for g := range 8 {
		workers.Go(func() {
			obj := cacheObject{scope: "s", name: fmt.Sprint(g % 3), identity: "1"}
			p := make([]byte, 100)
			for i := range 500 {
				off := int64(i%7) * 1024
				if !c.copyOut(obj, off, p) {
					c.admit(obj, off, data[:1+i%1024])
				}
			}
		})
	}
	workers.Wait()
	close(stop)
	wg.Wait()
	if over > 0 {
		t.Fatalf("the cache held %d bytes, over its 4096", over)
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
