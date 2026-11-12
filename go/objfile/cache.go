// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"sync"
	"sync/atomic"
)

// ByteCache is a concurrent, byte-bounded cache of validated object data.
// It may be shared by multiple Cache wrappers. Its zero value is disabled.
//
// It holds the bytes of complete reads, each under its key: the scope and
// name of a Cache wrapper, the Identity of the version read, and the
// extent. A lookup scans only the nearest maxEntryScan entries starting
// at or before the read's offset, so it can miss an older covering entry.
// A read is served from one entry that holds all of its bytes.
// When an admission does not fit, the least recently used entries are
// dropped; an entry a reader is still copying from counts until it is
// done.
type ByteCache struct {
	max int64

	mu            sync.Mutex
	used          int64     // payload owned: entries, dropped ones still read, reservations
	entries       int       // entries published and admissions reserved, at most maxCacheEntries
	lru           list.List // of *cacheEntry, most recently used at the front
	objects       map[cacheObject][]*cacheEntry
	admitReserved func() // test-only handshake between reservation and publication
}

// maxCacheEntries bounds a ByteCache's bookkeeping apart from its bytes:
// past it, the least recently used entry is dropped for a new one.
const maxCacheEntries = 1 << 16

// maxEntryScan limits a lookup to the nearest entries of one object that
// start at or before the read's offset. An older covering entry can be missed.
const maxEntryScan = 8

// cacheObject is a version of an object as a Cache wrapper names it.
type cacheObject struct {
	scope, name, identity string
}

type cacheEntry struct {
	obj     cacheObject
	off     int64
	data    []byte
	elem    *list.Element
	readers int  // lookups copying from data
	dropped bool // evicted while readers copied: counted until they finish
}

// NewByteCache returns a cache whose cache-owned payload allocations,
// including pending admissions and entries still being copied, total
// at most maxBytes. Zero disables caching. A negative limit returns
// an error wrapping fs.ErrInvalid.
func NewByteCache(maxBytes int64) (*ByteCache, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("objfile: byte cache of %d bytes: %w", maxBytes, fs.ErrInvalid)
	}
	return &ByteCache{max: maxBytes, objects: map[cacheObject][]*cacheEntry{}}, nil
}

func (c *ByteCache) enabled() bool { return c != nil && c.max > 0 }

// copyOut copies [off, off+len(p)) of obj into p from an entry that holds
// it, reporting whether one did.
func (c *ByteCache) copyOut(obj cacheObject, off int64, p []byte) bool {
	c.mu.Lock()
	e := c.find(obj, off, int64(len(p)))
	if e == nil {
		c.mu.Unlock()
		return false
	}
	e.readers++
	c.lru.MoveToFront(e.elem)
	c.mu.Unlock()

	copy(p, e.data[off-e.off:])

	c.mu.Lock()
	if e.readers--; e.readers == 0 && e.dropped {
		c.used -= int64(len(e.data))
	}
	c.mu.Unlock()
	return true
}

// find returns an entry of obj that holds [off, off+n), or nil. c.mu is
// held.
func (c *ByteCache) find(obj cacheObject, off, n int64) *cacheEntry {
	es := c.objects[obj]
	// The entries that start at or before off, nearest first.
	i, _ := slices.BinarySearchFunc(es, off, func(e *cacheEntry, off int64) int {
		if e.off <= off {
			return -1
		}
		return 1
	})
	for k := i - 1; k >= 0 && k >= i-maxEntryScan; k-- {
		if e := es[k]; off+n <= e.off+int64(len(e.data)) {
			return e
		}
	}
	return nil
}

// admit keeps a copy of p, the bytes of obj from off, when it fits: its
// bytes are reserved first, dropping least recently used entries, and
// allocated and copied only once reserved. An extent already held, or one
// that does not fit, is not kept.
func (c *ByteCache) admit(obj cacheObject, off int64, p []byte) {
	n := int64(len(p))
	if n == 0 || n > c.max {
		return
	}
	c.mu.Lock()
	if c.find(obj, off, n) != nil {
		c.mu.Unlock()
		return
	}
	for c.used+n > c.max || c.entries >= maxCacheEntries {
		if !c.dropOldest() {
			break
		}
	}
	if c.used+n > c.max || c.entries >= maxCacheEntries {
		c.mu.Unlock() // what remains is being read or admitted: not kept
		return
	}
	c.used += n // reserved, with an entry's slot
	c.entries++
	c.mu.Unlock()
	published := false
	defer func() {
		if !published {
			c.mu.Lock()
			c.used -= n
			c.entries--
			c.mu.Unlock()
		}
	}()
	if c.admitReserved != nil {
		c.admitReserved()
	}

	data := make([]byte, n)
	copy(data, p)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.find(obj, off, n) != nil { // admitted meanwhile
		return
	}
	e := &cacheEntry{obj: obj, off: off, data: data}
	e.elem = c.lru.PushFront(e)
	es := c.objects[obj]
	i, _ := slices.BinarySearchFunc(es, off, func(e *cacheEntry, off int64) int {
		if e.off <= off {
			return -1
		}
		return 1
	})
	c.objects[obj] = slices.Insert(es, i, e)
	published = true
}

// dropOldest drops the least recently used entry, reporting whether there
// was one. Its bytes stop counting once no reader copies from it. c.mu is
// held.
func (c *ByteCache) dropOldest() bool {
	back := c.lru.Back()
	if back == nil {
		return false
	}
	e := back.Value.(*cacheEntry)
	c.lru.Remove(back)
	c.entries--
	es := c.objects[e.obj]
	if i := slices.Index(es, e); i >= 0 {
		es = slices.Delete(es, i, i+1)
	}
	if len(es) == 0 {
		delete(c.objects, e.obj)
	} else {
		c.objects[e.obj] = es
	}
	if e.readers > 0 {
		e.dropped = true
	} else {
		c.used -= int64(len(e.data))
	}
	return true
}

// Cache wraps o with identity-keyed byte caching. scope identifies the
// storage namespace and the interpretation of names passed to this
// wrapper (an opaque string, never parsed). Equal scopes permit sharing;
// callers must distinguish endpoints, containers, roots, and access
// boundaries. A name is the one given to this wrapper: placed below Root,
// it includes Root's prefix.
//
// Opens always delegate to o. Reads are cache-eligible only when the
// wrapped Object exposes a non-empty Identity at invocation entry (once
// for a whole batch): the first read of an OpenSized Object, or one Pin
// has not yet checked, neither uses nor fills the cache, so a cached byte
// never establishes or stands in for an identity check. Only complete,
// successfully validated extents are admitted: a ReadAt's, or each range
// of a batch read without error. A batch serves the ranges the cache holds
// and sends the rest as one ReadRanges of the wrapped Object, under its
// policy, errors reported at the ranges' own positions.
//
// Cache preserves RangeReader, delegates ReadObject without caching,
// honors CacheBypass, and preserves truthful IOReporter behavior. A nil or
// disabled cache leaves reads uncached: Cache returns o. An unknown
// CacheMode fails a context-bearing operation before I/O. Bytes served
// from the cache are reported to the context's Observer (CacheHit).
//
// Cache does not coalesce concurrent reads of the same extent: each miss
// is its own request.
func Cache(o Opener, scope string, c *ByteCache) Opener {
	if !c.enabled() {
		return o
	}
	return cached{o: o, scope: scope, c: c}
}

type cached struct {
	o     Opener
	scope string
	c     *ByteCache
}

func (w cached) ReportsIO() bool { return reportsIO(w.o) }

func (w cached) Open(ctx context.Context, name string) (Object, error) {
	if _, err := cacheMode(ctx); err != nil {
		return nil, err
	}
	f, err := w.o.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	return w.wrap(name, f), nil
}

func (w cached) OpenSized(ctx context.Context, name string, size int64) (Object, error) {
	if _, err := cacheMode(ctx); err != nil {
		return nil, err
	}
	f, err := w.o.OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return w.wrap(name, f), nil
}

func (w cached) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	if _, err := cacheMode(ctx); err != nil {
		return nil, "", err
	}
	return ReadObject(ctx, w.o, name, limit)
}

func (w cached) wrap(name string, f Object) Object {
	return &cachedObject{Object: f, c: w.c, scope: w.scope, name: name}
}

type cachedObject struct {
	Object
	c           *ByteCache
	scope, name string
	closed      atomic.Bool
}

func (f *cachedObject) ReportsIO() bool { return reportsIO(f.Object) }

func (f *cachedObject) batchPolicy() BatchPolicy { return policyOf(f.Object) }

func (f *cachedObject) Close() error {
	f.closed.Store(true)
	return f.Object.Close()
}

func (f *cachedObject) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(context.Background(), p, off)
}

func (f *cachedObject) WithContext(ctx context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, off int64) (int, error) { return f.readAt(ctx, p, off) })
}

// key returns the cache key of the Object's version as it is now, or false
// when the read under ctx is not cache-eligible: bypassed, closed, or not
// yet pinned.
func (f *cachedObject) key(mode CacheMode) (cacheObject, bool) {
	if mode != CacheDefault || f.closed.Load() {
		return cacheObject{}, false
	}
	id := f.Object.Identity()
	return cacheObject{scope: f.scope, name: f.name, identity: id}, id != ""
}

func (f *cachedObject) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	mode, err := cacheMode(ctx)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if ctx.Value(cacheBatchKey{}) != nil {
		return f.Object.WithContext(ctx).ReadAt(p, off)
	}
	key, ok := f.key(mode)
	if !ok || off < 0 || off >= f.Size() || len(p) == 0 {
		return f.Object.WithContext(ctx).ReadAt(p, off)
	}
	want := p[:min(int64(len(p)), f.Size()-off)]
	var eof error
	if len(want) < len(p) {
		eof = io.EOF
	}
	if f.c.copyOut(key, off, want) {
		if obs := ObserverFromContext(ctx); obs != nil {
			obs.CacheHit(int64(len(want)))
		}
		return len(want), eof
	}
	n, err := f.Object.WithContext(ctx).ReadAt(p, off)
	if n >= len(want) && (err == nil || err == io.EOF) {
		f.c.admit(key, off, want)
	}
	return n, err
}

// ReadRanges serves the ranges the cache holds and reads the rest as one
// batch of the wrapped Object, as Cache says.
func (f *cachedObject) ReadRanges(ctx context.Context, ranges []Range) error {
	return ReadRangesWith(ctx, f, ranges, policyOf(f.Object))
}

// readRanges filters original inputs before planning and admits only validated
// misses. key was frozen at the public batch boundary.
func (f *cachedObject) readRanges(ctx context.Context, ranges []Range, key cacheObject, next func([]Range) error) error {
	// ReadRangesWith has validated the whole original batch before any copy.
	var (
		misses []Range
		at     []int // the position in ranges of each miss
		hit    int64
	)
	for i, r := range ranges {
		if len(r.Buf) == 0 {
			continue
		}
		if f.c.copyOut(key, r.Off, r.Buf) {
			hit += int64(len(r.Buf))
			continue
		}
		misses = append(misses, r)
		at = append(at, i)
	}
	if hit > 0 {
		if obs := ObserverFromContext(ctx); obs != nil {
			obs.CacheHit(hit)
		}
	}
	if len(misses) == 0 {
		return nil
	}
	err := next(misses)
	var be *BatchError
	if err != nil && !errors.As(err, &be) && hit == 0 {
		return err // as the batch would fail uncached
	}
	if err != nil && be == nil {
		// The whole batch of misses failed; the hits stand.
		errs := make([]error, len(ranges))
		for _, i := range at {
			errs[i] = err
		}
		return &BatchError{Errs: errs}
	}
	var errs []error
	for k, r := range misses {
		if be != nil && be.Errs[k] != nil {
			if errs == nil {
				errs = make([]error, len(ranges))
			}
			errs[at[k]] = be.Errs[k]
			continue
		}
		f.c.admit(key, r.Off, r.Buf)
	}
	if errs != nil {
		return &BatchError{Errs: errs}
	}
	return nil
}
