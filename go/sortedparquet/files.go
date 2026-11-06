// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// FileCache bounds the descriptors a set of part files holds open. A part
// is a *CachedFile: each read leases an open handle for its duration,
// reopening the file by path when its handle was evicted, so a fixture of
// any number of parts holds at most Max descriptors, the least recently
// used idle handle going first when one more is needed. Every open, the
// first included, happens only after the cache has reserved its slot, so
// the descriptors open never exceed Max.
//
// Opens do not block on what a path names: on unix a file is opened
// non-blocking, and anything but a regular file is refused before it is
// read (a FIFO put in a part's place cannot stall a read). A reopened
// file must be the one first opened (os.SameFile), or the read fails: a
// part is never read from a file that replaced it.
//
// It is safe for concurrent use. A read waits while Max handles are all
// leased, until one is released, its context ends or its file is closed;
// reads of one file share its handle (ReadAt is safe concurrently).
type FileCache struct {
	max     int
	mu      sync.Mutex
	changed chan struct{} // closed and replaced on every release, eviction or close
	open    int           // descriptors open or being opened
	idle    list.List     // *CachedFile with an open handle and no lease, oldest first

	peak, opens int
}

// NewFileCache returns a cache holding at most max descriptors; max below
// one is DefaultMaxOpenFiles().
func NewFileCache(max int) *FileCache {
	if max < 1 {
		max = DefaultMaxOpenFiles()
	}
	return &FileCache{max: max, changed: make(chan struct{})}
}

// FileCacheStats are a cache's counts.
type FileCacheStats struct {
	// Open is the descriptors open now, Peak the most at once, and Opens
	// the files opened in all, first opens and reopens after eviction.
	Open, Peak, Opens int
}

// Stats returns the cache's counts.
func (c *FileCache) Stats() FileCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return FileCacheStats{Open: c.open, Peak: c.peak, Opens: c.opens}
}

// Max returns the descriptor bound.
func (c *FileCache) Max() int { return c.max }

// notifyLocked wakes every waiter.
func (c *FileCache) notifyLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// waitLocked waits, with c.mu released, for the cache to change or ctx to
// end.
func (c *FileCache) waitLocked(ctx context.Context) error {
	changed := c.changed
	c.mu.Unlock()
	defer c.mu.Lock()
	select {
	case <-changed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reserveLocked takes a descriptor slot, evicting the least recently used
// idle handle when the cache is full, or waits for one until ctx ends.
func (c *FileCache) reserveLocked(ctx context.Context) error {
	for c.open >= c.max && !c.evictLocked() {
		if err := c.waitLocked(ctx); err != nil {
			return err
		}
	}
	c.countLocked()
	return nil
}

// countLocked counts a descriptor about to be opened.
func (c *FileCache) countLocked() {
	c.open++
	c.opens++
	c.peak = max(c.peak, c.open)
}

// evictLocked closes the least recently used idle handle, false when none
// is idle.
func (c *FileCache) evictLocked() bool {
	e := c.idle.Front()
	if e == nil {
		return false
	}
	cf := e.Value.(*CachedFile)
	c.idle.Remove(e)
	cf.elem = nil
	_ = cf.f.Close()
	cf.f = nil
	c.open--
	return true
}

// CachedFile is one part file of a FileCache: an io.ReaderAt that holds a
// descriptor only while the cache keeps it.
type CachedFile struct {
	cache *FileCache
	path  string
	id    os.FileInfo
	size  int64

	// Guarded by cache.mu.
	f       *os.File
	leases  int
	opening bool
	closed  bool
	elem    *list.Element
}

// Open opens the part at path through open, after reserving its slot, and
// keeps it as idle. open must return path's file opened with OpenFlags (a
// caller that confines paths to a directory opens through an os.Root); it
// is called once, and the cache reopens the file by path itself after an
// eviction. A nil open opens path directly. A file that is not regular is
// refused with ErrNotRegular.
func (c *FileCache) Open(ctx context.Context, path string, open func() (*os.File, error)) (*CachedFile, error) {
	if open == nil {
		open = func() (*os.File, error) { return openRegular(path) }
	}
	c.mu.Lock()
	if err := c.reserveLocked(ctx); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.mu.Unlock()
	f, err := open()
	var info os.FileInfo
	if err == nil {
		if err = Blocking(f); err == nil {
			info, err = f.Stat()
		}
		if err != nil {
			_ = f.Close()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.open--
		c.notifyLocked()
		return nil, err
	}
	cf := &CachedFile{cache: c, path: path, id: info, size: info.Size(), f: f}
	cf.elem = c.idle.PushBack(cf)
	c.notifyLocked()
	return cf, nil
}

// Name returns the path the file is reopened by.
func (f *CachedFile) Name() string { return f.path }

// Size returns the file's size when it was opened.
func (f *CachedFile) Size() int64 { return f.size }

// ReadAt reads from the file through a leased handle, waiting for one
// without a deadline. Readers with a context read through WithContext.
func (f *CachedFile) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(context.Background(), p, off)
}

// WithContext returns a reader of the file whose wait for a handle ends
// with ctx.
func (f *CachedFile) WithContext(ctx context.Context) io.ReaderAt {
	return contextFile{f, ctx}
}

type contextFile struct {
	f   *CachedFile
	ctx context.Context
}

func (c contextFile) ReadAt(p []byte, off int64) (int, error) { return c.f.readAt(c.ctx, p, off) }

func (f *CachedFile) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	h, err := f.lease(ctx)
	if err != nil {
		return 0, err
	}
	defer f.release()
	return h.ReadAt(p, off)
}

func (f *CachedFile) lease(ctx context.Context) (*os.File, error) {
	c := f.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		switch {
		case f.closed:
			return nil, f.closedError()
		case f.f != nil:
			if f.leases == 0 && f.elem != nil {
				c.idle.Remove(f.elem)
				f.elem = nil
			}
			f.leases++
			return f.f, nil
		case f.opening:
			if err := c.waitLocked(ctx); err != nil {
				return nil, err
			}
		case c.open >= c.max && !c.evictLocked():
			// Every slot is leased: wait, then look at the file again (it
			// may have been reopened or closed meanwhile).
			if err := c.waitLocked(ctx); err != nil {
				return nil, err
			}
		default:
			c.countLocked()
			f.opening = true
			c.mu.Unlock()
			h, err := f.reopen()
			c.mu.Lock()
			f.opening = false
			c.notifyLocked()
			if err != nil {
				c.open--
				return nil, err
			}
			if f.closed {
				_ = h.Close()
				c.open--
				continue
			}
			f.f = h
		}
	}
}

// reopen opens the file by path and checks it is the file first opened.
func (f *CachedFile) reopen() (*os.File, error) {
	h, err := openRegular(f.path)
	if err != nil {
		return nil, fmt.Errorf("reopen part: %w", err)
	}
	info, err := h.Stat()
	if err == nil && !os.SameFile(info, f.id) {
		err = errors.New("the file was replaced since it was opened")
	}
	if err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("reopen part %s: %w", f.path, err)
	}
	return h, nil
}

func (f *CachedFile) closedError() error {
	return fmt.Errorf("%s: %w (%w)", f.path, ErrClosed, os.ErrClosed)
}

func (f *CachedFile) release() {
	c := f.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	f.leases--
	if f.leases == 0 {
		if f.closed {
			f.closeLocked()
		} else {
			f.elem = c.idle.PushBack(f)
		}
	}
	c.notifyLocked()
}

func (f *CachedFile) closeLocked() {
	if f.elem != nil {
		f.cache.idle.Remove(f.elem)
		f.elem = nil
	}
	if f.f != nil {
		_ = f.f.Close()
		f.f = nil
		f.cache.open--
	}
}

// Close marks the file closed: reads waiting for its handle fail with
// ErrClosed (and os.ErrClosed) at once, and its descriptor is closed now or when the last
// read holding it ends. Later reads fail.
func (f *CachedFile) Close() error {
	c := f.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	f.closed = true
	if f.leases == 0 {
		f.closeLocked()
	}
	c.notifyLocked()
	return nil
}

var _ io.ReaderAt = (*CachedFile)(nil)

// ErrNotRegular reports a part path that names something other than a
// regular file.
var ErrNotRegular = errors.New("not a regular file")
