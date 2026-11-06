// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortedparquet

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// FileCache bounds the descriptors a set of part files holds open. A part
// is a *CachedFile: each ReadAt leases an open handle for its duration,
// reopening the file by path when its handle was evicted, so a fixture of
// any number of parts holds at most Max descriptors, the least recently
// used idle handle going first when one more is needed. A reopened file
// must be the one first opened (os.SameFile), or the read fails: a part is
// never read from a file that replaced it.
//
// It is safe for concurrent use. A read waits while Max handles are all
// leased; reads per file share one handle (ReadAt is safe concurrently).
type FileCache struct {
	max  int
	mu   sync.Mutex
	cond *sync.Cond
	open int
	idle list.List // *CachedFile with an open handle and no lease, oldest first

	peak, opens int
}

// NewFileCache returns a cache holding at most max descriptors; max below
// one is DefaultMaxOpenFiles().
func NewFileCache(max int) *FileCache {
	if max < 1 {
		max = DefaultMaxOpenFiles()
	}
	c := &FileCache{max: max}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// FileCacheStats are a cache's counts.
type FileCacheStats struct {
	// Open is the handles open now, Peak the most at once, and Opens the
	// files opened in all, first opens and reopens after eviction.
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

// Add takes f, an open regular file, into the cache as the part at path
// (the path it is reopened by). The cache owns f from then on, also on
// error.
func (c *FileCache) Add(path string, f *os.File) (*CachedFile, error) {
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	cf := &CachedFile{cache: c, path: path, id: info, size: info.Size()}
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.open >= c.max {
		if !c.evictLocked() {
			c.cond.Wait()
		}
	}
	c.countOpenLocked()
	cf.f = f
	cf.elem = c.idle.PushBack(cf)
	return cf, nil
}

func (c *FileCache) countOpenLocked() {
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

// Name returns the path the file is reopened by.
func (f *CachedFile) Name() string { return f.path }

// Size returns the file's size when it was added.
func (f *CachedFile) Size() int64 { return f.size }

// ReadAt reads from the file through a leased handle.
func (f *CachedFile) ReadAt(p []byte, off int64) (int, error) {
	h, err := f.lease()
	if err != nil {
		return 0, err
	}
	defer f.release()
	return h.ReadAt(p, off)
}

func (f *CachedFile) lease() (*os.File, error) {
	c := f.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		switch {
		case f.closed:
			return nil, fmt.Errorf("%s: %w", f.path, os.ErrClosed)
		case f.f != nil:
			if f.leases == 0 && f.elem != nil {
				c.idle.Remove(f.elem)
				f.elem = nil
			}
			f.leases++
			return f.f, nil
		case f.opening:
			c.cond.Wait()
		case c.open < c.max:
			f.opening = true
			c.countOpenLocked()
			c.mu.Unlock()
			h, err := f.reopen()
			c.mu.Lock()
			f.opening = false
			c.cond.Broadcast()
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
		case !c.evictLocked():
			c.cond.Wait()
		}
	}
}

// reopen opens the file by path and checks it is the file first opened.
func (f *CachedFile) reopen() (*os.File, error) {
	h, err := os.Open(f.path)
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
	c.cond.Broadcast()
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

// Close releases the file's handle (after the reads holding it); later
// reads fail.
func (f *CachedFile) Close() error {
	c := f.cache
	c.mu.Lock()
	defer c.mu.Unlock()
	f.closed = true
	if f.leases == 0 {
		f.closeLocked()
	}
	c.cond.Broadcast()
	return nil
}

var _ io.ReaderAt = (*CachedFile)(nil)
