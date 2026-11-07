// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package faults injects crashes at named points for the crash-matrix
// tests. In production no hook is set and At costs one atomic load.
package faults

import (
	"errors"
	"sync/atomic"
)

// ErrCrash is what an injected crash returns. A run failing with it skips
// all cleanup, leaving the output as a killed process would.
var ErrCrash = errors.New("injected crash")

// Points are the crash points the matrix covers, in run order.
var Points = []string{"response-read", "split", "range-done", "encoding", "part-closed", "journal-append",
	"journal-sync", "recorded", "rename", "renamed-unsynced", "renamed", "published", "manifest"}

// SegmentPoints are the crash points inside the segment-parallel tail
// (order's segments, reached only when they engage): the segments planned
// and committed, none opened yet; a segment's part published (its
// neighbours still open); every segment's rows handed on and the last
// part held, before the router's Close.
var SegmentPoints = []string{"segments-begun", "segment-published", "segments-written"}

var hook atomic.Pointer[func(point string) error]

// Set installs f (nil removes the hook) and returns a function restoring
// the previous one.
func Set(f func(point string) error) (restore func()) {
	var p *func(string) error
	if f != nil {
		p = &f
	}
	old := hook.Swap(p)
	return func() { hook.Store(old) }
}

// At reports point and returns the hook's error, nil without a hook.
func At(point string) error {
	if f := hook.Load(); f != nil {
		return (*f)(point)
	}
	return nil
}

// File is a file whose writes and syncs Write and Sync can fail.
type File interface {
	Write([]byte) (int, error)
	Sync() error
	Name() string
}

// IOFault decides the fate of a durable write (op "write", b the bytes) or
// sync (op "sync") of the file named name: a nil error lets it happen; an
// error fails it, a write after writing its first keep bytes (a partial
// write), a sync without syncing.
type IOFault func(op, name string, b []byte) (keep int, err error)

var ioHook atomic.Pointer[IOFault]

// SetIO installs f (nil removes it) and returns a function restoring the
// previous one.
func SetIO(f IOFault) (restore func()) {
	var p *IOFault
	if f != nil {
		p = &f
	}
	old := ioHook.Swap(p)
	return func() { ioHook.Store(old) }
}

// Write writes b to f, unless the IOFault fails it.
func Write(f File, b []byte) (int, error) {
	if h := ioHook.Load(); h != nil {
		if keep, err := (*h)("write", f.Name(), b); err != nil {
			n, _ := f.Write(b[:min(max(keep, 0), len(b))])
			return n, err
		}
	}
	return f.Write(b)
}

// Sync syncs f, unless the IOFault fails it.
func Sync(f File) error {
	if h := ioHook.Load(); h != nil {
		if _, err := (*h)("sync", f.Name(), nil); err != nil {
			return err
		}
	}
	return f.Sync()
}

// Writer is f as an io.Writer whose writes go through Write.
func Writer(f File) interface{ Write([]byte) (int, error) } { return faultWriter{f} }

type faultWriter struct{ f File }

func (w faultWriter) Write(b []byte) (int, error) { return Write(w.f, b) }
