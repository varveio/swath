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

// SpillPoints are the durable spill's crash points in a run, in order: the
// sync of each directory the run creates (spill/, spill/durable/; twice in
// a run), a durable file's writes (the first, carrying the header and, at a range's
// start, the capture start; later rows frames; the trailer and tail), its
// fsync at the seal, then the flusher's directory sync,
// ledger append, ledger sync and unlink. The durable kill matrices
// (listing's TestDurableKillMatrix, cmd/swath's SIGKILL matrix) require
// each of them reached on a fixture built to reach it.
var SpillPoints = []string{"spill-dir-sync", "spill-write-header", "spill-write-frames", "spill-write-trailer", "spill-seal",
	"spill-ledger-dirsync", "spill-ledger-append", "spill-ledger-sync", "spill-unlink"}

// RecoveryPoints are the durable spill recovery's crash points, reached on
// a resume, in order: the ledger's truncation, after it (before its
// sync), the retirements' append, their sync, each unlink, the directory
// sync, the start of a recovery replay, and each captured frame it
// replays (mid replay, after earlier frames' rows were handed on).
var RecoveryPoints = []string{"spill-ledger-truncate", "spill-ledger-truncated", "spill-recovery-retire", "spill-recovery-sync",
	"spill-recovery-unlink", "spill-recovery-dirsync", "spill-replay", "spill-replay-frame"}

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
