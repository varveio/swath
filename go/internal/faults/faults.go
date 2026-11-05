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
	"journal-sync", "recorded", "rename", "renamed-unsynced", "renamed", "manifest", "success"}

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
