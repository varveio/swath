// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/signal"
	"sync/atomic"
)

// statusNow prints the running list command's progress record (nil: no
// run is going).
var statusNow atomic.Pointer[func()]

// statusOnSignal prints a progress record on stderr, in the log's format,
// at each status signal (statusSignals: SIGUSR1, and SIGINFO, Ctrl-T,
// where the platform has it), as GNU dd does.
func statusOnSignal() {
	if len(statusSignals) == 0 {
		return
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, statusSignals...)
	go func() {
		for range c {
			if f := statusNow.Load(); f != nil {
				(*f)()
			}
		}
	}()
}
