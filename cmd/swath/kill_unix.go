// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

func kill() { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL) }

// exit ends the process with code. A run a signal stopped (exitInterrupted,
// exitTerminated) dies by that signal once it has cleaned up: the handler is
// reset and the signal raised again, so a calling shell loop stops as it
// would for any command killed by it (the shell still reports 130 or 143).
func exit(code int) {
	sig := map[int]syscall.Signal{exitInterrupted: syscall.SIGINT, exitTerminated: syscall.SIGTERM}[code]
	if sig != 0 {
		signal.Reset(sig)
		_ = syscall.Kill(os.Getpid(), sig)
		time.Sleep(time.Second) // the signal is delivered meanwhile; the code is a fallback
	}
	os.Exit(code)
}
