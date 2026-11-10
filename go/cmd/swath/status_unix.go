// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build unix && !(darwin || dragonfly || freebsd || netbsd || openbsd)

package main

import (
	"os"
	"syscall"
)

// statusSignals ask a run for its progress record (statusOnSignal).
var statusSignals = []os.Signal{syscall.SIGUSR1}
