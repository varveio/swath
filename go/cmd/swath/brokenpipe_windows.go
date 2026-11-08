// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package main

import (
	"errors"

	"golang.org/x/sys/windows"
)

// brokenPipe reports whether err is the stdout reader closing the stream:
// a write to a pipe whose reader has gone fails with ERROR_NO_DATA (the
// pipe is closing) or ERROR_BROKEN_PIPE, never with EPIPE.
func brokenPipe(err error) bool {
	return errors.Is(err, windows.ERROR_NO_DATA) || errors.Is(err, windows.ERROR_BROKEN_PIPE)
}
