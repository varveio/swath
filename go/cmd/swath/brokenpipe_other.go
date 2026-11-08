// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"errors"
	"syscall"
)

// brokenPipe reports whether err is the stdout reader closing the stream.
func brokenPipe(err error) bool { return errors.Is(err, syscall.EPIPE) }
