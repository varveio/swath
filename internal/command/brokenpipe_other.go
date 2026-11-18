// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package command

import (
	"errors"
	"syscall"
)

// brokenPipe reports whether err is the stdout reader closing the stream.
func brokenPipe(err error) bool { return errors.Is(err, syscall.EPIPE) }
