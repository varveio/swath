// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package dataset

import (
	"errors"
	"syscall"
)

// Windows cannot flush a directory handle opened read-only; that specific
// refusal is not a durability failure.
func dirSyncUnsupported(err error) bool {
	return errors.Is(err, syscall.ERROR_ACCESS_DENIED) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.Errno(6)) // ERROR_INVALID_HANDLE
}
