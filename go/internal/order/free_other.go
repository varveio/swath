// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !unix && !windows

package order

// freeBytes is not measured on this platform: only the cap and ENOSPC hold.
func freeBytes(string) int64 { return 1<<63 - 1 }
